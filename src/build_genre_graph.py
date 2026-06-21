#!/usr/bin/env python3
"""F2 Phase 2 — build the genre-level derivation graph.

MusicBrainz stores genres through the tag system: a tag counts as a genre when
its name is in the controlled `genre` vocabulary. We resolve a primary genre per
recording (most-voted recording_tag genre; fallback = the recording's artist
most-voted genre) and aggregate the filtered sample/remix edge set into a
genre -> genre graph (A -> B = a genre-A recording derived from a genre-B one).

Outputs:
  graph_output/mb_genre_graph.graphml.gz
  analysis_output/recording_genre_map.csv
"""

from __future__ import annotations

import csv
import os
from collections import Counter

import duckdb
import networkx as nx

import build_mb_graph as b
import graphs

MAP_CSV = os.path.join(b.REPO, "analysis_output", "recording_genre_map.csv")

EXTRA_COLS = {
    "genre": ["id", "gid", "name", "comment", "edits_pending", "last_updated"],
    "tag": ["id", "name", "ref_count"],
    "artist_tag": ["artist", "tag", "count", "last_updated"],
    "recording_tag": ["recording", "tag", "count", "last_updated"],
}


def main() -> None:
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4")
    con.execute("PRAGMA memory_limit='6GB'")
    b.load_all(con)
    b.cast_columns(con)

    print("Loading genre/tag tables...")
    b.COLS.update(EXTRA_COLS)
    for t in EXTRA_COLS:
        b._load_table(con, t)
    con.execute("ALTER TABLE genre ALTER id TYPE BIGINT USING TRY_CAST(id AS BIGINT)")
    con.execute("ALTER TABLE tag ALTER id TYPE BIGINT USING TRY_CAST(id AS BIGINT)")
    for c in ("artist", "tag", "count"):
        con.execute(f"ALTER TABLE artist_tag ALTER {c} TYPE BIGINT USING TRY_CAST({c} AS BIGINT)")
    for c in ("recording", "tag", "count"):
        con.execute(f"ALTER TABLE recording_tag ALTER {c} TYPE BIGINT USING TRY_CAST({c} AS BIGINT)")

    # genres = tags whose name is in the controlled genre vocabulary
    con.execute("""
        CREATE TEMPORARY TABLE genre_tag AS
        SELECT t.id AS tag_id, lower(g.name) AS gname
        FROM tag t JOIN genre g ON lower(t.name) = lower(g.name)
    """)
    print(f"  {con.execute('SELECT COUNT(*) FROM genre_tag').fetchone()[0]} genre tag-ids")

    b.attach_geo_temporal(con)          # build_filtered_edges joins rec_year/artist_country
    df = b.build_filtered_edges(con)    # creates temp filtered_edges; df has entity0/1, e0/e1_artist
    print(f"  {len(df):,} edges")

    con.execute("""
        CREATE TEMPORARY TABLE edge_recs AS
        SELECT entity0 AS rid FROM filtered_edges
        UNION SELECT entity1 FROM filtered_edges
    """)
    rec_genre = dict(con.execute("""
        WITH rt AS (
          SELECT r.recording AS rid, gt.gname,
                 row_number() OVER (PARTITION BY r.recording
                                    ORDER BY r.count DESC, gt.gname) rn
          FROM recording_tag r
          JOIN genre_tag gt ON gt.tag_id = r.tag
          WHERE r.recording IN (SELECT rid FROM edge_recs) AND r.count > 0
        )
        SELECT rid, gname FROM rt WHERE rn = 1
    """).fetchall())
    print(f"  {len(rec_genre):,} recordings with a direct genre")

    art_genre = dict(con.execute("""
        WITH ats AS (
          SELECT a.artist AS aid, gt.gname,
                 row_number() OVER (PARTITION BY a.artist
                                    ORDER BY a.count DESC, gt.gname) rn
          FROM artist_tag a
          JOIN genre_tag gt ON gt.tag_id = a.tag
          WHERE a.count > 0
        )
        SELECT aid, gname FROM ats WHERE rn = 1
    """).fetchall())
    print(f"  {len(art_genre):,} artists with a genre")

    def genre_of(rid, aid):
        g = rec_genre.get(rid)
        if g:
            return g
        try:
            if aid is not None and aid == aid and int(aid) >= 0:
                return art_genre.get(int(aid))
        except (TypeError, ValueError):
            pass
        return None

    H = nx.DiGraph()
    edge_acc: dict = {}
    endpoint: Counter = Counter()
    rec_resolved: dict = {}
    dropped = 0
    for r in df.itertuples(index=False):
        e0, e1 = int(r.entity0), int(r.entity1)
        g0 = genre_of(e0, r.e0_artist)
        g1 = genre_of(e1, r.e1_artist)
        rec_resolved[e0] = g0
        rec_resolved[e1] = g1
        if not g0 or not g1:
            dropped += 1
            continue
        endpoint[g0] += 1
        endpoint[g1] += 1
        bucket = edge_acc.setdefault((g0, g1), {"weight": 0, "types": set()})
        bucket["weight"] += 1
        bucket["types"].add(b.TYPE_SHORT.get(r.edge_type, r.edge_type))

    for g, c in endpoint.items():
        H.add_node(g, endpoint_count=int(c))
    for (a, c), bucket in edge_acc.items():
        H.add_edge(a, c, weight=int(bucket["weight"]),
                   types=";".join(sorted(t for t in bucket["types"] if t)))
    print(f"  genre graph: {H.number_of_nodes()} genres / {H.number_of_edges()} edges; "
          f"dropped {dropped:,} edges with a missing genre")

    graphs.save(H, "genre")

    cov = sum(1 for v in rec_resolved.values() if v)
    tot = len(rec_resolved)
    print(f"  recording genre coverage: {cov:,}/{tot:,} ({100*cov/max(tot,1):.1f}%)")
    os.makedirs(os.path.dirname(MAP_CSV), exist_ok=True)
    with open(MAP_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["recording_id", "genre"])
        for rid, g in rec_resolved.items():
            if g:
                w.writerow([rid, g])
    print(f"  saved: {os.path.basename(MAP_CSV)}")
    print("DONE (build genre graph).")


if __name__ == "__main__":
    main()
