#!/usr/bin/env python3
"""Build a sample/remix/cover/mashup graph from a MusicBrainz core dump.

Loads the extracted TSVs into DuckDB, filters recording-recording
relationships to the relationship types we care about, computes a
popularity proxy (number of track appearances per recording), thresholds
the graph to roughly TARGET_EDGES, and emits GraphML + analysis artefacts.

Source: MusicBrainz core dump 20260425-002540 — public domain (CC0).
"""

from __future__ import annotations

import os
import sys
from collections import Counter

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import networkx as nx

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TSV_DIR = os.path.join(REPO, "data", "tsv")
OUT_DIR = os.path.join(REPO, "graph_output")
os.makedirs(OUT_DIR, exist_ok=True)

GRAPHML_FILE = os.path.join(OUT_DIR, "mb_sample_graph.graphml")
ARTIST_GRAPHML = os.path.join(OUT_DIR, "mb_artist_graph.graphml")
DECADE_GRAPHML = os.path.join(OUT_DIR, "mb_decade_graph.graphml")
COUNTRY_GRAPHML = os.path.join(OUT_DIR, "mb_country_graph.graphml")
STATS_FILE = os.path.join(OUT_DIR, "graph_stats.txt")
ARTIST_STATS_FILE = os.path.join(OUT_DIR, "artist_graph_stats.txt")
DECADE_STATS_FILE = os.path.join(OUT_DIR, "decade_graph_stats.txt")
COUNTRY_STATS_FILE = os.path.join(OUT_DIR, "country_graph_stats.txt")

# Relationship-type names we keep. Verified against MB stats page (2026-04).
KEEP_LINK_TYPES = (
    "samples material",
    "is a remix of",
    "is a mash-up of",
    "is an edit of",
    "is a DJ-mix of",
)
TYPE_SHORT = {
    "samples material": "sample",
    "is a remix of": "remix",
    "is a mash-up of": "mashup",
    "is an edit of": "edit",
    "is a DJ-mix of": "dj_mix",
}

TARGET_EDGES = 100_000


# Column names for each TSV. Source: MusicBrainz schema (admin/sql/CreateTables.sql).
COLS = {
    "artist": [
        "id", "gid", "name", "sort_name",
        "begin_date_year", "begin_date_month", "begin_date_day",
        "end_date_year", "end_date_month", "end_date_day",
        "type", "area", "gender", "comment", "edits_pending",
        "last_updated", "ended", "begin_area", "end_area",
    ],
    "artist_credit": [
        "id", "name", "artist_count", "ref_count", "created",
        "edits_pending", "gid",
    ],
    "artist_credit_name": [
        "artist_credit", "position", "artist", "name", "join_phrase",
    ],
    "recording": [
        "id", "gid", "name", "artist_credit", "length", "comment",
        "edits_pending", "last_updated", "video",
    ],
    "link": [
        "id", "link_type",
        "begin_date_year", "begin_date_month", "begin_date_day",
        "end_date_year", "end_date_month", "end_date_day",
        "attribute_count", "created", "ended",
    ],
    "link_type": [
        "id", "parent", "child_order", "gid",
        "entity_type0", "entity_type1", "name", "description",
        "link_phrase", "reverse_link_phrase", "long_link_phrase",
        "last_updated", "is_deprecated", "has_dates",
        "entity0_cardinality", "entity1_cardinality",
    ],
    "l_recording_recording": [
        "id", "link", "entity0", "entity1",
        "edits_pending", "last_updated", "link_order",
        "entity0_credit", "entity1_credit",
    ],
    "track": [
        "id", "gid", "recording", "medium", "position",
        "number", "name", "artist_credit", "length",
        "edits_pending", "last_updated", "is_data_track",
    ],
    "area": [
        "id", "gid", "name", "type", "edits_pending", "last_updated",
        "begin_date_year", "begin_date_month", "begin_date_day",
        "end_date_year", "end_date_month", "end_date_day",
        "ended", "comment",
    ],
    "medium": [
        "id", "release", "position", "format", "name",
        "edits_pending", "last_updated", "track_count", "gid",
    ],
    "release_country": [
        "release", "country", "date_year", "date_month", "date_day",
    ],
    "release_unknown_country": [
        "release", "date_year", "date_month", "date_day",
    ],
}


def _columns_clause(table: str) -> str:
    cols = COLS[table]
    return ", ".join(f"'{c}': 'VARCHAR'" for c in cols)


def _load_table(con: duckdb.DuckDBPyConnection, name: str) -> None:
    path = os.path.join(TSV_DIR, name)
    if not os.path.exists(path):
        sys.exit(f"missing TSV: {path}")
    cols = COLS[name]
    columns_arg = "{" + ", ".join(f"'{c}': 'VARCHAR'" for c in cols) + "}"
    sql = f"""
        CREATE TABLE {name} AS
        SELECT * FROM read_csv(
            '{path}',
            delim='\t',
            header=false,
            quote='',
            escape='',
            nullstr='\\N',
            columns={columns_arg}
        )
    """
    con.execute(sql)
    n = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
    print(f"  loaded {name}: {n:,} rows")


def load_all(con: duckdb.DuckDBPyConnection) -> None:
    print("Loading TSVs into DuckDB...")
    for t in [
        "link_type",
        "link",
        "l_recording_recording",
        "artist",
        "artist_credit",
        "artist_credit_name",
        "recording",
        "track",
        "area",
        "medium",
        "release_country",
        "release_unknown_country",
    ]:
        _load_table(con, t)


def cast_columns(con: duckdb.DuckDBPyConnection) -> None:
    """Cast key id/length columns from VARCHAR to BIGINT for join performance."""
    print("Casting id/length columns...")
    for table, cols in [
        ("artist", ["id", "area"]),
        ("artist_credit", ["id"]),
        ("artist_credit_name", ["artist_credit", "artist", "position"]),
        ("recording", ["id", "artist_credit", "length"]),
        ("link", ["id", "link_type"]),
        ("link_type", ["id"]),
        ("l_recording_recording", ["id", "link", "entity0", "entity1"]),
        ("track", ["id", "recording", "medium", "artist_credit"]),
        ("area", ["id", "type"]),
        ("medium", ["id", "release"]),
        ("release_country", ["release", "country", "date_year"]),
        ("release_unknown_country", ["release", "date_year"]),
    ]:
        for c in cols:
            con.execute(
                f"ALTER TABLE {table} ALTER {c} TYPE BIGINT USING TRY_CAST({c} AS BIGINT)"
            )


def attach_geo_temporal(con: duckdb.DuckDBPyConnection) -> None:
    """Build helper tables: rec_year (recording -> first release year)
    and artist_country (artist -> country name, only for area.type=1).
    """
    print("Computing first-release year per release...")
    con.execute("""
        CREATE OR REPLACE TEMPORARY TABLE rel_year AS
        SELECT release, MIN(date_year) AS y
        FROM (
            SELECT release, date_year
            FROM release_country
            WHERE date_year IS NOT NULL
            UNION ALL
            SELECT release, date_year
            FROM release_unknown_country
            WHERE date_year IS NOT NULL
        )
        GROUP BY release
    """)
    n = con.execute("SELECT COUNT(*) FROM rel_year").fetchone()[0]
    print(f"  {n:,} releases with year info")

    print("Propagating year to recording level (track -> medium -> release)...")
    con.execute("""
        CREATE OR REPLACE TEMPORARY TABLE rec_year AS
        SELECT t.recording AS recording_id, MIN(ry.y) AS year
        FROM track t
        JOIN medium m  ON m.id = t.medium
        JOIN rel_year ry ON ry.release = m.release
        WHERE t.recording IS NOT NULL
        GROUP BY t.recording
    """)
    n = con.execute("SELECT COUNT(*) FROM rec_year").fetchone()[0]
    print(f"  {n:,} recordings with year info")

    print("Resolving artist -> country (area.type=1)...")
    con.execute("""
        CREATE OR REPLACE TEMPORARY TABLE artist_country AS
        SELECT a.id AS artist_id, ar.name AS country
        FROM artist a
        JOIN area ar ON ar.id = a.area
        WHERE ar.type = 1
    """)
    n = con.execute("SELECT COUNT(*) FROM artist_country").fetchone()[0]
    print(f"  {n:,} artists tagged with a country")


def build_filtered_edges(con: duckdb.DuckDBPyConnection):
    """Return a DataFrame of edges with endpoint metadata, popularity,
    year, and country.

    Columns: entity0, entity1, edge_type, e0_artist_credit, e1_artist_credit,
             e0_name, e1_name, e0_pop, e1_pop, e0_artist, e1_artist,
             e0_artist_name, e1_artist_name, e0_year, e1_year,
             e0_country, e1_country.
    """
    types_quoted = ", ".join(f"'{t}'" for t in KEEP_LINK_TYPES)
    print("Filtering recording-recording links by type...")
    con.execute(f"""
        CREATE TEMPORARY TABLE rel_edges AS
        SELECT
            l.entity0, l.entity1,
            lt.name AS edge_type
        FROM l_recording_recording l
        JOIN link  ON link.id = l.link
        JOIN link_type lt ON lt.id = link.link_type
        WHERE lt.name IN ({types_quoted})
    """)
    n_total = con.execute("SELECT COUNT(*) FROM rel_edges").fetchone()[0]
    print(f"  {n_total:,} relevant edges before popularity filter")

    print("Computing recording popularity (track appearances)...")
    con.execute("""
        CREATE TEMPORARY TABLE rec_pop AS
        SELECT recording AS recording_id, COUNT(*) AS popularity
        FROM track
        WHERE recording IS NOT NULL
        GROUP BY recording
    """)
    n_pop = con.execute("SELECT COUNT(*) FROM rec_pop").fetchone()[0]
    print(f"  {n_pop:,} recordings with at least one appearance")

    print("Picking popularity threshold to land near target...")
    threshold = _pick_threshold(con, n_total)
    print(f"  threshold = {threshold} appearances on both endpoints")

    print("Building filtered + enriched edge set...")
    con.execute(f"""
        CREATE OR REPLACE TEMPORARY TABLE filtered_edges AS
        SELECT
            r.entity0, r.entity1, r.edge_type,
            COALESCE(p0.popularity, 0) AS e0_pop,
            COALESCE(p1.popularity, 0) AS e1_pop,
            rec0.name AS e0_name,
            rec1.name AS e1_name,
            rec0.artist_credit AS e0_artist_credit,
            rec1.artist_credit AS e1_artist_credit
        FROM rel_edges r
        LEFT JOIN rec_pop p0 ON p0.recording_id = r.entity0
        LEFT JOIN rec_pop p1 ON p1.recording_id = r.entity1
        JOIN recording rec0 ON rec0.id = r.entity0
        JOIN recording rec1 ON rec1.id = r.entity1
        WHERE COALESCE(p0.popularity, 0) >= {threshold}
          AND COALESCE(p1.popularity, 0) >= {threshold}
    """)
    n_filtered = con.execute("SELECT COUNT(*) FROM filtered_edges").fetchone()[0]
    print(f"  {n_filtered:,} edges after popularity filter")

    print("Joining artist-credit names...")
    con.execute("""
        CREATE OR REPLACE TEMPORARY TABLE ac_first_artist AS
        SELECT artist_credit, MIN(position) AS pos
        FROM artist_credit_name
        GROUP BY artist_credit
    """)
    con.execute("""
        CREATE OR REPLACE TEMPORARY TABLE ac_to_artist AS
        SELECT a.artist_credit, n.artist AS artist_id, ar.name AS artist_name
        FROM ac_first_artist a
        JOIN artist_credit_name n
            ON n.artist_credit = a.artist_credit AND n.position = a.pos
        JOIN artist ar ON ar.id = n.artist
    """)

    df = con.execute("""
        SELECT
            f.entity0, f.entity1, f.edge_type,
            f.e0_pop, f.e1_pop,
            f.e0_name, f.e1_name,
            a0.artist_id AS e0_artist, a0.artist_name AS e0_artist_name,
            a1.artist_id AS e1_artist, a1.artist_name AS e1_artist_name,
            ry0.year     AS e0_year,    ry1.year     AS e1_year,
            ac0.country  AS e0_country, ac1.country  AS e1_country
        FROM filtered_edges f
        LEFT JOIN ac_to_artist a0   ON a0.artist_credit  = f.e0_artist_credit
        LEFT JOIN ac_to_artist a1   ON a1.artist_credit  = f.e1_artist_credit
        LEFT JOIN rec_year     ry0  ON ry0.recording_id  = f.entity0
        LEFT JOIN rec_year     ry1  ON ry1.recording_id  = f.entity1
        LEFT JOIN artist_country ac0 ON ac0.artist_id    = a0.artist_id
        LEFT JOIN artist_country ac1 ON ac1.artist_id    = a1.artist_id
    """).fetchdf()
    return df


def _pick_threshold(con: duckdb.DuckDBPyConnection, n_total: int) -> int:
    """Find a threshold T such that filtering by min-popularity T gets close to TARGET_EDGES."""
    candidates = [1, 2, 3, 4, 5, 7, 10, 15, 20, 30, 50, 100]
    best = 1
    for t in candidates:
        n = con.execute(f"""
            SELECT COUNT(*)
            FROM rel_edges r
            LEFT JOIN rec_pop p0 ON p0.recording_id = r.entity0
            LEFT JOIN rec_pop p1 ON p1.recording_id = r.entity1
            WHERE COALESCE(p0.popularity, 0) >= {t}
              AND COALESCE(p1.popularity, 0) >= {t}
        """).fetchone()[0]
        print(f"    threshold={t}: {n:,} edges")
        if n <= TARGET_EDGES:
            best = t
            break
        best = t
    # Ensure we keep something — if even threshold=1 was over target, accept that.
    return best


def df_to_graph(df) -> nx.DiGraph:
    print("Building NetworkX directed graph...")
    G = nx.DiGraph()

    def _ensure_node(rid, name, artist_id, artist_name, pop):
        nid = str(int(rid))
        if not G.has_node(nid):
            G.add_node(
                nid,
                title=str(name) if name is not None else "",
                artist=str(artist_name) if artist_name is not None else "",
                artist_id=int(artist_id) if artist_id == artist_id and artist_id is not None else -1,
                popularity=int(pop) if pop is not None else 0,
            )

    for r in df.itertuples(index=False):
        _ensure_node(r.entity0, r.e0_name, r.e0_artist, r.e0_artist_name, r.e0_pop)
        _ensure_node(r.entity1, r.e1_name, r.e1_artist, r.e1_artist_name, r.e1_pop)
        # WhoSampled-style orientation: derivative -> original.
        # For "is a remix of"/"samples material"/etc., entity0 IS the derivative
        # and entity1 IS the source. So edge entity0 -> entity1.
        a, b = str(int(r.entity0)), str(int(r.entity1))
        edge_type = TYPE_SHORT.get(r.edge_type, r.edge_type)
        if G.has_edge(a, b):
            d = G.get_edge_data(a, b)
            d["weight"] = d.get("weight", 1) + 1
            existing_types = set(t for t in d.get("types", "").split(";") if t)
            existing_types.add(edge_type)
            d["types"] = ";".join(sorted(existing_types))
            d["type"] = edge_type
        else:
            G.add_edge(a, b, type=edge_type, types=edge_type, weight=1)
    print(f"  {G.number_of_nodes():,} nodes, {G.number_of_edges():,} edges")
    return G


def build_artist_graph(track_graph: nx.DiGraph) -> nx.DiGraph:
    print("Aggregating to artist level...")
    H = nx.DiGraph()
    track_count: Counter = Counter()
    artist_name: dict = {}
    for n, d in track_graph.nodes(data=True):
        aid = d.get("artist_id")
        if aid is None or aid < 0:
            continue
        track_count[aid] += 1
        artist_name.setdefault(aid, d.get("artist", ""))
    for aid, c in track_count.items():
        H.add_node(str(aid), artist=artist_name.get(aid, ""), track_count=int(c))

    self_loops = 0
    edge_acc: dict = {}
    for u, v, d in track_graph.edges(data=True):
        a = track_graph.nodes[u].get("artist_id")
        b = track_graph.nodes[v].get("artist_id")
        if a is None or b is None or a < 0 or b < 0:
            continue
        if a == b:
            self_loops += 1
            continue
        key = (str(a), str(b))
        bucket = edge_acc.setdefault(key, {"weight": 0, "types": set()})
        bucket["weight"] += int(d.get("weight", 1))
        bucket["types"].add(d.get("type", ""))

    for (a, b), bucket in edge_acc.items():
        H.add_edge(
            a, b,
            weight=int(bucket["weight"]),
            types=";".join(sorted(t for t in bucket["types"] if t)),
        )

    print(f"  {H.number_of_nodes():,} artists, {H.number_of_edges():,} cross-artist edges")
    print(f"  dropped {self_loops:,} intra-artist self-loops")
    return H


def _decade_label(year: int) -> str:
    return f"{(year // 10) * 10}s"


def _is_real_year(v) -> bool:
    if v is None:
        return False
    try:
        if v != v:  # NaN
            return False
    except TypeError:
        pass
    try:
        y = int(v)
    except (TypeError, ValueError):
        return False
    return 1900 <= y <= 2030


def build_decade_graph(df) -> nx.DiGraph:
    """Aggregate edges by decade of first release on both endpoints.
    Drops edges where either side has no plausible year."""
    print("Aggregating to decade level...")
    H = nx.DiGraph()
    edge_acc: dict = {}
    dropped = 0
    for r in df.itertuples(index=False):
        if not (_is_real_year(r.e0_year) and _is_real_year(r.e1_year)):
            dropped += 1
            continue
        d0 = _decade_label(int(r.e0_year))
        d1 = _decade_label(int(r.e1_year))
        if not H.has_node(d0):
            H.add_node(d0, decade=int(r.e0_year) // 10 * 10)
        if not H.has_node(d1):
            H.add_node(d1, decade=int(r.e1_year) // 10 * 10)
        key = (d0, d1)
        bucket = edge_acc.setdefault(key, {"weight": 0, "types": set()})
        bucket["weight"] += 1
        bucket["types"].add(TYPE_SHORT.get(r.edge_type, r.edge_type))
    for (a, b), bucket in edge_acc.items():
        H.add_edge(
            a, b,
            weight=int(bucket["weight"]),
            types=";".join(sorted(t for t in bucket["types"] if t)),
        )
    print(f"  {H.number_of_nodes()} decades, {H.number_of_edges()} decade->decade edges")
    print(f"  dropped {dropped:,} edges with missing/implausible year on either side")
    return H


def build_country_graph(df) -> nx.DiGraph:
    """Aggregate edges by country of artist on both endpoints.
    Drops edges where either side's artist has no country."""
    print("Aggregating to country level...")
    H = nx.DiGraph()
    edge_acc: dict = {}
    dropped = 0
    track_count: Counter = Counter()
    for r in df.itertuples(index=False):
        c0 = r.e0_country
        c1 = r.e1_country
        if c0 is None or c1 is None:
            dropped += 1
            continue
        try:
            if c0 != c0 or c1 != c1:  # NaN
                dropped += 1
                continue
        except TypeError:
            pass
        c0 = str(c0)
        c1 = str(c1)
        track_count[c0] += 1
        track_count[c1] += 1
        key = (c0, c1)
        bucket = edge_acc.setdefault(key, {"weight": 0, "types": set()})
        bucket["weight"] += 1
        bucket["types"].add(TYPE_SHORT.get(r.edge_type, r.edge_type))
    for c, n in track_count.items():
        H.add_node(c, endpoint_count=int(n))
    for (a, b), bucket in edge_acc.items():
        H.add_edge(
            a, b,
            weight=int(bucket["weight"]),
            types=";".join(sorted(t for t in bucket["types"] if t)),
        )
    print(f"  {H.number_of_nodes()} countries, {H.number_of_edges()} country->country edges")
    print(f"  dropped {dropped:,} edges with missing country on either side")
    return H


def plot_degree(G, path, title_prefix=""):
    in_deg = Counter(d for _, d in G.in_degree())
    out_deg = Counter(d for _, d in G.out_degree())
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    for ax, counter, sub, color in (
        (axes[0], in_deg, "In-degree", "steelblue"),
        (axes[1], out_deg, "Out-degree", "coral"),
    ):
        keys = sorted(counter.keys())
        vals = [counter[k] for k in keys]
        if not keys:
            continue
        ax.scatter(keys, vals, s=12, alpha=0.7, color=color)
        ax.set_xscale("symlog", linthresh=1)
        ax.set_yscale("symlog", linthresh=1)
        ax.set_xlabel(f"{sub} (k)")
        ax.set_ylabel(f"Number of nodes with {sub.lower()} k")
        ax.set_title(f"{title_prefix}{sub} Distribution")
        ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  saved: {path}")


def plot_components(G, path, title_prefix=""):
    sccs = list(nx.strongly_connected_components(G))
    wccs = list(nx.weakly_connected_components(G))
    scc_sizes = Counter(len(c) for c in sccs)
    wcc_sizes = Counter(len(c) for c in wccs)
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    for ax, sizes, sub, color in (
        (axes[0], scc_sizes, "SCC", "purple"),
        (axes[1], wcc_sizes, "WCC", "forestgreen"),
    ):
        keys = sorted(sizes.keys())
        vals = [sizes[k] for k in keys]
        if not keys:
            ax.text(0.5, 0.5, "no components", ha="center", va="center",
                    transform=ax.transAxes)
            continue
        ax.scatter(keys, vals, s=12, alpha=0.7, color=color)
        ax.set_xscale("symlog", linthresh=1)
        ax.set_yscale("symlog", linthresh=1)
        ax.set_xlabel("Component size (nodes)")
        ax.set_ylabel("Number of components with that size")
        ax.set_title(f"{title_prefix}{sub} Size Distribution")
        ax.grid(True, alpha=0.3)
        biggest = max(keys)
        if biggest > 1:
            ax.annotate(f"largest: {biggest:,}", xy=(biggest, sizes[biggest]),
                        xytext=(max(biggest / 5, 2), max(max(vals) / 4, 2)),
                        arrowprops=dict(arrowstyle="->", color="gray"), fontsize=9)
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  saved: {path}")
    return sccs, wccs


def write_stats(G, sccs, wccs, path, label, *, top_field="artist"):
    n = G.number_of_nodes()
    m = G.number_of_edges()
    avg_in = sum(d for _, d in G.in_degree()) / n if n else 0.0
    avg_out = sum(d for _, d in G.out_degree()) / n if n else 0.0
    edge_types = Counter(d.get("type", "unknown") for _, _, d in G.edges(data=True))
    largest_scc = max((len(c) for c in sccs), default=0)
    largest_wcc = max((len(c) for c in wccs), default=0)

    in_strength = Counter()
    out_strength = Counter()
    for u, v, d in G.edges(data=True):
        w = int(d.get("weight", 1))
        in_strength[v] += w
        out_strength[u] += w

    def label_of(node):
        d = G.nodes[node]
        if top_field == "artist":
            return d.get("artist", node) or node
        if top_field in ("decade", "country", "self"):
            return node
        title = d.get("title", "") or ""
        artist = d.get("artist", "") or ""
        return f"{artist} — {title}" if (artist or title) else node

    with open(path, "w", encoding="utf-8") as f:
        f.write("=" * 60 + "\n")
        f.write(f"{label}\n")
        f.write("Source: MusicBrainz core dump 20260425-002540 (CC0)\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"Vertices:                       {n:,}\n")
        f.write(f"Edges:                          {m:,}\n")
        f.write(f"Average in-degree:              {avg_in:.4f}\n")
        f.write(f"Average out-degree:             {avg_out:.4f}\n")
        f.write(f"Density:                        {nx.density(G):.6e}\n\n")
        f.write("Edge types:\n")
        for t, c in edge_types.most_common():
            f.write(f"  {t}: {c:,}\n")
        f.write("\n")
        f.write(f"Strongly connected components:  {len(sccs):,}\n")
        f.write(f"  Largest SCC:                  {largest_scc:,} nodes\n")
        f.write(f"Weakly connected components:    {len(wccs):,}\n")
        f.write(f"  Largest WCC:                  {largest_wcc:,} nodes ({100*largest_wcc/max(n,1):.1f}%)\n\n")

        f.write("Top 20 by weighted in-strength:\n")
        for nid, w in in_strength.most_common(20):
            f.write(f"  {w:6d} | {label_of(nid)}\n")
        f.write("\nTop 20 by weighted out-strength:\n")
        for nid, w in out_strength.most_common(20):
            f.write(f"  {w:6d} | {label_of(nid)}\n")
    print(f"  saved: {path}")


def main() -> None:
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4")
    con.execute("PRAGMA memory_limit='6GB'")
    load_all(con)
    cast_columns(con)
    attach_geo_temporal(con)
    df = build_filtered_edges(con)
    print(f"\nFinal filtered DataFrame: {len(df):,} rows")
    print(f"  edges with year on both sides:    "
          f"{df.dropna(subset=['e0_year', 'e1_year']).shape[0]:,}")
    print(f"  edges with country on both sides: "
          f"{df.dropna(subset=['e0_country', 'e1_country']).shape[0]:,}")

    G = df_to_graph(df)
    print(f"\nWriting track-level GraphML -> {GRAPHML_FILE}")
    nx.write_graphml(G, GRAPHML_FILE)

    H = build_artist_graph(G)
    print(f"\nWriting artist-level GraphML -> {ARTIST_GRAPHML}")
    nx.write_graphml(H, ARTIST_GRAPHML)

    H_dec = build_decade_graph(df)
    print(f"Writing decade-level GraphML -> {DECADE_GRAPHML}")
    nx.write_graphml(H_dec, DECADE_GRAPHML)

    H_co = build_country_graph(df)
    print(f"Writing country-level GraphML -> {COUNTRY_GRAPHML}")
    nx.write_graphml(H_co, COUNTRY_GRAPHML)

    print("\n=== Track graph plots/stats ===")
    plot_degree(G, os.path.join(OUT_DIR, "degree_distribution.png"), "Track ")
    sccs, wccs = plot_components(G, os.path.join(OUT_DIR, "component_sizes.png"), "Track ")
    write_stats(G, sccs, wccs, STATS_FILE,
                "MusicBrainz Sample/Remix Network — track-level (F1)",
                top_field="track")

    print("\n=== Artist graph plots/stats ===")
    plot_degree(H, os.path.join(OUT_DIR, "artist_degree_distribution.png"), "Artist ")
    sccs2, wccs2 = plot_components(H, os.path.join(OUT_DIR, "artist_component_sizes.png"),
                                   "Artist ")
    write_stats(H, sccs2, wccs2, ARTIST_STATS_FILE,
                "MusicBrainz Sample/Remix Network — artist-level (F1)",
                top_field="artist")

    print("\n=== Decade graph plots/stats ===")
    plot_degree(H_dec, os.path.join(OUT_DIR, "decade_degree_distribution.png"), "Decade ")
    sccs3, wccs3 = plot_components(H_dec,
                                   os.path.join(OUT_DIR, "decade_component_sizes.png"),
                                   "Decade ")
    write_stats(H_dec, sccs3, wccs3, DECADE_STATS_FILE,
                "MusicBrainz Sample/Remix Network — decade-level (F1)",
                top_field="decade")

    print("\n=== Country graph plots/stats ===")
    plot_degree(H_co, os.path.join(OUT_DIR, "country_degree_distribution.png"), "Country ")
    sccs4, wccs4 = plot_components(H_co,
                                   os.path.join(OUT_DIR, "country_component_sizes.png"),
                                   "Country ")
    write_stats(H_co, sccs4, wccs4, COUNTRY_STATS_FILE,
                "MusicBrainz Sample/Remix Network — country-level (F1)",
                top_field="country")

    print("\nDONE.")


if __name__ == "__main__":
    main()
