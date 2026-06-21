#!/usr/bin/env python3
"""Re-emit the track-level graph with `year` and `country` on each node (F2).

The F1 track GraphML (`mb_sample_graph.graphml`) only carries
{title, artist, artist_id, popularity} on nodes — year and country live only on
the aggregated decade/country graphs. `build_mb_graph.py` already derives a year
and a country per recording in its enriched edge DataFrame, so this script just
reuses that pipeline and writes a node-enriched copy to a NEW file, leaving the
F1 artifacts byte-for-byte intact.

Output: graph_output/mb_sample_graph_v2.graphml
  node attrs: title, artist, artist_id, popularity, year (int; -1 = unknown),
              country (str; "" = unknown)
  edge attrs: types, weight   (identical semantics to F1)
"""

from __future__ import annotations

import os

import duckdb
import networkx as nx

import build_mb_graph as b  # reuse the tested F1 pipeline (no side effects on import)

OUT = os.path.join(b.OUT_DIR, "mb_sample_graph_v2.graphml.gz")  # uniform gz format


def _clean_year(v):
    try:
        if v is None or v != v:  # None / NaN
            return None
        y = int(v)
    except (TypeError, ValueError):
        return None
    return y if 1 <= y <= 2100 else None


def _clean_str(v):
    if v is None:
        return None
    try:
        if v != v:  # NaN
            return None
    except TypeError:
        pass
    s = str(v).strip()
    return s or None


def build_enriched_track_graph(df) -> nx.DiGraph:
    print("Building node-enriched track graph (year + country)...")
    G = nx.DiGraph()

    def ensure(rid, name, artist_id, artist_name, pop, year, country):
        nid = str(int(rid))
        if not G.has_node(nid):
            G.add_node(
                nid,
                title=str(name) if name is not None else "",
                artist=str(artist_name) if artist_name is not None else "",
                artist_id=int(artist_id)
                if artist_id == artist_id and artist_id is not None
                else -1,
                popularity=int(pop) if pop is not None else 0,
                year=-1,
                country="",
            )
        nd = G.nodes[nid]
        # backfill year/country from whichever endpoint row first has them
        if nd["year"] == -1:
            y = _clean_year(year)
            if y is not None:
                nd["year"] = y
        if nd["country"] == "":
            c = _clean_str(country)
            if c:
                nd["country"] = c

    for r in df.itertuples(index=False):
        ensure(r.entity0, r.e0_name, r.e0_artist, r.e0_artist_name, r.e0_pop,
               r.e0_year, r.e0_country)
        ensure(r.entity1, r.e1_name, r.e1_artist, r.e1_artist_name, r.e1_pop,
               r.e1_year, r.e1_country)
        a, bb = str(int(r.entity0)), str(int(r.entity1))
        et = b.TYPE_SHORT.get(r.edge_type, r.edge_type)
        if G.has_edge(a, bb):
            d = G.get_edge_data(a, bb)
            d["weight"] = d.get("weight", 1) + 1
            existing = set(t for t in d.get("types", "").split(";") if t)
            existing.add(et)
            d["types"] = ";".join(sorted(existing))
        else:
            G.add_edge(a, bb, types=et, weight=1)

    print(f"  {G.number_of_nodes():,} nodes, {G.number_of_edges():,} edges")
    with_year = sum(1 for _, d in G.nodes(data=True) if d.get("year", -1) != -1)
    with_co = sum(1 for _, d in G.nodes(data=True) if d.get("country", ""))
    n = G.number_of_nodes()
    print(f"  nodes with year:    {with_year:,} ({100*with_year/max(n,1):.1f}%)")
    print(f"  nodes with country: {with_co:,} ({100*with_co/max(n,1):.1f}%)")
    return G


def main() -> None:
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4")
    con.execute("PRAGMA memory_limit='6GB'")
    b.load_all(con)
    b.cast_columns(con)
    b.attach_geo_temporal(con)
    df = b.build_filtered_edges(con)
    print(f"\nFiltered DataFrame: {len(df):,} rows")
    G = build_enriched_track_graph(df)
    print(f"\nWriting -> {OUT}")
    nx.write_graphml(G, OUT)
    print("DONE.")


if __name__ == "__main__":
    main()
