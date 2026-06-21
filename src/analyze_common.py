#!/usr/bin/env python3
"""Shared helpers for the F2 analysis scripts.

Loads the F1/F2 GraphML instances and provides small utilities for writing
ranked tables (CSV + human-readable TXT) and JSON headline summaries into
`analysis_output/`.
"""

from __future__ import annotations

import csv
import json
import os

import networkx as nx

import graphs  # logical-name -> file resolver (uniform gzipped GraphML)

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GO = os.path.join(REPO, "graph_output")
OUT = os.path.join(REPO, "analysis_output")
os.makedirs(OUT, exist_ok=True)

# Resolved via the registry so callers stay agnostic to format/location.
TRACK = graphs.path("track", must_exist=False)
TRACK_V2 = graphs.path("track_v2", must_exist=False)
ARTIST = graphs.path("artist", must_exist=False)
DECADE = graphs.path("decade", must_exist=False)
COUNTRY = graphs.path("country", must_exist=False)


# Aggregation/placeholder artist credits that merge many distinct real entities
# into one super-node — excluded from artist-level centrality & community work.
PLACEHOLDER_NAMES = {
    "[unknown]", "various artists", "[no artist]", "[anonymous]",
    "[traditional]", "[dialogue]", "unknown", "[data]", "[soundtrack]",
    "[nameless]", "[unknown artist]", "[mixed]",
}


def is_placeholder(name) -> bool:
    return (name or "").strip().lower() in PLACEHOLDER_NAMES


def drop_placeholders(G) -> nx.DiGraph:
    """Return a copy of an artist-level graph with aggregation super-nodes removed."""
    bad = [n for n in G if is_placeholder(G.nodes[n].get("artist", ""))]
    if not bad:
        return G
    G = G.copy()
    G.remove_nodes_from(bad)
    print(f"  dropped {len(bad)} placeholder/aggregation nodes "
          f"-> {G.number_of_nodes():,} nodes / {G.number_of_edges():,} edges", flush=True)
    return G


def load(path) -> nx.DiGraph:
    print(f"  loading {os.path.basename(path)} ...", flush=True)
    G = nx.read_graphml(path)
    print(f"    {G.number_of_nodes():,} nodes / {G.number_of_edges():,} edges", flush=True)
    return G


def load_track(prefer_v2: bool = True) -> nx.DiGraph:
    """Load the track graph; prefer the year/country-enriched v2 if present."""
    if prefer_v2 and graphs.exists("track_v2"):
        return graphs.load("track_v2")
    return graphs.load("track")


def track_label(G, n) -> str:
    d = G.nodes[n]
    a = (d.get("artist", "") or "").strip()
    t = (d.get("title", "") or "").strip()
    if a and t:
        return f"{a} — {t}"
    return a or t or str(n)


def artist_label(G, n) -> str:
    return (G.nodes[n].get("artist", "") or "").strip() or str(n)


def in_strength(G) -> dict:
    s = {n: 0 for n in G}
    for _, v, d in G.edges(data=True):
        s[v] += int(d.get("weight", 1))
    return s


def out_strength(G) -> dict:
    s = {n: 0 for n in G}
    for u, _, d in G.edges(data=True):
        s[u] += int(d.get("weight", 1))
    return s


def save_ranking(items, header, stub, label_fn=None, top=50):
    """items: list of (node, value) or (node, v1, v2, ...). Writes CSV (full) + TXT (top)."""
    csv_path = os.path.join(OUT, stub + ".csv")
    txt_path = os.path.join(OUT, stub + ".txt")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for row in items:
            w.writerow(row)
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("  ".join(header) + "\n")
        f.write("-" * 70 + "\n")
        for row in items[:top]:
            f.write("  ".join(str(x) for x in row) + "\n")
    print(f"  saved: {os.path.basename(csv_path)} ({len(items)} rows) + .txt (top {top})", flush=True)


def save_summary(name, d):
    path = os.path.join(OUT, name + "_summary.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(d, f, indent=2, ensure_ascii=False)
    print(f"  saved: {os.path.basename(path)}", flush=True)
