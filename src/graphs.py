#!/usr/bin/env python3
"""Single registry + resolver for every graph instance.

Analysis code loads graphs by LOGICAL NAME (e.g. graphs.load("track_v2")), never
by path or file format. This decouples *what* a graph is from *how/where* it is
stored, so the storage strategy can scale without touching any caller:

  - Uniform on-disk format: gzipped GraphML (`*.graphml.gz`) for every instance
    regardless of size. networkx reads/writes `.gz` transparently and it gives
    ~8x size headroom under hosting limits.
  - If one instance ever outgrows the repo, move just that file to a GitHub
    Release / Zenodo and change a single entry here — no analysis code changes.

Run from `src/` so this module is importable by the analyze_*.py scripts.
"""

from __future__ import annotations

import os

import networkx as nx

GO = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                  "graph_output")

# logical name -> base filename (without extension)
REGISTRY = {
    "track":    "mb_sample_graph",      # F1 track-level
    "track_v2": "mb_sample_graph_v2",   # F2 track-level (+ year, country)
    "artist":   "mb_artist_graph",
    "decade":   "mb_decade_graph",
    "country":  "mb_country_graph",
    "genre":    "mb_genre_graph",       # F2 Phase 2 (may not exist yet)
}

CANONICAL_EXT = ".graphml.gz"           # what we always write
_READ_EXTS = (".graphml.gz", ".graphml")  # prefer compressed, fall back to raw


def _base(name: str) -> str:
    if name not in REGISTRY:
        raise KeyError(f"unknown graph '{name}'; known: {sorted(REGISTRY)}")
    return os.path.join(GO, REGISTRY[name])


def path(name: str, *, must_exist: bool = True) -> str:
    base = _base(name)
    for ext in _READ_EXTS:
        if os.path.exists(base + ext):
            return base + ext
    if must_exist:
        raise FileNotFoundError(f"no file for graph '{name}' at {base}{CANONICAL_EXT}")
    return base + CANONICAL_EXT


def exists(name: str) -> bool:
    base = _base(name)
    return any(os.path.exists(base + e) for e in _READ_EXTS)


def load(name: str) -> nx.DiGraph:
    p = path(name)
    print(f"  loading {os.path.basename(p)} ...", flush=True)
    G = nx.read_graphml(p)
    print(f"    {G.number_of_nodes():,} nodes / {G.number_of_edges():,} edges", flush=True)
    return G


def save(G, name: str) -> str:
    """Write a graph under its canonical gzipped-GraphML name."""
    p = _base(name) + CANONICAL_EXT
    nx.write_graphml(G, p)
    print(f"  wrote {os.path.basename(p)} "
          f"({G.number_of_nodes():,} nodes / {G.number_of_edges():,} edges)", flush=True)
    return p
