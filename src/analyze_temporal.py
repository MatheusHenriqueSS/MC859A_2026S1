#!/usr/bin/env python3
"""F2 — Pillar 3: temporal influence flow (needs the v2 track graph).

Edge A -> B = "A derived from B". With a year on each node we can measure how
influence travels through time:

  - influence lag = year(derivative) - year(source); distribution + "half-life".
  - temporal consistency: share of edges with derivative >= source year.
  - decade -> decade flow heatmap (from the decade graph).
  - evergreen sources: recordings whose derivatives span the most decades.
  - longest derivation lineages (DAG longest path, with titles).
  - influence footprint: transitive set of derivatives per top source.

Outputs to analysis_output/.
"""

from __future__ import annotations

import os
from collections import Counter, defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import networkx as nx

import analyze_common as C


def _year(G, n):
    y = G.nodes[n].get("year", -1)
    return y if isinstance(y, int) and y > 0 else None


def influence_lag(G):
    lags = []
    consistent = same = violate = 0
    sample_lags = []
    for u, v, d in G.edges(data=True):  # u derivative, v source
        yu, yv = _year(G, u), _year(G, v)
        if yu is None or yv is None:
            continue
        lag = yu - yv
        lags.append(lag)
        if lag > 0:
            consistent += 1
        elif lag == 0:
            same += 1
        else:
            violate += 1
        if "sample" in str(d.get("types", "")).split(";"):
            sample_lags.append(lag)
    lags = np.array(lags)
    pos = lags[lags > 0]
    stats = {
        "edges_with_both_years": int(lags.size),
        "median_lag": int(np.median(lags)) if lags.size else None,
        "mean_lag": round(float(lags.mean()), 2) if lags.size else None,
        "median_positive_lag_years": int(np.median(pos)) if pos.size else None,
        "consistent_pct": round(100 * consistent / lags.size, 1) if lags.size else None,
        "same_year_pct": round(100 * same / lags.size, 1) if lags.size else None,
        "violation_pct": round(100 * violate / lags.size, 1) if lags.size else None,
        "sample_median_lag": int(np.median(sample_lags)) if sample_lags else None,
    }
    # plot lag histogram (clip for readability)
    plt.figure(figsize=(10, 5))
    clip = lags[(lags >= -10) & (lags <= 70)]
    plt.hist(clip, bins=80, color="teal", alpha=0.8)
    plt.axvline(0, color="red", lw=1, ls="--")
    plt.xlabel("influence lag = year(derivative) − year(source)")
    plt.ylabel("number of edges")
    plt.title("Temporal influence lag (sample/remix/etc.)")
    plt.tight_layout()
    p = os.path.join(C.OUT, "temporal_lag_hist.png")
    plt.savefig(p, dpi=150)
    plt.close()
    print(f"  saved: {os.path.basename(p)}", flush=True)
    return stats


def decade_heatmap():
    if not os.path.exists(C.DECADE):
        return
    D = C.load(C.DECADE)
    decades = sorted(D.nodes(), key=lambda s: int(str(s).rstrip("s")))
    idx = {d: i for i, d in enumerate(decades)}
    M = np.zeros((len(decades), len(decades)))
    for u, v, d in D.edges(data=True):  # u deriv decade, v source decade
        M[idx[u], idx[v]] = int(d.get("weight", 1))
    plt.figure(figsize=(9, 7.5))
    plt.imshow(np.log1p(M), cmap="magma", aspect="auto")
    plt.colorbar(label="log(1 + #edges)")
    plt.xticks(range(len(decades)), decades, rotation=45)
    plt.yticks(range(len(decades)), decades)
    plt.xlabel("source decade")
    plt.ylabel("derivative decade")
    plt.title("Decade → decade derivation flow")
    plt.tight_layout()
    p = os.path.join(C.OUT, "temporal_decade_heatmap.png")
    plt.savefig(p, dpi=150)
    plt.close()
    print(f"  saved: {os.path.basename(p)}", flush=True)


def evergreen_sources(G, top=40):
    """Sources whose derivatives span the most distinct decades."""
    spans = {}
    for u, v in G.edges():  # v is source
        yu = _year(G, u)
        if yu is None:
            continue
        spans.setdefault(v, set()).add((yu // 10) * 10)
    rows = []
    for v, decs in spans.items():
        if len(decs) >= 2:
            rows.append((C.track_label(G, v), len(decs),
                         f"{min(decs)}s-{max(decs)}s", _year(G, v) or "?",
                         G.in_degree(v)))
    rows.sort(key=lambda r: (r[1], r[4]), reverse=True)
    C.save_ranking(rows, ["source", "n_decades_of_derivatives", "decade_span",
                          "source_year", "in_degree"],
                   "temporal_evergreen_sources", top=top)
    return [r[0] for r in rows[:10]]


def longest_lineages(G):
    cond = nx.condensation(G)
    path = nx.dag_longest_path(cond)
    members = cond.graph["mapping"]
    inv = defaultdict(list)
    for node, comp in members.items():
        inv[comp].append(node)
    # render the chain as track labels (one representative per condensed node)
    chain = [C.track_label(G, inv[c][0]) for c in path]
    p = os.path.join(C.OUT, "temporal_longest_lineage.txt")
    with open(p, "w", encoding="utf-8") as f:
        f.write(f"Longest derivation lineage: {len(path)-1} hops "
                f"(derivative -> ... -> original)\n" + "=" * 60 + "\n")
        for i, lbl in enumerate(chain):
            yr = ""
            f.write(f"{i:>2}. {lbl}\n")
    print(f"  saved: {os.path.basename(p)} ({len(path)-1} hops)", flush=True)
    return len(path) - 1


def influence_footprint(G, top=25):
    """For top sources by in-strength, count transitive derivatives (ancestors,
    since edges point derivative -> source)."""
    ins = C.in_strength(G)
    top_sources = sorted(ins, key=lambda n: ins[n], reverse=True)[:top]
    rows = []
    for v in top_sources:
        foot = len(nx.ancestors(G, v))
        rows.append((C.track_label(G, v), ins[v], G.in_degree(v), foot))
    rows.sort(key=lambda r: r[3], reverse=True)
    C.save_ranking(rows, ["source", "in_strength", "direct_in_degree",
                          "transitive_footprint"],
                   "temporal_influence_footprint", top=top)
    return rows[:5]


def main():
    G = C.load_track(prefer_v2=True)
    if not any(isinstance(G.nodes[n].get("year"), int) for n in list(G.nodes)[:50]):
        raise SystemExit("track graph has no year attr — run build_track_v2.py first")
    print("=== influence lag ===", flush=True)
    lag = influence_lag(G)
    print("  ", lag, flush=True)
    print("=== decade heatmap ===", flush=True)
    decade_heatmap()
    print("=== evergreen sources ===", flush=True)
    evergreen_sources(G)
    print("=== longest lineage ===", flush=True)
    hops = longest_lineages(G)
    print("=== influence footprint ===", flush=True)
    influence_footprint(G)
    C.save_summary("temporal", {"lag": lag, "longest_lineage_hops": hops})
    print("\nDONE (temporal).", flush=True)


if __name__ == "__main__":
    main()
