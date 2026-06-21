#!/usr/bin/env python3
"""F2 Phase 2 — genre influence flow & homophily (needs mb_genre_graph).

Mirrors the geography analysis at the genre level:
  - net influence balance per genre (sampled by others vs samples others).
  - homophily: within-genre edge share vs an independence null
    ("do genres mostly sample within themselves?").
  - top cross-genre corridors ("does hip-hop sample funk?").
  - genre -> genre flow heatmap.

Outputs to analysis_output/.
"""

from __future__ import annotations

import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

import analyze_common as C
import graphs


def main():
    G = graphs.load("genre")
    ins, outs = C.in_strength(G), C.out_strength(G)
    W = sum(int(d.get("weight", 1)) for _, _, d in G.edges(data=True))
    self_w = sum(int(d.get("weight", 1)) for u, v, d in G.edges(data=True) if u == v)

    rows = sorted(((g, ins[g], outs[g], ins[g] - outs[g]) for g in G),
                  key=lambda r: r[3], reverse=True)
    C.save_ranking(rows, ["genre", "in_strength_sampled", "out_strength_samples",
                          "net_influence_in_minus_out"], "genre_net_flow", top=40)

    obs = self_w / W
    exp = sum((ins[g] / W) * (outs[g] / W) for g in G)
    homophily = {
        "within_genre_share_observed": round(obs, 4),
        "within_genre_share_expected_null": round(exp, 4),
        "homophily_ratio": round(obs / exp, 2) if exp else None,
    }
    print("  homophily:", homophily, flush=True)

    corr = sorted(((u, v, int(d.get("weight", 1)))
                   for u, v, d in G.edges(data=True) if u != v),
                  key=lambda r: r[2], reverse=True)
    C.save_ranking([(f"{u} → {v}", w) for u, v, w in corr],
                   ["corridor_deriv_draws_from_source", "weight"],
                   "genre_corridors", top=60)

    # heatmap of the top genres by total activity
    top = [g for g, _ in sorted(((g, ins[g] + outs[g]) for g in G),
                                key=lambda r: r[1], reverse=True)[:25]]
    idx = {g: i for i, g in enumerate(top)}
    M = np.zeros((len(top), len(top)))
    for u, v, d in G.edges(data=True):
        if u in idx and v in idx:
            M[idx[u], idx[v]] = int(d.get("weight", 1))
    plt.figure(figsize=(12, 10))
    plt.imshow(np.log1p(M), cmap="magma", aspect="auto")
    plt.colorbar(label="log(1 + #edges)")
    plt.xticks(range(len(top)), top, rotation=90, fontsize=7)
    plt.yticks(range(len(top)), top, fontsize=7)
    plt.xlabel("source genre")
    plt.ylabel("derivative genre (draws from)")
    plt.title("Genre → genre derivation flow (top 25)")
    plt.tight_layout()
    p = os.path.join(C.OUT, "genre_flow_heatmap.png")
    plt.savefig(p, dpi=150)
    plt.close()
    print(f"  saved: {os.path.basename(p)}", flush=True)

    C.save_summary("genre", {
        **homophily,
        "n_genres": G.number_of_nodes(),
        "top_net_sources": [(r[0], r[3]) for r in rows[:8]],
        "top_net_importers": [(r[0], r[3]) for r in rows[-8:]],
        "top_cross_genre_corridors": [(f"{u}→{v}", w) for u, v, w in corr[:12]],
    })
    print("\nDONE (genre).", flush=True)


if __name__ == "__main__":
    main()
