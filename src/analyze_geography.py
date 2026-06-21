#!/usr/bin/env python3
"""F2 — Pillar 4: geographic influence flow & homophily (country graph).

Edge c0 -> c1 = "an artist from c0 derived from an artist in c1" (c0 draws from
c1). The country graph keeps within-country self-loops.

  - net influence balance: in-strength (sampled by others) − out-strength
    (samples others) per country -> net exporters vs importers of material.
  - homophily: observed within-country edge share vs an independence null.
  - top cross-country corridors.
  - Brazil case study (who Brazil samples / who samples Brazil).
  - flow heatmap for the top countries.

Outputs to analysis_output/.
"""

from __future__ import annotations

import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

import analyze_common as C


def main():
    Cg = C.load(C.COUNTRY)
    ins, outs = C.in_strength(Cg), C.out_strength(Cg)
    W = sum(int(d.get("weight", 1)) for _, _, d in Cg.edges(data=True))
    self_w = sum(int(d.get("weight", 1)) for u, v, d in Cg.edges(data=True) if u == v)

    # --- net influence balance (in = sampled by others; out = samples others) ---
    rows = []
    for c in Cg:
        rows.append((c, ins[c], outs[c], ins[c] - outs[c]))
    rows.sort(key=lambda r: r[3], reverse=True)
    C.save_ranking(rows, ["country", "in_strength_sampled", "out_strength_samples",
                          "net_influence_in_minus_out"], "geo_net_flow", top=40)

    # --- homophily ---
    obs_within = self_w / W
    exp_within = sum((ins[c] / W) * (outs[c] / W) for c in Cg)
    homophily = {
        "within_country_share_observed": round(obs_within, 4),
        "within_country_share_expected_null": round(exp_within, 4),
        "homophily_ratio": round(obs_within / exp_within, 2) if exp_within else None,
    }
    print("  homophily:", homophily, flush=True)

    # --- top cross-country corridors (exclude self-loops) ---
    corr = [(u, v, int(d.get("weight", 1)))
            for u, v, d in Cg.edges(data=True) if u != v]
    corr.sort(key=lambda r: r[2], reverse=True)
    C.save_ranking([(f"{u} → {v}", w) for u, v, w in corr],
                   ["corridor_deriv_draws_from_source", "weight"],
                   "geo_corridors", top=40)

    # --- Brazil case study ---
    BR = "Brazil"
    if BR in Cg:
        out_n = sorted(((v, int(d.get("weight", 1))) for _, v, d in Cg.out_edges(BR, data=True)),
                       key=lambda r: r[1], reverse=True)
        in_n = sorted(((u, int(d.get("weight", 1))) for u, _, d in Cg.in_edges(BR, data=True)),
                      key=lambda r: r[1], reverse=True)
        with open(os.path.join(C.OUT, "geo_brazil.txt"), "w", encoding="utf-8") as f:
            f.write(f"BRAZIL — net influence (in−out) = {ins[BR]-outs[BR]} "
                    f"(sampled {ins[BR]} / samples {outs[BR]})\n\n")
            f.write("Brazil draws FROM (Brazil derivative → source country):\n")
            for v, w in out_n[:20]:
                f.write(f"  {w:5d}  {v}\n")
            f.write("\nWho draws FROM Brazil (source = Brazil):\n")
            for u, w in in_n[:20]:
                f.write(f"  {w:5d}  {u}\n")
        print(f"  saved: geo_brazil.txt (net {ins[BR]-outs[BR]})", flush=True)

    # --- flow heatmap (top countries) ---
    top = [c for c, _ in sorted(((c, ins[c] + outs[c]) for c in Cg),
                                key=lambda r: r[1], reverse=True)[:20]]
    idx = {c: i for i, c in enumerate(top)}
    M = np.zeros((len(top), len(top)))
    for u, v, d in Cg.edges(data=True):
        if u in idx and v in idx:
            M[idx[u], idx[v]] = int(d.get("weight", 1))
    plt.figure(figsize=(11, 9))
    plt.imshow(np.log1p(M), cmap="viridis", aspect="auto")
    plt.colorbar(label="log(1 + #edges)")
    plt.xticks(range(len(top)), top, rotation=90, fontsize=8)
    plt.yticks(range(len(top)), top, fontsize=8)
    plt.xlabel("source country")
    plt.ylabel("derivative country (draws from)")
    plt.title("Country → country derivation flow (top 20)")
    plt.tight_layout()
    p = os.path.join(C.OUT, "geo_country_heatmap.png")
    plt.savefig(p, dpi=150)
    plt.close()
    print(f"  saved: {os.path.basename(p)}", flush=True)

    C.save_summary("geography", {
        **homophily,
        "top_net_sources": [(r[0], r[3]) for r in rows[:5]],
        "top_net_importers": [(r[0], r[3]) for r in rows[-5:]],
        "top_corridors": [(f"{u}→{v}", w) for u, v, w in corr[:5]],
    })
    print("\nDONE (geography).", flush=True)


if __name__ == "__main__":
    main()
