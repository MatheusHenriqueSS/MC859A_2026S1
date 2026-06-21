#!/usr/bin/env python3
"""F2 — Pillar 1: influence ranking & roles.

Edge orientation is A -> B = "A is derived from B" (A samples/remixes B), so B
(the source) accumulates rank from its derivatives.

  - PageRank on the graph as-is  -> foundational SOURCE influence (sampled a lot).
  - PageRank on the reversed graph -> prolific SAMPLER / curatorial reach.
  - HITS on the giant WCC: authorities = canonical sources, hubs = samplers.
    (HITS is degenerate on the full disconnected graph, so we restrict it.)
  - in- vs out-strength role quadrant (artist graph).
  - sampled betweenness on the artist giant WCC -> bridge artists.

Artist-level runs first drop aggregation super-nodes ([unknown], Various
Artists, ...). Runs on the F1 graphs (no v2 needed). Outputs to analysis_output/.
"""

from __future__ import annotations

import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import networkx as nx

import analyze_common as C


def _ranked(d):
    return sorted(d.items(), key=lambda kv: kv[1], reverse=True)


def pagerank_rankings(G, label_fn, name):
    print(f"[{name}] PageRank (source influence)...", flush=True)
    pr = nx.pagerank(G, weight="weight")
    print(f"[{name}] PageRank (reversed = sampler reach)...", flush=True)
    pr_rev = nx.pagerank(G.reverse(copy=True), weight="weight")
    ins, outs = C.in_strength(G), C.out_strength(G)
    C.save_ranking([(label_fn(G, n), f"{v:.6g}", ins[n]) for n, v in _ranked(pr)],
                   ["entity", "pagerank", "in_strength"],
                   f"infl_{name}_pagerank_source")
    C.save_ranking([(label_fn(G, n), f"{v:.6g}", outs[n]) for n, v in _ranked(pr_rev)],
                   ["entity", "pagerank_rev", "out_strength"],
                   f"infl_{name}_pagerank_sampler")
    return {"pagerank_top": [label_fn(G, n) for n, _ in _ranked(pr)[:10]],
            "sampler_top": [label_fn(G, n) for n, _ in _ranked(pr_rev)[:10]],
            "ins": ins, "outs": outs}


def hits_on_giant(G, label_fn, name):
    wcc = max(nx.weakly_connected_components(G), key=len)
    Hg = G.subgraph(wcc).copy()
    print(f"[{name}] HITS on giant WCC ({Hg.number_of_nodes():,} nodes)...", flush=True)
    try:
        hubs, auth = nx.hits(Hg, max_iter=3000, normalized=True)
    except nx.PowerIterationFailedConvergence:
        hubs, auth = nx.hits(Hg, max_iter=10000, tol=1e-6, normalized=True)
    ins, outs = C.in_strength(Hg), C.out_strength(Hg)
    C.save_ranking([(label_fn(Hg, n), f"{v:.6g}", ins[n]) for n, v in _ranked(auth)],
                   ["entity", "authority", "in_strength"],
                   f"infl_{name}_hits_authority")
    C.save_ranking([(label_fn(Hg, n), f"{v:.6g}", outs[n]) for n, v in _ranked(hubs)],
                   ["entity", "hub", "out_strength"],
                   f"infl_{name}_hits_hub")
    return {"authority_top": [label_fn(Hg, n) for n, _ in _ranked(auth)[:10]],
            "hub_top": [label_fn(Hg, n) for n, _ in _ranked(hubs)[:10]]}


def role_quadrant(G, ins, outs, label_fn, name):
    print(f"[{name}] role quadrant plot...", flush=True)
    xs = [ins[n] + 0.5 for n in G]
    ys = [outs[n] + 0.5 for n in G]
    plt.figure(figsize=(8, 8))
    plt.scatter(xs, ys, s=6, alpha=0.25, color="steelblue")
    plt.xscale("log")
    plt.yscale("log")
    plt.xlabel("in-strength  (sampled by others)  →")
    plt.ylabel("out-strength  (samples others)  →")
    plt.title(f"Role quadrant — {name}-level\n(right = source/authority, top = sampler/hub)")
    top_in = sorted(G, key=lambda n: ins[n], reverse=True)[:8]
    top_out = sorted(G, key=lambda n: outs[n], reverse=True)[:8]
    for n in set(top_in) | set(top_out):
        plt.annotate(label_fn(G, n), (ins[n] + 0.5, outs[n] + 0.5),
                     fontsize=7, alpha=0.9)
    plt.grid(True, which="both", alpha=0.2)
    plt.tight_layout()
    p = os.path.join(C.OUT, f"infl_{name}_role_quadrant.png")
    plt.savefig(p, dpi=150)
    plt.close()
    print(f"  saved: {os.path.basename(p)}", flush=True)


def bridge_betweenness(H, label_fn, k=800):
    wcc = max(nx.weakly_connected_components(H), key=len)
    Hg = H.subgraph(wcc).copy()
    kk = min(k, Hg.number_of_nodes() - 1)
    print(f"[artist] betweenness on giant WCC ({Hg.number_of_nodes():,} nodes), k={kk}...",
          flush=True)
    bc = nx.betweenness_centrality(Hg, k=kk, seed=42, normalized=True)
    C.save_ranking([(label_fn(Hg, n), f"{v:.6g}") for n, v in _ranked(bc)],
                   ["artist", "betweenness"], "infl_artist_betweenness")
    return [label_fn(Hg, n) for n, _ in _ranked(bc)[:10]]


def main():
    summary = {}
    print("=== ARTIST-LEVEL ===", flush=True)
    H = C.drop_placeholders(C.load(C.ARTIST))
    a = pagerank_rankings(H, C.artist_label, "artist")
    role_quadrant(H, a["ins"], a["outs"], C.artist_label, "artist")
    h = hits_on_giant(H, C.artist_label, "artist")
    summary["artist"] = {**{k: a[k] for k in ("pagerank_top", "sampler_top")}, **h,
                         "betweenness_top": bridge_betweenness(H, C.artist_label)}

    print("\n=== TRACK-LEVEL ===", flush=True)
    G = C.load_track(prefer_v2=True)
    t = pagerank_rankings(G, C.track_label, "track")
    h2 = hits_on_giant(G, C.track_label, "track")
    summary["track"] = {**{k: t[k] for k in ("pagerank_top", "sampler_top")}, **h2}

    C.save_summary("influence", summary)
    print("\nDONE (influence).", flush=True)


if __name__ == "__main__":
    main()
