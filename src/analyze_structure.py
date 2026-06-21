#!/usr/bin/env python3
"""F2 — Pillar 5: structural characterization.

  - Power-law vs lognormal/exponential fit of the in-degree distribution
    (rigorously tests the F1 "scale-free" claim) via the `powerlaw` package.
  - Degree assortativity, reciprocity, global clustering (transitivity).
  - Attribute homophily: country and year (track graph, needs v2).
  - Per-relation-type subgraph comparison (sample = deep chains vs remix =
    shallow stars): size, giant WCC share, mean in-degree, longest lineage.

Outputs to analysis_output/.
"""

from __future__ import annotations

import networkx as nx
import numpy as np
import powerlaw

import analyze_common as C

TYPES = ["sample", "remix", "edit", "dj_mix", "mashup"]


def fit_powerlaw(degseq, name):
    deg = [int(d) for d in degseq if d and d > 0]
    fit = powerlaw.Fit(deg, discrete=True, verbose=False)
    R_ln, p_ln = fit.distribution_compare("power_law", "lognormal")
    R_exp, p_exp = fit.distribution_compare("power_law", "exponential")
    res = {
        "n_positive": len(deg),
        "alpha": round(float(fit.power_law.alpha), 3),
        "xmin": int(fit.power_law.xmin),
        "R_vs_lognormal": round(float(R_ln), 3), "p_vs_lognormal": round(float(p_ln), 4),
        "R_vs_exponential": round(float(R_exp), 3), "p_vs_exp": round(float(p_exp), 4),
    }
    # R>0 favors power-law; for lognormal a non-significant p means "can't
    # distinguish" — the usual real-world verdict for heavy tails.
    print(f"  [{name}] alpha={res['alpha']} xmin={res['xmin']} "
          f"PL-vs-LN R={res['R_vs_lognormal']} p={res['p_vs_lognormal']} | "
          f"PL-vs-Exp R={res['R_vs_exponential']} p={res['p_vs_exp']}", flush=True)
    return res


def basic_structure(G, name):
    print(f"  [{name}] assortativity / reciprocity / clustering...", flush=True)
    try:
        deg_assort = round(nx.degree_assortativity_coefficient(G), 4)
    except Exception:
        deg_assort = None
    recip = round(nx.reciprocity(G), 5)
    trans = round(nx.transitivity(G), 5)
    return {"degree_assortativity": deg_assort, "reciprocity": recip,
            "transitivity": trans}


def longest_chain(G):
    """Longest derivation chain (edges) via condensation -> DAG longest path."""
    cond = nx.condensation(G)
    return int(nx.dag_longest_path_length(cond))


def type_subgraph(G, t):
    keep = [(u, v) for u, v, d in G.edges(data=True)
            if t in str(d.get("types", "")).split(";")]
    S = nx.DiGraph()
    S.add_edges_from(keep)
    if S.number_of_nodes() == 0:
        return None
    wccs = list(nx.weakly_connected_components(S))
    largest = max((len(c) for c in wccs), default=0)
    n = S.number_of_nodes()
    mean_in = S.number_of_edges() / n
    return {
        "type": t, "nodes": n, "edges": S.number_of_edges(),
        "largest_wcc": largest, "largest_wcc_pct": round(100 * largest / n, 1),
        "n_wcc": len(wccs), "mean_in_degree": round(mean_in, 3),
        "longest_chain": longest_chain(S),
    }


def main():
    summary = {}
    print("=== TRACK structure ===", flush=True)
    G = C.load_track(prefer_v2=True)
    summary["track"] = {
        "powerlaw_in_degree": fit_powerlaw([d for _, d in G.in_degree()], "track in-deg"),
        **basic_structure(G, "track"),
        "longest_chain_overall": longest_chain(G),
    }
    # attribute homophily (needs v2 attrs)
    if any("country" in G.nodes[n] for n in list(G.nodes)[:5]):
        print("  [track] attribute homophily (country / year)...", flush=True)
        # restrict to nodes with the attribute for a fair coefficient
        Gc = G.subgraph([n for n in G if (G.nodes[n].get("country") or "")]).copy()
        try:
            summary["track"]["country_homophily"] = round(
                nx.attribute_assortativity_coefficient(Gc, "country"), 4)
        except Exception as e:
            summary["track"]["country_homophily"] = f"err: {e}"
        Gy = G.subgraph([n for n in G if isinstance(G.nodes[n].get("year"), int)
                         and G.nodes[n]["year"] > 0]).copy()
        try:
            summary["track"]["year_assortativity"] = round(
                nx.numeric_assortativity_coefficient(Gy, "year"), 4)
        except Exception as e:
            summary["track"]["year_assortativity"] = f"err: {e}"

    print("=== per-relation-type subgraphs (track) ===", flush=True)
    rows = []
    for t in TYPES:
        r = type_subgraph(G, t)
        if r:
            rows.append((r["type"], r["nodes"], r["edges"], r["largest_wcc_pct"],
                         r["mean_in_degree"], r["longest_chain"]))
            print(f"  {t}: {r['nodes']:,}n {r['edges']:,}e "
                  f"giantWCC={r['largest_wcc_pct']}% chain={r['longest_chain']}", flush=True)
    C.save_ranking(rows, ["type", "nodes", "edges", "giant_wcc_pct",
                          "mean_in_degree", "longest_chain"],
                   "struct_per_type", top=10)

    print("=== ARTIST structure ===", flush=True)
    H = C.drop_placeholders(C.load(C.ARTIST))
    summary["artist"] = {
        "powerlaw_in_degree": fit_powerlaw([d for _, d in H.in_degree()], "artist in-deg"),
        **basic_structure(H, "artist"),
    }

    C.save_summary("structure", summary)
    print("\nDONE (structure).", flush=True)


if __name__ == "__main__":
    main()
