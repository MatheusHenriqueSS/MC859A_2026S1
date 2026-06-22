#!/usr/bin/env python3
"""F2 — Brazil targeted study (track + artist granularity) on the CC0 backbone.

The country pillar (analyze_geography.py) treats Brazil as a single node. This
drills into the *recordings and artists*: which Brazilian material is most reused,
what Brazilian tracks themselves sample, the internal BR↔BR network, and the thin
but real cross-border reuse of Brazilian sources abroad.

Convention (track_v2 graph): edge A -> B means "A is derived from B" (A samples /
remixes / covers B). So for a Brazilian node:
  - IN-edges  (someone -> BR)  = the BR recording was USED AS A SOURCE ("sampled").
  - OUT-edges (BR -> someone)  = the BR recording DERIVES FROM another ("samples").

Everything here is reproducible from the public MusicBrainz dump. Outputs ->
analysis_output/ (rankings as CSV+TXT, a JSON summary, and a BR ego-subgraph
GraphML for Gephi).
"""

from __future__ import annotations

import os
from collections import Counter, defaultdict
from statistics import median

import networkx as nx

import analyze_common as C
import graphs

BR = "Brazil"
REL_TYPES = ("sample", "remix", "edit", "dj_mix", "mashup")


def country(G, n) -> str:
    return (G.nodes[n].get("country") or "") or "(unknown)"


def is_br(G, n) -> bool:
    return (G.nodes[n].get("country") or "") == BR


def year(G, n):
    y = G.nodes[n].get("year")
    try:
        return int(y)
    except (TypeError, ValueError):
        return None


def etypes(d) -> list[str]:
    return [t for t in (d.get("types", "") or "").split(";") if t]


def w(d) -> int:
    return int(d.get("weight", 1))


def label_ctry(G, n) -> str:
    """Track label with year + country, e.g. 'Tim Maia — Azul da Cor do Mar (1972) [Brazil]'."""
    base = C.track_label(G, n)
    y = year(G, n)
    return f"{base}" + (f" ({y})" if y else "") + f" [{country(G, n)}]"


def main():
    G = graphs.load("track_v2")
    br = {n for n in G if is_br(G, n)}
    print(f"  Brazilian-artist recordings: {len(br):,}", flush=True)

    out_e = [(u, v, d) for u, v, d in G.edges(data=True) if u in br]   # BR samples someone
    in_e = [(u, v, d) for u, v, d in G.edges(data=True) if v in br]    # someone samples BR
    internal = [(u, v, d) for u, v, d in out_e if v in br]             # BR <-> BR
    print(f"  edges: BR-as-deriv (samples) {len(out_e):,} | "
          f"BR-as-source (sampled) {len(in_e):,} | internal BR↔BR {len(internal):,}", flush=True)

    # ---- 1. Most-sampled Brazilian RECORDINGS (BR used as source) -------------
    # split each BR source's reuse into domestic (from BR) vs foreign (from non-BR).
    tot_in, dom_in, frn_in = Counter(), Counter(), Counter()
    for u, v, d in in_e:
        tot_in[v] += w(d)
        (dom_in if is_br(G, u) else frn_in)[v] += w(d)
    rows = [(label_ctry(G, n), tot_in[n], dom_in[n], frn_in[n])
            for n in sorted(tot_in, key=lambda n: (tot_in[n], frn_in[n]), reverse=True)]
    C.save_ranking(rows, ["brazilian_source_track", "times_sampled_total",
                          "by_brazil", "by_foreign"], "br_top_sampled_tracks", top=50)

    # most-sampled BR tracks that were reused ABROAD specifically
    rows_frn = [(label_ctry(G, n), frn_in[n], tot_in[n])
                for n in sorted(frn_in, key=lambda n: frn_in[n], reverse=True) if frn_in[n]]
    C.save_ranking(rows_frn, ["brazilian_source_track", "times_sampled_abroad",
                              "times_sampled_total"], "br_sampled_abroad", top=50)

    # ---- 2. What Brazilian tracks SAMPLE — their top sources ------------------
    src = Counter()
    for u, v, d in out_e:
        src[v] += w(d)
    rows = [(label_ctry(G, n), src[n]) for n in sorted(src, key=lambda n: src[n], reverse=True)]
    C.save_ranking(rows, ["source_track", "times_sampled_by_brazil"],
                   "br_top_sources", top=50)
    # foreign-only sources (what BR draws from abroad)
    rows_f = [(label_ctry(G, n), src[n])
              for n in sorted(src, key=lambda n: src[n], reverse=True) if not is_br(G, n)]
    C.save_ranking(rows_f, ["foreign_source_track", "times_sampled_by_brazil"],
                   "br_top_foreign_sources", top=50)

    # ---- 3. Brazilian ARTISTS most reused as sources -------------------------
    # aggregate BR sources by their artist (drop placeholder/aggregation credits).
    def artist_of(n):
        return (G.nodes[n].get("artist") or "").strip()

    art_in_tot, art_in_dom, art_in_frn = Counter(), Counter(), Counter()
    for u, v, d in in_e:
        a = artist_of(v)
        if not a or C.is_placeholder(a):
            continue
        art_in_tot[a] += w(d)
        (art_in_dom if is_br(G, u) else art_in_frn)[a] += w(d)
    rows = [(a, art_in_tot[a], art_in_dom[a], art_in_frn[a])
            for a in sorted(art_in_tot, key=lambda a: (art_in_tot[a], art_in_frn[a]), reverse=True)]
    C.save_ranking(rows, ["brazilian_artist", "times_sampled_total",
                          "by_brazil", "by_foreign"], "br_top_sampled_artists", top=50)

    # ---- 4. Brazilian ARTISTS who sample the most ----------------------------
    art_out = Counter()
    for u, v, d in out_e:
        a = artist_of(u)
        if a and not C.is_placeholder(a):
            art_out[a] += w(d)
    rows = [(a, art_out[a]) for a in sorted(art_out, key=lambda a: art_out[a], reverse=True)]
    C.save_ranking(rows, ["brazilian_artist", "times_it_samples_others"],
                   "br_top_sampling_artists", top=50)

    # ---- 5. International reuse, edge by edge (BR source -> foreign deriver) ---
    intl = [(label_ctry(G, v), label_ctry(G, u), country(G, u), ";".join(etypes(d)) or "?", w(d))
            for u, v, d in in_e if not is_br(G, u)]
    intl.sort(key=lambda r: r[4], reverse=True)
    C.save_ranking(intl, ["brazilian_source", "foreign_derivative",
                          "deriver_country", "relation", "weight"],
                   "br_international_reuse", top=80)

    # ---- 6. Edge-type mix, BR-as-deriv vs BR-as-source -----------------------
    mix_out, mix_in = Counter(), Counter()
    for *_, d in out_e:
        for t in etypes(d):
            mix_out[t] += 1
    for *_, d in in_e:
        for t in etypes(d):
            mix_in[t] += 1

    # ---- 7. Partner-country split (weight-aware) -----------------------------
    samples_from = Counter()
    sampled_by = Counter()
    for u, v, d in out_e:
        samples_from[country(G, v)] += w(d)
    for u, v, d in in_e:
        sampled_by[country(G, u)] += w(d)

    # ---- 8. Internal BR↔BR subgraph structure --------------------------------
    H = G.subgraph(br).copy()           # induced: BR nodes + internal edges
    H.remove_nodes_from([n for n in list(H) if H.degree(n) == 0])
    wcc = list(nx.weakly_connected_components(H))
    scc = list(nx.strongly_connected_components(H))
    internal_mix = Counter()
    for *_, d in internal:
        for t in etypes(d):
            internal_mix[t] += 1

    # ---- 9. Temporal lag (year of derivative − year of source) ---------------
    def lags(edges):
        out = []
        for u, v, d in edges:
            yu, yv = year(G, u), year(G, v)
            if yu is not None and yv is not None:
                out.extend([yu - yv] * w(d))
        return out

    lag_out = lags(out_e)   # how old is the material BR samples
    lag_in = lags(in_e)     # how long after a BR source until it is reused

    # ---- 10. Export BR ego-subgraph for Gephi --------------------------------
    keep = set(br)
    for u, v, d in out_e + in_e:
        keep.add(u); keep.add(v)
    ego = nx.DiGraph()
    for n in keep:
        ego.add_node(n, **G.nodes[n], is_brazil=int(is_br(G, n)))
    for u, v, d in out_e + in_e:
        ego.add_edge(u, v, **d)
    ego_path = os.path.join(C.OUT, "br_subgraph.graphml.gz")
    nx.write_graphml(ego, ego_path)
    print(f"  saved: {os.path.basename(ego_path)} "
          f"({ego.number_of_nodes():,} nodes / {ego.number_of_edges():,} edges)", flush=True)

    # ---- headline summary -----------------------------------------------------
    def topc(counter, k=8):
        return [[c, n] for c, n in counter.most_common(k)]

    summary = {
        "brazilian_recordings": len(br),
        "edges_brazil_samples_others": len(out_e),
        "edges_brazil_sampled_by_others": len(in_e),
        "internal_br_br_edges": len(internal),
        "cross_border_out_edges": len(out_e) - len(internal),
        "cross_border_in_edges": len(in_e) - len(internal),
        "samples_from_country_top": topc(samples_from),
        "sampled_by_country_top": topc(sampled_by),
        "edge_type_mix_when_brazil_samples": dict(mix_out.most_common()),
        "edge_type_mix_when_brazil_is_sampled": dict(mix_in.most_common()),
        "internal_edge_type_mix": dict(internal_mix.most_common()),
        "internal_subgraph": {
            "nodes": H.number_of_nodes(), "edges": H.number_of_edges(),
            "weakly_connected_components": len(wcc),
            "largest_wcc": max((len(c) for c in wcc), default=0),
            "strongly_connected_components_gt1": sum(1 for c in scc if len(c) > 1),
            "largest_scc": max((len(c) for c in scc), default=0),
        },
        "temporal": {
            "lag_when_brazil_samples_median_yr": median(lag_out) if lag_out else None,
            "lag_when_brazil_samples_n": len(lag_out),
            "lag_until_brazil_source_reused_median_yr": median(lag_in) if lag_in else None,
            "lag_until_brazil_source_reused_n": len(lag_in),
        },
    }
    C.save_summary("brazil", summary)

    print("\n===== BRAZIL STUDY (MusicBrainz CC0) =====")
    print(f"  {len(br):,} BR recordings | samples others {len(out_e)} "
          f"({len(out_e)-len(internal)} cross-border) | sampled by others {len(in_e)} "
          f"({len(in_e)-len(internal)} cross-border) | internal {len(internal)}")
    print(f"  BR samples FROM: {samples_from.most_common(6)}")
    print(f"  BR sampled BY:   {sampled_by.most_common(6)}")
    print(f"  internal subgraph: {H.number_of_nodes()} nodes / {H.number_of_edges()} edges, "
          f"largest WCC {summary['internal_subgraph']['largest_wcc']}")
    print(f"  lag — BR samples material aged (median): "
          f"{summary['temporal']['lag_when_brazil_samples_median_yr']} yr")
    print("\nDONE (brazil).", flush=True)


if __name__ == "__main__":
    main()
