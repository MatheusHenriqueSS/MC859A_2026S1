#!/usr/bin/env python3
"""F2 — Pillar 2: community detection & scene discovery (artist level).

Runs Louvain (primary) and Leiden (cross-check) on the undirected weighted
projection of the artist giant WCC (placeholders removed), then characterizes the
largest communities by size, top members, dominant relation type, and — if the
v2 track graph is present — dominant decade and country (via artist_id).

Outputs: community assignments CSV, a labeled summary, a Gephi-ready GraphML with
a `community` node attribute, and modularity scores in the summary JSON.
"""

from __future__ import annotations

import os
from collections import Counter, defaultdict

import networkx as nx

import analyze_common as C

import community as community_louvain  # python-louvain
import igraph as ig
import leidenalg


def undirected_weighted(H) -> nx.Graph:
    U = nx.Graph()
    U.add_nodes_from(H.nodes(data=True))
    for u, v, d in H.edges(data=True):
        w = int(d.get("weight", 1))
        if U.has_edge(u, v):
            U[u][v]["weight"] += w
        else:
            U.add_edge(u, v, weight=w)
    return U


def leiden_modularity(U):
    nodes = list(U.nodes())
    idx = {n: i for i, n in enumerate(nodes)}
    edges = [(idx[u], idx[v]) for u, v in U.edges()]
    weights = [U[u][v]["weight"] for u, v in U.edges()]
    g = ig.Graph(n=len(nodes), edges=edges)
    g.es["weight"] = weights
    part = leidenalg.find_partition(g, leidenalg.ModularityVertexPartition,
                                    weights="weight", seed=42)
    membership = {nodes[i]: c for i, c in enumerate(part.membership)}
    return membership, part.modularity


def artist_track_profiles(artist_ids):
    """From the v2 track graph, build artist_id -> (decade Counter, country Counter)."""
    if not os.path.exists(C.TRACK_V2):
        return None
    G = C.load(C.TRACK_V2)
    dec = defaultdict(Counter)
    cou = defaultdict(Counter)
    want = set(int(a) for a in artist_ids if str(a).lstrip("-").isdigit())
    for _, d in G.nodes(data=True):
        aid = d.get("artist_id", -1)
        if aid not in want:
            continue
        y = d.get("year", -1)
        if isinstance(y, int) and y > 0:
            dec[aid][f"{(y // 10) * 10}s"] += 1
        c = d.get("country", "")
        if c:
            cou[aid][c] += 1
    return dec, cou


def characterize(H, partition, top_n=15):
    """H: directed giant WCC (artist). partition: node->community id."""
    members = defaultdict(list)
    for n, c in partition.items():
        members[c].append(n)
    ins = C.in_strength(H)
    # dominant relation type per community (intra-community edges)
    comm_types = defaultdict(Counter)
    for u, v, d in H.edges(data=True):
        if partition.get(u) == partition.get(v):
            for t in str(d.get("types", "")).split(";"):
                if t:
                    comm_types[partition[u]][t] += 1

    big = sorted(members.items(), key=lambda kv: len(kv[1]), reverse=True)[:top_n]
    big_ids = [c for c, _ in big]
    profiles = artist_track_profiles(
        [a for c in big_ids for a in members[c]])

    rows = []
    for c, mem in big:
        mem_sorted = sorted(mem, key=lambda n: ins.get(n, 0), reverse=True)
        top_members = "; ".join(C.artist_label(H, n) for n in mem_sorted[:5])
        dom_type = comm_types[c].most_common(1)
        dom_type = dom_type[0][0] if dom_type else "-"
        dec_lbl = cou_lbl = "-"
        if profiles is not None:
            dec_acc, cou_acc = profiles
            dc, cc = Counter(), Counter()
            for n in mem:
                aid = int(n) if str(n).lstrip("-").isdigit() else None
                if aid is not None:
                    dc.update(dec_acc.get(aid, {}))
                    cc.update(cou_acc.get(aid, {}))
            dec_lbl = ", ".join(f"{k}({v})" for k, v in dc.most_common(3)) or "-"
            cou_lbl = ", ".join(f"{k}({v})" for k, v in cc.most_common(3)) or "-"
        rows.append((c, len(mem), dom_type, dec_lbl, cou_lbl, top_members))
    return rows


def main():
    print("=== community detection (artist) ===", flush=True)
    H = C.drop_placeholders(C.load(C.ARTIST))
    wcc = max(nx.weakly_connected_components(H), key=len)
    Hg = H.subgraph(wcc).copy()
    U = undirected_weighted(Hg)
    print(f"  giant WCC undirected: {U.number_of_nodes():,} nodes / "
          f"{U.number_of_edges():,} edges", flush=True)

    print("  Louvain...", flush=True)
    louvain = community_louvain.best_partition(U, weight="weight", random_state=42)
    mod_louvain = community_louvain.modularity(louvain, U, weight="weight")
    n_louvain = len(set(louvain.values()))
    print(f"    {n_louvain} communities, modularity {mod_louvain:.4f}", flush=True)

    print("  Leiden...", flush=True)
    leiden, mod_leiden = leiden_modularity(U)
    n_leiden = len(set(leiden.values()))
    print(f"    {n_leiden} communities, modularity {mod_leiden:.4f}", flush=True)

    # assignments CSV
    C.save_ranking(
        sorted([(C.artist_label(Hg, n), louvain[n], C.in_strength(Hg).get(n, 0))
                for n in Hg], key=lambda r: (r[1], -r[2])),
        ["artist", "community", "in_strength"], "comm_artist_assignments", top=80)

    rows = characterize(Hg, louvain)
    C.save_ranking(rows,
                   ["comm_id", "size", "dom_type", "top_decades", "top_countries",
                    "top_members"],
                   "comm_artist_characterization", top=15)

    # Gephi-ready labeled graph
    for n in Hg:
        Hg.nodes[n]["community"] = int(louvain[n])
    gpath = os.path.join(C.OUT, "artist_giant_communities.graphml.gz")
    nx.write_graphml(Hg, gpath)
    print(f"  saved: {os.path.basename(gpath)}", flush=True)

    C.save_summary("communities", {
        "giant_wcc_nodes": U.number_of_nodes(),
        "louvain_communities": n_louvain, "louvain_modularity": round(mod_louvain, 4),
        "leiden_communities": n_leiden, "leiden_modularity": round(mod_leiden, 4),
        "top_community_sizes": sorted(Counter(louvain.values()).values(),
                                      reverse=True)[:15],
    })
    print("\nDONE (communities).", flush=True)


if __name__ == "__main__":
    main()
