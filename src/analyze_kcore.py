#!/usr/bin/env python3
"""F2 — Decomposição em k-cores: localiza o backbone mutuamente mais denso.

Complementa os outros pilares: a centralidade (analyze_influence) acha nós
*individuais* de topo e a detecção de comunidades (analyze_communities) particiona
a rede em cenas; o k-core isola o subgrafo maximal em que todo nó tem pelo menos k
vizinhos *dentro dele* — o núcleo mutuamente mais denso — e perfila a estrutura
core/periferia.

Método: descasca iterativamente — remove repetidamente os nós de grau < k; o maior
k para o qual sobra um subgrafo não-vazio é a degeneração, e o subgrafo
sobrevivente é o k-core. Calculado sobre a projeção não-dirigida (sem self-loops)
do grafo já limpo de super-nós placeholder, consistente com o pilar de comunidades.

Saídas em analysis_output/:
  - kcore_summary.json        números-síntese (artista + faixa)
  - kcore_artist_core.csv/txt núcleo máximo de artistas (comunidade + in-strength)
  - kcore_shells.csv          tamanho de cada k-shell (k -> nº de nós), artistas
  - kcore_track_core.txt      núcleo máximo de faixas (expõe artefato de catálogo)
  - kcore_shell_decay.png     curva de decaimento dos k-shells
"""

from __future__ import annotations

import csv
import os
from collections import Counter

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import networkx as nx

import analyze_common as C
import graphs


def undirected_simple(G) -> nx.Graph:
    """Projeção não-dirigida sem self-loops (k-core exige ausência de self-loops)."""
    U = nx.Graph()
    U.add_nodes_from(G.nodes(data=True))
    for u, v in G.edges():
        if u != v:
            U.add_edge(u, v)
    return U


def load_communities():
    """nome do artista -> comunidade; comunidade -> rótulo-âncora (1º top member)."""
    name2comm, comm2scene = {}, {}
    ap = os.path.join(C.OUT, "comm_artist_assignments.csv")
    cp = os.path.join(C.OUT, "comm_artist_characterization.csv")
    if os.path.exists(ap):
        with open(ap, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                name2comm[row["artist"]] = row["community"]
    if os.path.exists(cp):
        with open(cp, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                members = (row.get("top_members", "") or "").split(";")
                comm2scene[row["comm_id"]] = members[0].strip() if members else "?"
    return name2comm, comm2scene


def analyze_artist():
    A = graphs.load("artist")
    U = undirected_simple(C.drop_placeholders(A))
    core = nx.core_number(U)
    degen = max(core.values())
    maxcore = [n for n, c in core.items() if c == degen]
    backbone = [n for n in U if U.degree(n) > 0]
    periphery = [n for n in backbone if core[n] <= 1]
    giant = max((len(c) for c in nx.connected_components(U)), default=0)
    ins = C.in_strength(A)
    name2comm, comm2scene = load_communities()

    # tamanho de cada k-shell
    shells = Counter(core.values())
    with open(os.path.join(C.OUT, "kcore_shells.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["k_shell", "n_nodes"])
        for k in range(degen + 1):
            w.writerow([k, shells.get(k, 0)])

    # membros do core máximo, com comunidade e in-strength
    rows, comm_dist = [], Counter()
    for n in maxcore:
        nm = A.nodes[n].get("artist", "")
        cid = name2comm.get(nm, "?")
        comm_dist[cid] += 1
        rows.append((nm, core[n], ins.get(n, 0), cid, comm2scene.get(cid, "?")))
    rows.sort(key=lambda r: r[2], reverse=True)
    C.save_ranking(rows, ["artist", "core", "in_strength", "community", "scene_anchor"],
                   "kcore_artist_core", top=len(rows))

    # curva de decaimento dos k-shells
    ks = list(range(degen + 1))
    plt.figure(figsize=(8, 5))
    plt.bar(ks, [shells.get(k, 0) for k in ks], color="indianred", alpha=0.85)
    plt.yscale("log")
    plt.xlabel("k-shell")
    plt.ylabel("nº de artistas (log)")
    plt.title(f"Decomposição em k-cores — artistas (degeneração = {degen})")
    plt.tight_layout()
    p = os.path.join(C.OUT, "kcore_shell_decay.png")
    plt.savefig(p, dpi=150)
    plt.close()
    print(f"  saved: {os.path.basename(p)}", flush=True)

    return {
        "nodes_total": A.number_of_nodes(),
        "backbone_interconnected": len(backbone),
        "periphery_core_le1": len(periphery),
        "periphery_pct": round(100 * len(periphery) / len(backbone), 1) if backbone else None,
        "degeneracy": degen,
        "max_core_size": len(maxcore),
        "max_core_pct_of_backbone": round(100 * len(maxcore) / len(backbone), 2) if backbone else None,
        "giant_component": giant,
        "communities_spanned_by_core": len(comm_dist),
        "core_community_distribution": [[c, n, comm2scene.get(c, "?")]
                                        for c, n in comm_dist.most_common()],
    }


def analyze_track():
    G = graphs.load("track")
    U = undirected_simple(G)
    core = nx.core_number(U)
    degen = max(core.values())
    maxcore = [n for n, c in core.items() if c == degen]
    backbone = [n for n in U if U.degree(n) > 0]
    periphery = [n for n in backbone if core[n] <= 1]
    with open(os.path.join(C.OUT, "kcore_track_core.txt"), "w", encoding="utf-8") as f:
        f.write(f"Faixa: {degen}-core ({len(maxcore)} nós) — as faixas mutuamente mais densas\n")
        f.write("=" * 60 + "\n")
        for n in maxcore:
            f.write(f"  {C.track_label(G, n)}\n")
    print(f"  saved: kcore_track_core.txt ({len(maxcore)} nós)", flush=True)
    return {
        "backbone_interconnected": len(backbone),
        "degeneracy": degen,
        "max_core_size": len(maxcore),
        "periphery_pct": round(100 * len(periphery) / len(backbone), 1) if backbone else None,
    }


def main():
    print("=== k-core: artistas ===", flush=True)
    art = analyze_artist()
    print("  ", art, flush=True)
    print("=== k-core: faixas ===", flush=True)
    trk = analyze_track()
    print("  ", trk, flush=True)
    C.save_summary("kcore", {"artist": art, "track": trk})
    print("\nDONE (kcore).", flush=True)


if __name__ == "__main__":
    main()
