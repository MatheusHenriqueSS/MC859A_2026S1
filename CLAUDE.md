# MC859 — F2: MusicBrainz Sample/Remix Network — Implementation & Analysis

This is the **working / public-deliverable repo** for the MC859 graph project
(UNICAMP IC, 2026/S1, Prof. Dr. Ruben Interian — author: Matheus Henrique de S.
Silva, RA 239995). Git origin: `github.com/MatheusHenriqueSS/MC859A_2026S1` (public).

F1 (data + 4 graph instances + initial analysis) is **done**. We are now in **F2**.

## F2 objective (course definition)

> *F2 – Implementação, aplicação prática, análise de resultados, interpretação — 50% da nota.*

Concretely, F2 means: run real graph algorithms on the F1 instances to **extract
knowledge and detect patterns**, then **analyze and interpret** the results and
write the final report (≤ 15 pages, PT-BR) covering data capture, instance
creation, **algorithm pseudocode**, results, and discoveries. Final grade weight 50%.

**F2 deliverables:** (1) reproducible analysis code; (2) result artifacts
(tables, plots, derived graphs) committed to this public repo; (3) the F2 report.

> ⚠️ Deadline check: the course `CLAUDE.md` lists F2 due **2026-06-08** and F3
> presentations **2026-06-26**, but today is 2026-06-20 — confirm the real F2
> deadline with the user before sizing the work.

## Where things live

- `/home/mathe/musicbrainz_graph/` — **this repo**: code, data, graphs, F2 analysis.
  - `src/extract_tsvs.py` — selective extractor over `data/mbdump.tar.bz2`
    (6.7 GB, still on disk); writes 12 tables to `data/tsv/` (~17 GB).
  - `src/build_mb_graph.py` — DuckDB → NetworkX builder for the 4 GraphML graphs.
  - `graph_output/` — the 4 `.graphml`, `*_stats.txt`, and `*.png` plots.
  - `venv/` — Python 3.12.
- `/home/mathe/graph/` — report-authoring workspace (`f1_report.tex/.pdf`,
  `proposta.*`, course-level `CLAUDE.md`, persistent memory).

## The data we built (F2 starting point)

4 **directed** graphs from MusicBrainz dump `20260425-002540` (CC0). Edge
orientation `A → B` = "**A is derived from B**" (A samples/remixes/covers B), so
**in-strength = how often used as a source**, **out-strength = how much it derives
from others**. Edge `weight` = number of parallel relations; `types` = `;`-joined
subset of {sample, remix, edit, dj_mix, mashup}.

| Graph | file | nodes | edges | node attrs | notes |
|---|---|---|---|---|---|
| track | `mb_sample_graph.graphml` | 321,738 | 222,696 | title, artist, artist_id, popularity | ~DAG (max SCC=3); largest WCC 36,297 (11.3%) |
| artist | `mb_artist_graph.graphml` | 47,286 | 41,014 | artist, track_count | largest WCC 19,348 (40.9%); largest SCC 404 |
| decade | `mb_decade_graph.graphml` | 13 | 88 | decade | dense; 2010s dominant both directions |
| country | `mb_country_graph.graphml` | 159 | 1,003 | endpoint_count | largest WCC 124 (78%); US/UK ≈ 80% of flow |

Edge-type mix (track): remix 128,560 · edit 31,728 · sample 22,706 · dj_mix 21,222 · mashup 18,668.
`popularity` = number of `track` (release) appearances of the recording (the F1 filter threshold).

## Environment & gaps to close for F2

- **venv has:** networkx 3.6.1, pandas, matplotlib, duckdb, numpy.
- **F2 needs (pip install):** `scipy`, a community-detection lib
  (`python-louvain` and/or `python-igraph` + `leidenalg`), `powerlaw`
  (degree-distribution fit), `seaborn` (plots).
- **Data enablers (decide per scope):**
  1. ~~Track nodes lack `year`/`country`~~ **DONE** — `mb_sample_graph_v2`
     carries `year` (99.2%) + `country` (75%) on nodes (`build_track_v2.py`).
  2. ~~Genre is not extracted~~ **DONE** — tags come from the *derived* dump
     (`mbdump-derived.tar.bz2`, ~478 MB) + `release` from core; genre per recording
     resolved in `build_genre_graph.py` via 3-tier fallback (recording → album →
     artist). Coverage 83.1% — the other ~17% have no genre tag at any level.
- **Identity:** recording `id` is the canonical join key; display names may have
  near-duplicates — never merge on name.

## F2 scope (locked 2026-06-20) & sequence

Time budget: **a few days**. Data scope chosen: **re-emit track w/ year+country**,
**genre layer**, **Spotify popularity cross**. Sequenced so the report is complete
even if the riskiest piece (Spotify) slips:

- **Phase 0 — setup/unblock (~30 min):** `pip install scipy python-louvain
  python-igraph leidenalg powerlaw seaborn`; re-emit `mb_sample_graph_v2.graphml`
  with `year`+`country` on nodes (builder already has them per edge).
- **Phase 1 — core analysis (pillars 1–5):** `src/analyze_*.py` → `analysis_output/`.
- **Phase 2 — genre layer ✅ DONE:** tags live in the *derived* dump
  (`mbdump-derived.tar.bz2`), not core; `extract_tsvs.py` now reads both.
  `build_genre_graph.py` resolves a genre per recording via a 3-tier fallback
  (recording_tag → release-group/album → artist_tag; **83.1% coverage**) →
  `mb_genre_graph.graphml.gz` (888 genres). Homophily 8.13; net sources =
  funk/soul/jazz/disco, net importers = electronic/hip hop (validates the method).
- **Phase 3 — Spotify (scoped, GATED):** top ~3–5K influential recordings matched
  via MusicBrainz **ISRC** → Spotify. **Blocker: user must supply Spotify client
  ID/secret.** Runs last; optional report subsection if time runs short.
- **Phase 4 — report (PT-BR, ≤15 pp).**

## Planned F2 analyses

Grounded in what these graphs support. Core = 1–5; stretch = 6–7.

1. **Influence ranking & roles** — weighted PageRank on the *reversed* graph
   (influence flows to foundational sources); HITS (authorities = sources, hubs =
   prolific samplers/DJs); in- vs out-strength role quadrant; sampled betweenness
   on the artist giant component (bridge artists). Graphs: track, artist.
2. **Community detection & scene discovery** — Louvain/Leiden on the artist giant
   WCC; characterize top communities by dominant edge type / decade / country /
   top members (genres & scenes).
3. **Temporal influence flow** — *needs track year re-emit*. Source→derivative
   time-gap ("influence half-life"), temporal-consistency check, decade flow
   heatmap, evergreen breaks, longest DAG derivation chains, descendant footprint.
4. **Geographic flow & homophily** — net exporter/importer per country, within- vs
   cross-country edge share vs a null model, top corridors, Brazil case study.
5. **Structural characterization** — power-law vs lognormal fit of in-degree
   (test the F1 "scale-free" claim), degree & attribute assortativity, reciprocity,
   clustering; per-relation-type subgraph comparison (sample = deep chains vs
   remix = shallow stars).
6. **Genre layer (stretch)** — re-extract genre, build genre graph + genre flow
   matrix + genre homophily.
7. **Case-study narratives & viz (stretch)** — derivation trees of iconic breaks
   (Amen Brother, Funky Drummer, Gangnam Style); Gephi-ready exports.

## F2 code map (added during Phase 0–1)

- `src/build_track_v2.py` — re-emits `graph_output/mb_sample_graph_v2.graphml.gz`
  with `year` (99.2% coverage) + `country` (75%) on nodes. Reuses the F1
  pipeline; ~15 min (DuckDB loads ~17 GB TSV).
- `src/graphs.py` — **graph storage registry**: load every instance by logical
  name (`graphs.load("track_v2")`), never by path. Uniform format = gzipped
  GraphML (`*.graphml.gz`); `.gitignore` excludes raw `*.graphml`. Storage of a
  single instance can move (Release/Zenodo) by editing one registry entry — no
  analysis-code change. networkx reads `.gz` natively; Gephi needs `gunzip` first.
- `src/analyze_common.py` — shared loaders (delegates to `graphs`), strength
  helpers, placeholder filter, `save_ranking`/`save_summary` → `analysis_output/`.
- `src/analyze_influence.py` (P1) · `analyze_communities.py` (P2) ·
  `analyze_structure.py` (P5) · `analyze_temporal.py` (P3) ·
  `analyze_geography.py` (P4) · `analyze_genre.py` (P2 genre). Run from `src/`.
- `src/extract_tsvs.py` reads BOTH core + derived dumps; `src/build_genre_graph.py`
  resolves recording genres and builds the `genre` instance.

**Methodological decisions (verified 2026-06-20):**
- **Drop placeholder artist nodes** (`[unknown]`, `Various Artists`, `[no
  artist]`, `[anonymous]`, `[traditional]`, `[dialogue]`) before any artist-level
  centrality/community — they are aggregation super-nodes that distort results
  (`[unknown]` otherwise tops betweenness). See `C.drop_placeholders`.
- **HITS only on the giant WCC** — it is degenerate on the full disconnected
  graph (collapses onto one isolated 2-node component). On the giant WCC it is
  coherent: authorities = most-mashed pop stars, hubs = mashup producers (the
  giant component is the pop-mashup ecosystem).
- PageRank on the graph as-is = foundational source influence (rank accumulates
  at sources, which have high in-degree); reversed = sampler/curatorial reach.

## Conventions

- Code in English, deterministic, reproducible from the dump; write F2 outputs to
  `graph_output/` or a new `analysis_output/`. Report in PT-BR (like F1).
- Keep the F1 GraphML instances stable; if re-emitting, version the filenames
  (e.g. `mb_sample_graph_v2.graphml`) so F1 artifacts stay reproducible.
- AI assistance must be cited in the report (course rule); the student owns the content.
