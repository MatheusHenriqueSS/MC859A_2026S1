# MC859 — Redes de Samples Musicais (Entrega Final F2)

Projeto de **MC859 — Projeto em Teoria da Computação**, Instituto de
Computação, UNICAMP, 2026/S1, sob orientação do Prof. Dr. Ruben
Interian. Autor: Matheus Henrique de S. Silva (RA 239995).

Este repositório hospeda **seis instâncias de grafo direcionado**
representando relações derivativas (sample, remix, edit, DJ-mix,
mash-up) entre gravações musicais reais — extraídas do dump público do
MusicBrainz (versão `20260425-002540`, licença CC0) — e o **pipeline de
análise** que extrai conhecimento e detecta padrões sobre essas
instâncias (influência, comunidades, estrutura, tempo, geografia,
gênero e um estudo de caso sobre o Brasil).

## Relatórios

- [`f2_report.pdf`](./f2_report.pdf) — **entrega final F2**: captura de
  dados, criação das instâncias, pseudocódigo dos algoritmos, resultados,
  descobertas e interpretação, além do estudo de caso do Brasil.
- [`f1_report.pdf`](./f1_report.pdf) — entrega parcial F1 (4 páginas):
  intro, metodologia, construção do grafo, análise inicial e apêndice
  sobre o pivô WhoSampled → MusicBrainz.

## Estrutura do repositório

```
.
├── README.md                       este arquivo
├── f2_report.pdf                   relatório da entrega final F2
├── f1_report.pdf                   relatório da entrega parcial F1
├── src/
│   ├── extract_tsvs.py               extrai seletivamente do core + derived dump
│   ├── build_mb_graph.py             constrói os 4 grafos F1 via DuckDB+NetworkX
│   ├── build_track_v2.py             re-emite o grafo de faixa com ano+país (F2)
│   ├── build_genre_graph.py          grafo de gênero (tags do dump derived) (F2)
│   ├── graphs.py                     registro nome-lógico → arquivo (.graphml.gz)
│   ├── analyze_common.py             utilitários compartilhados das análises
│   ├── analyze_influence.py          PageRank, HITS, betweenness, papéis
│   ├── analyze_communities.py        Louvain/Leiden + caracterização de cenas
│   ├── analyze_structure.py          power-law vs lognormal, assortatividade
│   ├── analyze_temporal.py           lag de influência, evergreens, linhagens
│   ├── analyze_geography.py          homofilia e fluxo país→país
│   ├── analyze_genre.py              homofilia e fluxo gênero→gênero
│   ├── analyze_brazil.py             estudo de caso do Brasil (faixa/artista)
│   ├── analyze_kcore.py              decomposição em k-core (backbone denso)
│   └── exploration/                  sondagens de fontes externas (auxiliar)
├── graph_output/                     instâncias em GraphML gzipado
│   ├── mb_sample_graph.graphml.gz       nível-faixa F1 (321.738 nós / 222.696 ar.)
│   ├── mb_sample_graph_v2.graphml.gz    nível-faixa F2 (+ ano, país por nó)
│   ├── mb_artist_graph.graphml.gz       nível-artista (47.286 nós / 41.014 ar.)
│   ├── mb_decade_graph.graphml.gz       nível-década (13 nós / 88 ar.)
│   ├── mb_country_graph.graphml.gz      nível-país   (159 nós / 1.003 ar.)
│   ├── mb_genre_graph.graphml.gz        nível-gênero (888 nós / 10.944 ar., F2)
│   └── *_stats.txt, *.png               estatísticas e visualizações
└── analysis_output/                  resultados F2 (rankings, métricas, plots)
    ├── *_summary.json                  números-síntese de cada pilar
    ├── infl_*, comm_*, struct_*        rankings de influência/comunidades/estrutura
    ├── temporal_*, geo_*, genre_*      lag/evergreen, fluxo geográfico e de gênero
    ├── br_*, brazil_summary.json       estudo de caso do Brasil
    └── br_subgraph.graphml.gz          subgrafo do Brasil para visualização (Gephi)
```

## Formato e carregamento das instâncias

Todas as instâncias são **GraphML gzipado** (`*.graphml.gz`) — formato uniforme
independente do tamanho. NetworkX lê/escreve `.gz` diretamente
(`nx.read_graphml("...graphml.gz")`); para ferramentas como o Gephi, descomprima
antes (`gunzip -k arquivo.graphml.gz`). O código carrega grafos por **nome
lógico** via `src/graphs.py` (ex.: `graphs.load("track_v2")`), de modo que o
local de armazenamento de uma instância pode mudar sem afetar as análises.

## Reprodução

Pré-requisitos: Python 3.12+, ~50 GB de espaço livre, ~16 GB de RAM.

```bash
# 1. Clonar o repositório
git clone git@github.com:MatheusHenriqueSS/MC859A_2026S1.git
cd MC859A_2026S1

# 2. Criar venv e instalar dependências
python3 -m venv venv
source venv/bin/activate
pip install duckdb pandas networkx numpy matplotlib \
            scipy python-louvain python-igraph leidenalg powerlaw seaborn

# 3. Baixar os dumps do MusicBrainz (core ~6,7 GB + derived ~0,5 GB)
mkdir -p data
BASE=https://data.metabrainz.org/pub/musicbrainz/data/fullexport/20260425-002540
curl -L -o data/mbdump.tar.bz2         $BASE/mbdump.tar.bz2
curl -L -o data/mbdump-derived.tar.bz2 $BASE/mbdump-derived.tar.bz2

# 4. Extrair seletivamente as tabelas necessárias (core + derived, ~10 min)
python src/extract_tsvs.py

# 5. Construir as instâncias de grafo
python src/build_mb_graph.py     # 4 grafos F1 + plots + stats (~5 min)
python src/build_track_v2.py     # grafo de faixa com ano+país (F2)
python src/build_genre_graph.py  # grafo de gênero (F2)

# 6. Rodar as análises (escrevem em analysis_output/)
cd src
for a in influence communities structure temporal geography genre brazil kcore; do
    python analyze_$a.py
done
```

A pipeline é determinística: rodando duas vezes sobre o mesmo dump
produz GraphMLs byte-a-byte idênticos (modulo ordenação interna
do NetworkX, que é estável).

## Métodos

### Construção das instâncias (F1)

- **Filtragem de relações:** apenas os 5 tipos de `link_type` válidos
  para `entity_type0=entity_type1='recording'` que caracterizam
  derivação musical (samples material, remix, edit, DJ-mix, mashes up)
  são mantidos. Total bruto: 231.049 arestas.
- **Filtro de popularidade:** uma gravação é mantida apenas se aparece
  em pelo menos um lançamento (track count ≥ 1). Reduz para 223.018 arestas.
- **Direção das arestas:** `A → B` significa "A é derivada de B"
  (sampleia, remixa ou cobre). In-strength mede quanto se foi
  sampleado; out-strength mede quanto se sampleou outras fontes.
- **Agregação artista/década/país:** colapsa por `artist.id`, década da
  primeira publicação (`track → medium → release_country`) e
  `artist.area` (nível-país, `area.type=1`).

### Enriquecimentos e análise (F2)

- **Ano + país por nó** (`build_track_v2.py`): cobertura 99,2% (ano) e
  75% (país), habilitando os pilares temporal e geográfico.
- **Camada de gênero** (`build_genre_graph.py`): tags do dump *derived*,
  com gênero por gravação resolvido por *fallback* de três níveis
  (gravação → álbum → artista), cobertura 83,1%.
- **Influência e papéis:** PageRank ponderado (no grafo e no seu reverso),
  HITS na maior componente conexa, betweenness e quadrante in/out-strength.
- **Comunidades e cenas:** Louvain/Leiden sobre a maior WCC de artistas
  (após remover super-nós agregadores), caracterizadas por tipo de
  relação, década, país e membros dominantes.
- **Estrutura:** ajuste *power-law* vs *lognormal* do in-degree
  (biblioteca `powerlaw`), assortatividade, reciprocidade, transitividade
  e comparação por tipo de relação.
- **Temporal:** *lag* de influência, consistência cronológica, fontes
  *evergreen* e maior linhagem de derivação.
- **Geografia e gênero:** homofilia via modelo nulo de independência,
  exportadores/importadores líquidos e principais corredores.
- **Estudo de caso do Brasil** (`analyze_brazil.py`): desce ao nível de
  faixa/artista — material brasileiro mais reutilizado (doméstica vs
  internacionalmente), o que as faixas brasileiras mais sampleiam, e a
  rede interna BR↔BR.
- **Decomposição em k-core** (`analyze_kcore.py`): núcleo mutuamente mais denso
  (degeneração) e perfil core/periferia; o core de artistas atravessa múltiplas
  comunidades (tecido conectivo entre cenas) e o core de faixas expõe um artefato
  de catálogo.

> **Fontes externas (`src/exploration/`).** Sondagens auxiliares (Spotify,
> Discogs, Genius) avaliaram fontes adicionais de sampling, sobretudo para o
> Brasil. O grafo entregue permanece **100% MusicBrainz (CC0)**; os dados do
> Genius são proprietários e usados apenas como **estudo de caso citado** no
> relatório — as arestas brutas **não** são redistribuídas neste repositório.

## Licença

- **Dados (`graph_output/*`):** derivados do MusicBrainz Database,
  redistribuídos sob **CC0 1.0** conforme termos do MusicBrainz.
- **Código (`src/*`):** **MIT**.
