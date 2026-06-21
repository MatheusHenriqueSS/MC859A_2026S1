# MC859 — Redes de Samples Musicais (Entrega Parcial F1)

Projeto de **MC859 — Projeto em Teoria da Computação**, Instituto de
Computação, UNICAMP, 2026/S1, sob orientação do Prof. Dr. Ruben
Interian. Autor: Matheus Henrique de S. Silva (RA 239995).

Este repositório hospeda **quatro instâncias de grafo direcionado**
representando relações derivativas (sample, remix, edit, DJ-mix,
mash-up) entre gravações musicais reais, extraídas do core dump
público do MusicBrainz (versão `20260425-002540`, licença CC0).

## Relatório

[`f1_report.pdf`](./f1_report.pdf) — entrega parcial F1 (4 páginas)
cobrindo intro, metodologia, construção do grafo, análise inicial e
apêndice sobre o pivô WhoSampled → MusicBrainz.

## Estrutura do repositório

```
.
├── README.md                       este arquivo
├── f1_report.pdf                   relatório da entrega parcial F1
├── src/
│   ├── extract_tsvs.py                extrai seletivamente do mbdump.tar.bz2
│   ├── build_mb_graph.py             constrói os 4 grafos F1 via DuckDB+NetworkX
│   ├── build_track_v2.py             re-emite o grafo de faixa com ano+país (F2)
│   ├── build_genre_graph.py          grafo de gênero (tags do dump derived) (F2)
│   ├── graphs.py                     registro nome-lógico → arquivo (.graphml.gz)
│   └── analyze_*.py                  análises F2 (influência, comunidades, ...)
├── graph_output/                     instâncias em GraphML gzipado
│   ├── mb_sample_graph.graphml.gz       nível-faixa F1 (321.738 nós / 222.696 ar.)
│   ├── mb_sample_graph_v2.graphml.gz    nível-faixa F2 (+ ano, país por nó)
│   ├── mb_artist_graph.graphml.gz       nível-artista (47.286 nós / 41.014 ar.)
│   ├── mb_decade_graph.graphml.gz       nível-década (13 nós / 88 ar.)
│   ├── mb_country_graph.graphml.gz      nível-país   (159 nós / 1.003 ar.)
│   ├── mb_genre_graph.graphml.gz        nível-gênero (881 gêneros / 9.037 ar., F2)
│   └── *_stats.txt, *.png               estatísticas e visualizações
└── analysis_output/                  resultados F2 (rankings, métricas, plots)
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
pip install duckdb pandas networkx matplotlib

# 3. Baixar o core dump do MusicBrainz (~6,7 GB)
mkdir -p data
curl -L -o data/mbdump.tar.bz2 \
  https://data.metabrainz.org/pub/musicbrainz/data/fullexport/20260425-002540/mbdump.tar.bz2

# 4. Extrair seletivamente as 12 tabelas necessárias (~10 min)
python src/extract_tsvs.py

# 5. Construir os 4 grafos + plots + stats (~5 min)
python src/build_mb_graph.py
```

A pipeline é determinística: rodando duas vezes sobre o mesmo dump
produz GraphMLs byte-a-byte idênticos (modulo ordenação interna
do NetworkX, que é estável).

## Métodos

- **Filtragem de relações:** apenas os 5 tipos de `link_type` válidos
  para `entity_type0=entity_type1='recording'` que caracterizam
  derivação musical (samples material, remix, edit, DJ-mix, mashes up)
  são mantidos. Total bruto: 231.049 arestas.
- **Filtro de popularidade:** uma gravação é mantida apenas se aparece
  em pelo menos um lançamento (track count $\geq 1$). Reduz para
  223.018 arestas.
- **Direção das arestas:** `A → B` significa "A é derivada de B"
  (sampleia, remixa ou cobre). In-strength mede quanto se foi
  sampleado; out-strength mede quanto se sampleou outras fontes.
- **Agregação artista:** colapsa por `artist.id` da contagem-artista
  primária, dropa auto-laços (remixes do próprio artista).
- **Agregação década:** propaga ano via
  `track → medium → release_country/release_unknown_country`
  e bucketeia por década da primeira publicação.
- **Agregação país:** usa `artist.area` restrito a `area.type=1`
  (entradas em nível de país). Dropa artistas sem área-país.

## Licença

- **Dados (`graph_output/*`):** derivados do MusicBrainz Database,
  redistribuídos sob **CC0 1.0** conforme termos do MusicBrainz.
- **Código (`src/*`):** **MIT**.
