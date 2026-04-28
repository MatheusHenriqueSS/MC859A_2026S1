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
│   ├── extract_tsvs.py             extrai seletivamente do mbdump.tar.bz2
│   └── build_mb_graph.py           constrói os 4 grafos via DuckDB+NetworkX
└── graph_output/
    ├── mb_sample_graph.graphml     nível-faixa  (321.738 nós / 222.696 arestas)
    ├── mb_artist_graph.graphml     nível-artista ( 47.286 nós /  41.014 arestas)
    ├── mb_decade_graph.graphml     nível-década (     13 nós /      88 arestas)
    ├── mb_country_graph.graphml    nível-país   (    159 nós /   1.003 arestas)
    ├── *_stats.txt                 dimensões, componentes, top-20 por força
    ├── degree_distribution.png     distribuição de in-/out-degree (faixa)
    ├── component_sizes.png         distribuição de tamanhos de SCC/WCC (faixa)
    ├── artist_*.png                idem, agregação artista
    ├── decade_*.png                idem, agregação década
    └── country_*.png               idem, agregação país
```

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
