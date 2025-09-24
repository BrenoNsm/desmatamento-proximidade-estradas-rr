# desmatamento-proximidade-estradas-rr
 Dinâmica do Desmatamento em Roraima em Função da Proximidade das Estradas Análises PRODES × OSM com buffers de distância, agregações em DuckDB e visualização Streamlit.

# Dinâmica do Desmatamento em Roraima em Função da Proximidade das Estradas

Análises **PRODES × OSM** com buffers de distância, agregações em **DuckDB** e visualização em **Streamlit**.

> **Resumo do pipeline**
> 1) Prepara estradas (OSM) e AOI de Roraima  
> 2) Recorta PRODES para Roraima  
> 3) Gera buffers/anéis de distância (0–5, 5–10, 10–20, >20 km)  
> 4) (opção A) Interseção direta → GeoParquet  
> 5) (opção B recomendada) Pré-cálculo + **DuckDB** → app fica rápido  
> 6) App `Streamlit` (`app.py`) e relatório em PDF (`doc.py`)  

---

## 1) Requisitos

- Python 3.10+  
- Virtualenv (opcional, recomendado)

Instale dependências:

```bash
# Linux/WSL
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Windows (PowerShell)
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## 2) Estrutura de pastas esperada
.
├── app.py
├── doc.py
├── gen_figures.py
├── requirements.txt
├── scripts/
│   ├── 01_prepare_osm_rr.py
│   ├── 02_prepare_prodes_rr.py
│   ├── 03_create_buffers.py
│   ├── 04_intersection.py                  # (opção A)
│   ├── 05_precompute_intersections.py      # (opção B – recomendada)
│   ├── 06_build_duckdb.py                  # (opção B – recomendada)
│   └── check_data.py                       # (opcional; ver abaixo)
└── data/
    ├── external/
    │   ├── ibge_uf/                        # shapefile da UF (RR)
    │   │   └── RR_UF_2024.(shp|shx|dbf|prj|...)
    │   └── ibge_municipal/                 # shapefile de municípios de RR
    │       └── RR_Municipios_2024.(shp|shx|dbf|prj|...)
    ├── osm/
    │   └── gis_osm_roads_free_1.(shp|shx|dbf|prj|cpg)
    ├── prodes/
    │   └── yearly_deforestation_biome.(shp|shx|dbf|prj|...)
    └── processed/
        ├── roraima_aoi.geojson
        ├── roads_rr.(shp|shx|dbf|prj)
        ├── buffers/
        │   ├── roads_buffer_5km.(shp|shx|dbf|prj)
        │   ├── roads_buffer_10km.(shp|shx|dbf|prj)
        │   ├── roads_buffer_20km.(shp|shx|dbf|prj)
        │   └── buffer_rings.(shp|shx|dbf|prj)
        └── intersection/
            ├── inter_prodes_rings.parquet
            ├── by_ring_year.csv
            ├── by_ring_total.csv
            └── intersections.duckdb


## 3) Onde baixar os dados (fontes)

Os links podem mudar; se mudar, pesquise pelos nomes indicados.

3.1 OSM (estradas) – Geofabrik https://download.geofabrik.de/south-america/brazil.html

Fonte: Geofabrik — “Brazil/norte” (shapefile)

O que baixar: pacote que contém gis_osm_roads_free_1.*

Como organizar: extraia os arquivos do shapefile em data/osm/

3.2 IBGE – UF e Municípios (Roraima)

Fonte: IBGE — Malhas Territoriais (2024) https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais/15774-malhas.html

O que baixar:

UF Roraima → salve como data/external/ibge_uf/RR_UF_2024.(shp|...)

Municípios de Roraima → salve como data/external/ibge_municipal/RR_Municipios_2024.(shp|...)

3.3 PRODES – INPE/TerraBrasilis https://dados.gov.br/dados/conjuntos-dados/prodes
seção = Incremento no desmatamento do Bioma Amazônia a partir de 2008

Fonte: TerraBrasilis / dados.gov.br

O que baixar: Incremento no desmatamento do Bioma Amazônia a partir de 2008 (shapefile) 

Como organizar: extraia o shapefile para data/prodes/ com o nome
yearly_deforestation_biome.(shp|shx|dbf|prj|...)

Se também quiser DETER (alertas), baixe “Mapa de Alertas (DETER) – Amazônia” e organize similarmente
em data/deter/ (não é obrigatório para rodar o app atual).


## 4) Pipeline (execução)

# Linux/WSL
python scripts/01_prepare_osm_rr.py

# Windows
python .\scripts\01_prepare_osm_rr.py

Passo 2 — Recorte do PRODES (RR)
python scripts/02_prepare_prodes_rr.py

Passo 3 — Buffers/Anéis (0–5, 5–10, 10–20, >20 km)
# Se der erro de memória no Windows, use --chunk-size
python scripts/03_create_buffers.py --dist 5 10 20 --chunk-size 20000

Passo 4B — (Recomendado) Pré-cálculo + DuckDB
python scripts/05_precompute_intersections.py
python scripts/06_build_duckdb.py

## 5) Executar o app (Streamlit)
# WSL/Linux
streamlit run app.py --server.fileWatcherType=none

# Windows
streamlit run app.py --server.fileWatcherType=none
