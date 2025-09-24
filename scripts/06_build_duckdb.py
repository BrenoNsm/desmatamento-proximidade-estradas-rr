#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
06_build_duckdb.py
Cria um banco DuckDB com as tabelas da interseção (prontas para consultas rápidas).
Requisitos: pip install duckdb
"""

from pathlib import Path
import duckdb

PROJ = Path(__file__).resolve().parents[1]
INTER_DIR = PROJ / "data" / "processed" / "intersection"
PARQUET_PATH = INTER_DIR / "inter_prodes_rings.parquet"
CSV_RING_YEAR = INTER_DIR / "by_ring_year.csv"
CSV_RING = INTER_DIR / "by_ring_total.csv"
DB_PATH = INTER_DIR / "intersections.duckdb"

if not PARQUET_PATH.exists():
    raise FileNotFoundError(f"GeoParquet não encontrado: {PARQUET_PATH}\nRode antes: python scripts/05_precompute_intersections.py")

con = duckdb.connect(DB_PATH.as_posix())

# Tabela principal (lendo direto do parquet)
con.execute(f"CREATE OR REPLACE VIEW inter AS SELECT * FROM read_parquet('{PARQUET_PATH.as_posix()}');")

# Descobre coluna de ano para normalizar
year_col = con.execute("PRAGMA table_info('inter');").fetchdf()
year_col = year_col.loc[year_col['name'].str.lower().eq('year'), 'name'].iloc[0]

# Materializa agregados
con.execute(f"""
CREATE OR REPLACE TABLE by_ring_year AS
SELECT CAST(ring_id AS VARCHAR) AS ring_id,
       CAST({year_col} AS INT) AS year,
       SUM(area_ha) AS area_ha
FROM inter
GROUP BY 1,2
ORDER BY 2,1;
""")

con.execute("""
CREATE OR REPLACE TABLE by_ring AS
SELECT ring_id, SUM(area_ha) AS area_ha
FROM by_ring_year
GROUP BY 1
ORDER BY ring_id;
""")

# (Opcional) também guarda os CSVs como tabelas, se quiser comparar
if CSV_RING_YEAR.exists():
    con.execute(f"CREATE OR REPLACE TABLE by_ring_year_csv AS SELECT * FROM read_csv_auto('{CSV_RING_YEAR.as_posix()}');")
if CSV_RING.exists():
    con.execute(f"CREATE OR REPLACE TABLE by_ring_csv AS SELECT * FROM read_csv_auto('{CSV_RING.as_posix()}');")

con.close()
print("[OK] DuckDB criado em:", DB_PATH)
