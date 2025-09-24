#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
02_prepare_prodes_rr.py
- Recorta PRODES (yearly_deforestation_biome.shp) para dentro de Roraima (AOI)
- Saídas:
    data/processed/deforestation_rr.shp
    data/processed/deforestation_rr_preview.geojson  (amostra pequena p/ visualizar rápido)
Uso:
    python scripts/02_prepare_prodes_rr.py
"""

from pathlib import Path
import sys
import geopandas as gpd
import pandas as pd

PROJ_ROOT = Path(__file__).resolve().parents[1]
AOI_PATH  = PROJ_ROOT / "data" / "processed" / "roraima_aoi.geojson"
PRODES_DIR = PROJ_ROOT / "data" / "prodes"
PRODES_NAME = "yearly_deforestation_biome.shp"   # ajuste se o seu nome for diferente

OUT_DIR   = PROJ_ROOT / "data" / "processed"
OUT_SHP   = OUT_DIR / "deforestation_rr.shp"
OUT_PREV  = OUT_DIR / "deforestation_rr_preview.geojson"

DEFAULT_GEO = "EPSG:4326"

def info(m): print(f"[INFO] {m}")
def warn(m): print(f"[AVISO] {m}")
def err(m):
    print(f"[ERRO] {m}", file=sys.stderr); sys.exit(1)

def load_gdf(path: Path, label: str) -> gpd.GeoDataFrame:
    if not path.exists(): err(f"{label} não encontrado: {path}")
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        gdf.set_crs(DEFAULT_GEO, inplace=True)
        warn(f"{label} sem CRS — assumindo {DEFAULT_GEO}.")
    return gdf

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1) AOI
    info(f"Lendo AOI: {AOI_PATH}")
    aoi = load_gdf(AOI_PATH, "AOI")

    # 2) PRODES
    prodes_path = PRODES_DIR / PRODES_NAME
    info(f"Lendo PRODES: {prodes_path}")
    prodes = load_gdf(prodes_path, "PRODES")

    # 3) Recorte
    if prodes.crs != aoi.crs:
        prodes = prodes.to_crs(aoi.crs)

    info("Recortando PRODES para dentro de Roraima…")
    try:
        clipped = gpd.overlay(prodes, aoi[["geometry"]], how="intersection")
    except Exception:
        # fallback com clip (funciona bem para polígonos)
        clipped = gpd.clip(prodes, aoi)

    info(f"[OK] Feições após recorte: {len(clipped)}")

    # 4) Salvar
    clipped.to_file(OUT_SHP)
    info(f"[OK] Shapefile salvo: {OUT_SHP}")

    # amostra p/ preview rápido (até 100 features)
    prev = clipped.head(100).to_crs(DEFAULT_GEO)
    prev.to_file(OUT_PREV, driver="GeoJSON")
    info(f"[OK] Preview salvo (100 feições): {OUT_PREV}")

    # 5) Resumo
    cols = [c.lower() for c in clipped.columns]
    year_col = None
    for c in ["year", "ano"]:
        if c in cols:
            year_col = clipped.columns[cols.index(c)]
            break

    if year_col:
        stats = (clipped.groupby(year_col).size()
                 .reset_index(name="n_features")
                 .sort_values(year_col))
        print("\n[Resumo] Feições por ano:")
        print(stats.to_string(index=False))
    else:
        info("Coluna de ano não encontrada (esperado 'year'). Mostrando colunas:")
        print(list(clipped.columns))

if __name__ == "__main__":
    main()
