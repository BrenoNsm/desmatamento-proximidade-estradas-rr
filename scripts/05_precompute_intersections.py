#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
05_precompute_intersections.py
Pré-calcula PRODES ∩ anéis (Roraima) e salva em Parquet + agregados CSV.

Saídas:
  data/processed/intersection/inter_prodes_rings.parquet
  data/processed/intersection/by_ring_year.csv
  data/processed/intersection/by_ring_total.csv
"""

from pathlib import Path
import geopandas as gpd
import pandas as pd
from shapely.ops import unary_union   # corrigindo depreciação
from shapely.validation import make_valid
from shapely.geometry import Polygon, MultiPolygon

PROJ = Path(__file__).resolve().parents[1]
PROC = PROJ / "data" / "processed"
OUTD = PROC / "intersection"
OUTD.mkdir(parents=True, exist_ok=True)

EQUAL_AREA = "EPSG:5880"
WGS84 = "EPSG:4326"

def _fix_geoms(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Conserta geometrias inválidas e mantém apenas polígonos."""
    gdf = gdf.copy()
    # 1) tornar válidas
    gdf["geometry"] = gdf.geometry.apply(lambda g: make_valid(g) if g is not None else None)
    # 2) explodir coleções (MultiPolygon, GeometryCollection) em peças individuais
    gdf = gdf.explode(index_parts=False, ignore_index=True)
    # 3) filtrar só Polygon/MultiPolygon (após explode deve ser Polygon; mas deixo seguro)
    gdf = gdf[gdf.geometry.notnull()]
    gdf = gdf[gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"])]
    # 4) se ainda houver MultiPolygon, explode de novo
    if (gdf.geometry.geom_type == "MultiPolygon").any():
        gdf = gdf.explode(index_parts=False, ignore_index=True)
        gdf = gdf[gdf.geometry.geom_type == "Polygon"]
    gdf = gdf.reset_index(drop=True)
    return gdf

# ---------- carregar ----------
rings = gpd.read_file(PROC / "buffers" / "buffer_rings.shp").to_crs(EQUAL_AREA)[["ring_id","geometry"]]
prodes = gpd.read_file(PROC / "deforestation_rr.shp").to_crs(EQUAL_AREA)

# ---------- sanear geometrias ----------
rings = _fix_geoms(rings)
prodes = _fix_geoms(prodes)

# ---------- bbox clip para acelerar ----------
bbox = unary_union(rings.geometry).envelope
prodes = gpd.clip(prodes, bbox)

# ---------- overlay (interseção) ----------
# Obs.: agora 'prodes' contém apenas POLYGON -> não dá mais o erro de mixed types
inter = gpd.overlay(prodes, rings, how="intersection", keep_geom_type=True)

if inter.empty:
    # salvar arquivos vazios coerentes
    parquet_path = OUTD / "inter_prodes_rings.parquet"
    gpd.GeoDataFrame(columns=["ring_id","year","area_ha","geometry"], geometry="geometry", crs=EQUAL_AREA)\
        .to_parquet(parquet_path, index=False)
    pd.DataFrame(columns=["ring_id","year","area_ha"]).to_csv(OUTD / "by_ring_year.csv", index=False)
    pd.DataFrame(columns=["ring_id","area_ha"]).to_csv(OUTD / "by_ring_total.csv", index=False)
    print("[AVISO] Interseção vazia. Arquivos vazios salvos em", OUTD)
else:
    # ---------- área (ha) e limpeza ----------
    # coluna 'year'
    year_col = next((c for c in inter.columns if c.lower()=="year"), None)
    if year_col is None:
        raise RuntimeError("Coluna 'year' não encontrada no PRODES recortado.")

    inter["area_ha"] = inter.geometry.area / 10_000.0
    # normaliza ano (int)
    try:
        inter[year_col] = inter[year_col].astype(float).round().astype(int)
    except Exception:
        pass

    keep = ["ring_id", year_col, "area_ha", "geometry"]
    inter = inter[keep].copy()

    # ---------- salvar parquet (GeoParquet) ----------
    parquet_path = OUTD / "inter_prodes_rings.parquet"
    inter.to_parquet(parquet_path, index=False)
    print("[OK]", parquet_path)

    # ---------- agregados ----------
    by_ring_year = (inter.groupby(["ring_id", year_col])["area_ha"].sum()
                    .reset_index().sort_values([year_col,"ring_id"])
                    .rename(columns={year_col: "year"}))
    by_ring = (inter.groupby(["ring_id"])["area_ha"].sum()
               .reset_index().sort_values("area_ha", ascending=False))

    by_ring_year.to_csv(OUTD / "by_ring_year.csv", index=False)
    by_ring.to_csv(OUTD / "by_ring_total.csv", index=False)
    print("[OK] agregados CSV salvos em", OUTD)
