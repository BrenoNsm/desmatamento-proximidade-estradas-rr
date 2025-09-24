#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
04_intersection.py
Pré-calcula a interseção PRODES (Roraima) × anéis de distância das estradas
e salva em GeoParquet + agregados CSV.

Entradas esperadas:
  data/processed/deforestation_rr.shp
  data/processed/buffers/buffer_rings.shp

Saídas:
  data/processed/intersection/inter_prodes_rings.parquet
  data/processed/intersection/by_ring_year.csv
  data/processed/intersection/by_ring_total.csv

Uso:
  python scripts/04_intersection.py
  # ou com opções:
  python scripts/04_intersection.py --max-preview 0
"""

from pathlib import Path
import sys
import argparse
import geopandas as gpd
import pandas as pd

PROJ = Path(__file__).resolve().parents[1]
DATA_PROC = PROJ / "data" / "processed"
RINGS_PATH = DATA_PROC / "buffers" / "buffer_rings.shp"
PRODES_PATH = DATA_PROC / "deforestation_rr.shp"

OUT_DIR = DATA_PROC / "intersection"
OUT_PARQUET = OUT_DIR / "inter_prodes_rings.parquet"
OUT_BY_RING_YEAR = OUT_DIR / "by_ring_year.csv"
OUT_BY_RING = OUT_DIR / "by_ring_total.csv"

# Projeção métrica estável no Brasil
EQUAL_AREA = "EPSG:5880"   # SIRGAS 2000 / Brazil Polyconic
WGS84 = "EPSG:4326"

def info(msg): print(f"[INFO] {msg}")
def warn(msg): print(f"[AVISO] {msg}")
def err(msg):
    print(f"[ERRO] {msg}", file=sys.stderr)
    sys.exit(1)

def load_gdf(path: Path, label: str) -> gpd.GeoDataFrame:
    if not path.exists():
        err(f"{label} não encontrado: {path}")
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        gdf = gdf.set_crs(WGS84)
        warn(f"{label} sem CRS — assumindo {WGS84}.")
    return gdf

def find_year_col(gdf: gpd.GeoDataFrame) -> str | None:
    for c in gdf.columns:
        if c.lower() == "year":
            return c
    return None

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-preview", type=int, default=500,
                    help="Apenas informativo: nº máx. de feições para pré-visualizar durante o processamento (não salva). 0 desativa.")
    return ap.parse_args()

def main():
    args = parse_args()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1) Carregar dados
    info(f"Lendo anéis: {RINGS_PATH}")
    rings = load_gdf(RINGS_PATH, "Anéis").to_crs(EQUAL_AREA)
    info(f"Lendo PRODES (RR): {PRODES_PATH}")
    prodes = load_gdf(PRODES_PATH, "PRODES").to_crs(EQUAL_AREA)

    # 2) Checar coluna de ano
    year_col = find_year_col(prodes)
    if year_col is None:
        err("Coluna 'year' não encontrada no PRODES.")

    # 3) Otimização: recorte preliminar por bbox dos anéis (reduz muito)
    bbox = unary_union(rings.geometry.values).envelope
    info("Recortando PRODES pela bounding box dos anéis…")
    try:
        prodes = gpd.clip(prodes, bbox)
    except Exception:
        # bbox é um polígono; se clip falhar, faz overlay simples
        prodes = gpd.overlay(prodes, gpd.GeoDataFrame(geometry=[bbox], crs=rings.crs), how="intersection")
    info(f"PRODES após bbox-clip: {len(prodes)} feições")

    # 4) Interseção espacial PRODES × anéis
    info("Fazendo interseção espacial (overlay)… isso pode levar alguns minutos.")
    rings_keep = rings[["ring_id", "geometry"]].copy()
    try:
        inter = gpd.overlay(prodes, rings_keep, how="intersection")
    except Exception as e:
        warn(f"overlay padrão falhou ({e}). Tentando fallback com clip por anel (mais lento)…")
        parts = []
        for _, ring in rings_keep.iterrows():
            sub = gpd.clip(prodes, ring.geometry)
            if not sub.empty:
                sub = sub.copy()
                sub["ring_id"] = ring["ring_id"]
                parts.append(sub)
        if not parts:
            err("Interseção resultou vazia.")
        inter = gpd.GeoDataFrame(pd.concat(parts, ignore_index=True), crs=prodes.crs)

    if inter.empty:
        warn("Interseção vazia. Verifique se os dados se sobrepõem.")
        # ainda assim, salvar arquivos vazios coerentes
        gpd.GeoDataFrame(columns=["ring_id", year_col, "area_ha", "geometry"], geometry="geometry", crs=EQUAL_AREA)\
            .to_parquet(OUT_PARQUET, index=False)
        pd.DataFrame(columns=["ring_id", "year", "area_ha"]).to_csv(OUT_BY_RING_YEAR, index=False)
        pd.DataFrame(columns=["ring_id", "area_ha"]).to_csv(OUT_BY_RING, index=False)
        info("[OK] Arquivos vazios salvos (sem interseção).")
        return

    # 5) Área em hectares + limpeza de colunas
    inter["area_ha"] = inter.geometry.area / 10_000.0

    # Normalizar ano (int)
    try:
        inter[year_col] = inter[year_col].astype(float).round().astype(int)
    except Exception:
        pass

    keep_cols = [c for c in ["ring_id", year_col, "area_ha", "geometry"] if c in inter.columns]
    inter = inter[keep_cols].copy()

    # 6) Salvar GeoParquet (rápido para o Streamlit)
    info(f"Salvando GeoParquet: {OUT_PARQUET}")
    inter.to_parquet(OUT_PARQUET, index=False)

    # 7) Agregados prontos (CSV)
    info("Gerando agregados (CSV)…")
    by_ring_year = (
        inter.groupby(["ring_id", year_col])["area_ha"]
        .sum().reset_index().sort_values([year_col, "ring_id"])
        .rename(columns={year_col: "year"})
    )
    by_ring = (
        inter.groupby(["ring_id"])["area_ha"]
        .sum().reset_index().sort_values("area_ha", ascending=False)
    )

    by_ring_year.to_csv(OUT_BY_RING_YEAR, index=False, encoding="utf-8")
    by_ring.to_csv(OUT_BY_RING, index=False, encoding="utf-8")
    info(f"[OK] {OUT_BY_RING_YEAR}")
    info(f"[OK] {OUT_BY_RING}")

    # 8) Preview (opcional, só para log)
    if args.max_preview > 0:
        prev = inter.head(min(len(inter), args.max_preview)).copy()
        total_ha = inter["area_ha"].sum()
        info(f"Preview: {len(prev)} feições | Área total (ha): {total_ha:,.0f}".replace(",", "."))

    info("✅ Finalizado com sucesso.")

if __name__ == "__main__":
    main()
