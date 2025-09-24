#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
01_prepare_osm_rr_ibge.py  —  usa IBGE (UF) para AOI de Roraima
- Lê estradas do OSM em data/osm/gis_osm_roads_free_1.shp
- Lê malha de UFs do IBGE em data/external/ibge_uf/*.shp
- Extrai polígono de Roraima (sigla RR ou nome 'Roraima')
- Recorta estradas para dentro de Roraima
- Salva:
    data/processed/roraima_aoi.geojson
    data/processed/roads_rr.shp

Rodar (na raiz do projeto):
    python .\scripts\01_prepare_osm_rr_ibge.py
"""

from pathlib import Path
import sys
import geopandas as gpd

PROJ_ROOT = Path(__file__).resolve().parents[1]
DATA_OSM  = PROJ_ROOT / "data" / "osm"
DATA_IBGE = PROJ_ROOT / "data" / "external" / "ibge_uf"
OUT_DIR   = PROJ_ROOT / "data" / "processed"

ROADS_NAME = "gis_osm_roads_free_1.shp"
DEFAULT_GEO = "EPSG:4326"

UF_SIGLA_CANDS = ["SIGLA_UF", "SIGLA", "CD_UF", "UF", "UF_SIGLA", "SG_UF"]
UF_NOME_CANDS  = ["NM_UF", "NOME_UF", "NM_ESTADO", "NMUF", "NOME", "NOME_ESTADO"]

def info(m): print(f"[INFO] {m}")
def warn(m): print(f"[AVISO] {m}")
def err(m):
    print(f"[ERRO] {m}", file=sys.stderr); sys.exit(1)

def ensure_dir(p: Path): p.mkdir(parents=True, exist_ok=True)

def load_gdf(path: Path, label: str) -> gpd.GeoDataFrame:
    if not path.exists(): err(f"{label} não encontrado: {path}")
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        gdf.set_crs(DEFAULT_GEO, inplace=True)
        warn(f"{label} sem CRS — assumindo {DEFAULT_GEO}.")
    return gdf

def find_col(gdf: gpd.GeoDataFrame, candidates) -> str | None:
    lower = {c.lower(): c for c in gdf.columns}
    for c in candidates:
        if c.lower() in lower: return lower[c.lower()]
    return None

def pick_rr_from_ibge(ibge: gpd.GeoDataFrame) -> gpd.GeoDataFrame | None:
    sigla = find_col(ibge, UF_SIGLA_CANDS)
    if sigla:
        m = ibge[sigla].astype(str).str.upper().str.strip().eq("RR")
        rr = ibge[m].copy()
        if not rr.empty: return rr.dissolve().reset_index(drop=True)
    nome = find_col(ibge, UF_NOME_CANDS)
    if nome:
        m = ibge[nome].astype(str).str.lower().str.strip().eq("roraima")
        rr = ibge[m].copy()
        if not rr.empty: return rr.dissolve().reset_index(drop=True)
    # fallback: qualquer coluna contendo "roraima"
    for col in ibge.columns:
        if ibge[col].dtype == object:
            m = ibge[col].astype(str).str.lower().str.contains("roraima", na=False)
            rr = ibge[m].copy()
            if not rr.empty: return rr.dissolve().reset_index(drop=True)
    return None

def clip_roads(roads: gpd.GeoDataFrame, aoi: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if roads.crs != aoi.crs:
        roads = roads.to_crs(aoi.crs)
    try:
        return gpd.clip(roads, aoi)
    except Exception:
        return gpd.overlay(roads, aoi[["geometry"]], how="intersection")

def main():
    ensure_dir(OUT_DIR)

    # 1) Estradas do OSM
    roads_path = DATA_OSM / ROADS_NAME
    info(f"Lendo estradas OSM: {roads_path}")
    roads = load_gdf(roads_path, "roads OSM")

    # 2) IBGE UFs
    if not DATA_IBGE.exists():
        err(f"Pasta do IBGE não existe: {DATA_IBGE}. Coloque lá o shapefile de UFs (ex.: BR_UF_2024.shp).")
    shp_list = list(DATA_IBGE.glob("*.shp")) or list(DATA_IBGE.rglob("*.shp"))
    if not shp_list:
        err(f"Não encontrei .shp em {DATA_IBGE}. Extraia o zip do IBGE ali.")
    # prioriza nomes que parecem UF
    shp_list.sort(key=lambda p: (("UF" not in p.name.upper()), p.name.upper()))
    ibge_path = shp_list[0]
    info(f"Lendo IBGE UFs: {ibge_path}")
    ibge = load_gdf(ibge_path, "IBGE UFs")

    # 3) Extrai Roraima
    aoi_rr = pick_rr_from_ibge(ibge)
    if aoi_rr is None or aoi_rr.empty:
        err("Não achei Roraima no shapefile do IBGE (nem por sigla, nem por nome).")

    # 4) Salva AOI
    aoi_out = OUT_DIR / "roraima_aoi.geojson"
    aoi_rr.to_file(aoi_out, driver="GeoJSON")
    info(f"[OK] AOI salva: {aoi_out}")

    # 5) Recorta estradas
    info("Recortando estradas dentro de Roraima…")
    roads_rr = clip_roads(roads, aoi_rr)
    roads_out = OUT_DIR / "roads_rr.shp"
    roads_rr.to_file(roads_out)
    info(f"[OK] Estradas salvas: {roads_out}")
    info(f"[OK] Total de segmentos: {len(roads_rr)}")

if __name__ == "__main__":
    main()
