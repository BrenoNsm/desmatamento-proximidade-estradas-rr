#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
03_create_buffers.py  (robusto, em chunks)
- Lê AOI (data/processed/roraima_aoi.geojson) e estradas RR (data/processed/roads_rr.shp)
- Cria buffers em km (ex.: 5 10 20) em LOTES para evitar 'bad allocation'
- Constrói anéis (0–d1, d1–d2, ..., >dmax), recortados à AOI
- Saídas:
    data/processed/buffers/roads_buffer_5km.shp (etc)
    data/processed/buffers/buffer_rings.shp
    data/processed/buffers/buffer_rings_preview.geojson

Uso:
    python scripts/03_create_buffers.py --dist 5 10 20 [--chunk-size 20000] [--road-classes primary secondary tertiary]
"""

from pathlib import Path
import sys
import argparse
import geopandas as gpd
import pandas as pd
from shapely.ops import unary_union

PROJ_ROOT = Path(__file__).resolve().parents[1]
DATA_PROC = PROJ_ROOT / "data" / "processed"
OUT_DIR   = DATA_PROC / "buffers"

AOI_PATH   = DATA_PROC / "roraima_aoi.geojson"
ROADS_PATH = DATA_PROC / "roads_rr.shp"

EQUAL_AREA = "EPSG:5880"  # SIRGAS 2000 / Brazil Polyconic
WGS84      = "EPSG:4326"

def info(m): print(f"[INFO] {m}")
def warn(m): print(f"[AVISO] {m}")
def err(m):
    print(f"[ERRO] {m}", file=sys.stderr); sys.exit(1)

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dist", nargs="+", type=float, required=True, help="Distâncias de buffer em km (ex.: 5 10 20)")
    ap.add_argument("--chunk-size", type=int, default=20000, help="Tamanho do lote para buffer (default=20000)")
    ap.add_argument("--road-classes", nargs="*", default=None,
                    help="Filtrar estradas por fclass (ex.: primary secondary tertiary trunk motorway).")
    return ap.parse_args()

def load_gdf(path: Path, label: str) -> gpd.GeoDataFrame:
    if not path.exists(): err(f"{label} não encontrado: {path}")
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        gdf = gdf.set_crs(WGS84)
        warn(f"{label} sem CRS — assumindo {WGS84}.")
    return gdf

def filter_by_fclass(roads: gpd.GeoDataFrame, keep: list[str]) -> gpd.GeoDataFrame:
    if "fclass" not in roads.columns:
        warn("Coluna 'fclass' não existe; ignorando filtro de classes.")
        return roads
    kk = set([k.lower() for k in keep])
    m = roads["fclass"].astype(str).str.lower().isin(kk)
    n0, n1 = len(roads), int(m.sum())
    info(f"Filtrando por fclass {sorted(kk)} -> {n1}/{n0} segmentos.")
    return roads.loc[m].copy()

def buffer_in_chunks(roads_m: gpd.GeoDataFrame, aoi_m: gpd.GeoDataFrame, d_m: float, chunk_size: int):
    """
    Faz buffer por LOTES e recorta pela AOI a cada lote para economizar memória.
    Retorna uma geometria unificada (MultiPolygon/Polygon) do buffer total.
    """
    total = len(roads_m)
    parts = []
    for i in range(0, total, chunk_size):
        j = min(i + chunk_size, total)
        info(f"  - Lote {i}:{j} de {total} (buffer {d_m/1000:.1f} km)")
        # buffer vetorizado no lote
        buf_geoms = roads_m.geometry.iloc[i:j].buffer(d_m)
        # une o lote
        lot_union = unary_union(buf_geoms.values)
        # recorta pelo limite da AOI (reduz tamanho)
        lot_gdf = gpd.GeoDataFrame(geometry=[lot_union], crs=roads_m.crs)
        lot_clip = gpd.overlay(lot_gdf, aoi_m[["geometry"]], how="intersection")
        if not lot_clip.empty:
            parts.append(lot_clip.geometry.iloc[0])
        # Libera referências
        del buf_geoms, lot_union, lot_gdf, lot_clip

    if not parts:
        return None
    return unary_union(parts)

def main():
    args = parse_args()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1) carrega AOI e estradas
    aoi = load_gdf(AOI_PATH, "AOI").to_crs(EQUAL_AREA)
    roads = load_gdf(ROADS_PATH, "Estradas")

    # filtro opcional por fclass
    if args.road_classes:
        roads = filter_by_fclass(roads, args.road_classes)
        if roads.empty:
            err("Filtro por fclass resultou em zero estradas. Remova o filtro ou verifique valores.")

    roads_m = roads.to_crs(EQUAL_AREA)

    # 2) buffers por distância (em chunks)
    dists_km = sorted(set(args.dist))
    buffers = []
    for d in dists_km:
        d_m = d * 1000.0
        info(f"Criando buffer de {d} km em chunks de {args.chunk_size}…")
        union_buf = buffer_in_chunks(roads_m, aoi, d_m, args.chunk_size)
        if union_buf is None:
            warn(f"Nenhuma geometria no buffer de {d} km (talvez estradas vazias?). Pulando.")
            continue
        buf_gdf = gpd.GeoDataFrame({"dist_km":[d]}, geometry=[union_buf], crs=EQUAL_AREA)
        out = OUT_DIR / f"roads_buffer_{int(d)}km.shp"
        buf_gdf.to_file(out)
        info(f"[OK] salvo: {out}")
        buffers.append((d, buf_gdf))

    if not buffers:
        err("Não foi possível gerar nenhum buffer. Verifique os dados de estradas.")

    # 3) anéis 0–d1, d1–d2, ..., >dmax dentro da AOI
    rings = []
    # anel 0–d1
    d0, g0 = buffers[0]
    rings.append({"ring_id": f"0-{int(d0)}km", "min_km": 0.0, "max_km": float(d0), "geometry": g0.geometry.iloc[0]})
    # anéis intermediários
    prev_d, prev_g = d0, g0
    for d, g in buffers[1:]:
        ring_geom = g.geometry.iloc[0].difference(prev_g.geometry.iloc[0])
        rings.append({"ring_id": f"{int(prev_d)}-{int(d)}km", "min_km": float(prev_d), "max_km": float(d), "geometry": ring_geom})
        prev_d, prev_g = d, g
    # > dmax
    dmax, gmax = buffers[-1]
    outside = aoi.geometry.unary_union.difference(gmax.geometry.iloc[0])
    rings.append({"ring_id": f">{int(dmax)}km", "min_km": float(dmax), "max_km": None, "geometry": outside})

    rings_gdf = gpd.GeoDataFrame(rings, geometry="geometry", crs=EQUAL_AREA)
    rings_out = OUT_DIR / "buffer_rings.shp"
    rings_gdf.to_file(rings_out)
    info(f"[OK] Anéis salvos: {rings_out}")

    rings_prev = rings_gdf.to_crs(WGS84)
    rings_prev.to_file(OUT_DIR / "buffer_rings_preview.geojson", driver="GeoJSON")
    info(f"[OK] Preview: {OUT_DIR / 'buffer_rings_preview.geojson'}")

    # resumo de áreas
    areas = rings_gdf.copy()
    areas["area_km2"] = areas.geometry.area / 1_000_000.0
    print("\n[Resumo] Área dos anéis (km²):")
    print(areas[["ring_id","area_km2"]])

if __name__ == "__main__":
    main()
