#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
04_analyze_deforestation.py
Cruza PRODES (Roraima) com anéis de distância dos buffers de estradas.
Produz:
  - outputs/tabelas/deforestation_by_ring_year.csv  (área ha por ring_id e ano)
  - outputs/tabelas/deforestation_by_ring_total.csv (área ha total por ring_id no período filtrado)
  - outputs/graficos/area_por_faixa.png             (gráfico barra simples)
  - outputs/mapas/recorte_preview.geojson           (amostra do overlay p/ conferir no QGIS/navegador)

Uso (na raiz do projeto):
  python scripts/04_analyze_deforestation.py --year-min 2019 --year-max 2024 --class-keep DESMATAMENTO
  # ou sem filtro de classe:
  python scripts/04_analyze_deforestation.py --year-min 2008 --year-max 2024
"""

from pathlib import Path
import sys
import argparse
import warnings
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt

PROJ_ROOT = Path(__file__).resolve().parents[1]

DATA_PROC      = PROJ_ROOT / "data" / "processed"
RINGS_PATH     = DATA_PROC / "buffers" / "buffer_rings.shp"
PRODES_PATH    = DATA_PROC / "deforestation_rr.shp"

OUT_BASE       = PROJ_ROOT / "outputs"
OUT_TABLES     = OUT_BASE / "tabelas"
OUT_FIGS       = OUT_BASE / "graficos"
OUT_MAPS       = OUT_BASE / "mapas"

EQUAL_AREA = "EPSG:5880"  # SIRGAS 2000 / Brazil Polyconic (métrico)
WGS84      = "EPSG:4326"

def info(m): print(f"[INFO] {m}")
def warn(m): print(f"[AVISO] {m}")
def err(m):  print(f"[ERRO] {m}", file=sys.stderr); sys.exit(1)

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year-min", type=int, default=None, help="Ano inicial do filtro (inclusive)")
    ap.add_argument("--year-max", type=int, default=None, help="Ano final do filtro (inclusive)")
    ap.add_argument("--class-field", default="main_class", help="Campo de classe no PRODES (padrão: main_class)")
    ap.add_argument("--class-keep", nargs="*", default=None,
                    help="Lista de classes para manter (ex.: DESMATAMENTO). Se não passado, usa tudo.")
    ap.add_argument("--preview-n", type=int, default=500,
                    help="Número de feições do overlay para salvar em preview GeoJSON (default=500)")
    return ap.parse_args()

def load_gdf(path: Path, label: str) -> gpd.GeoDataFrame:
    if not path.exists(): err(f"{label} não encontrado: {path}")
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        gdf = gdf.set_crs(WGS84)
        warn(f"{label} sem CRS — assumindo {WGS84}.")
    return gdf

def ensure_dirs():
    OUT_TABLES.mkdir(parents=True, exist_ok=True)
    OUT_FIGS.mkdir(parents=True, exist_ok=True)
    OUT_MAPS.mkdir(parents=True, exist_ok=True)

def main():
    args = parse_args()
    ensure_dirs()

    # 1) Lê dados
    rings = load_gdf(RINGS_PATH, "Anéis")
    prodes = load_gdf(PRODES_PATH, "PRODES")

    # 2) Harmoniza CRS (métrico)
    rings_m  = rings.to_crs(EQUAL_AREA)
    prodes_m = prodes.to_crs(EQUAL_AREA)

    # 3) Seleciona colunas úteis e filtros
    # Campo de ano
    year_col = None
    for c in prodes_m.columns:
        if c.lower() == "year":
            year_col = c; break
    if year_col is None:
        err("Não encontrei coluna 'year' no PRODES. Verifique os campos.")

    # Filtro de anos
    if args.year_min is not None:
        prodes_m = prodes_m[prodes_m[year_col] >= args.year_min]
    if args.year_max is not None:
        prodes_m = prodes_m[prodes_m[year_col] <= args.year_max]

    # Filtro de classe (opcional)
    if args.class_keep:
        cf = args.class_field
        if cf not in prodes_m.columns:
            warn(f"Campo de classe '{cf}' não existe no PRODES; ignorando filtro de classe.")
        else:
            keep = set([k.upper() for k in args.class_keep])
            vals = prodes_m[cf].astype(str).str.upper()
            before = len(prodes_m)
            prodes_m = prodes_m[vals.isin(keep)]
            info(f"Filtro de classe {sorted(list(keep))}: {len(prodes_m)}/{before} feições mantidas.")

    if prodes_m.empty:
        err("PRODES ficou vazio após filtros (ano/classe). Ajuste os parâmetros.")

    # 4) Overlay: PRODES ∩ anéis
    info("Fazendo interseção espacial (PRODES ∩ anéis)…")
    try:
        inter = gpd.overlay(prodes_m, rings_m[["ring_id", "geometry"]], how="intersection")
    except Exception as e:
        warn(f"Falha no overlay padrão: {e}. Tentando gpd.clip em loop (mais lento).")
        parts = []
        for _, ring in rings_m.iterrows():
            sub = gpd.clip(prodes_m, ring.geometry)
            if not sub.empty:
                sub = sub.copy()
                sub["ring_id"] = ring["ring_id"]
                parts.append(sub)
        if not parts:
            err("Interseção resultou vazia.")
        inter = gpd.GeoDataFrame(pd.concat(parts, ignore_index=True), crs=prodes_m.crs)

    if inter.empty:
        err("Interseção resultou vazia. Verifique dados/CRS.")

    # 5) Área em hectares
    inter["area_ha"] = inter.geometry.area / 10_000.0

    # 6) Agregações
    # por ring_id e ano
    g_year = (inter.groupby(["ring_id", year_col])["area_ha"]
                   .sum()
                   .reset_index()
                   .sort_values([year_col, "ring_id"]))
    g_year_path = OUT_TABLES / "deforestation_by_ring_year.csv"
    g_year.to_csv(g_year_path, index=False, encoding="utf-8")
    info(f"[OK] Tabela salva: {g_year_path}")

    # total por ring_id (no período filtrado)
    g_tot = (inter.groupby(["ring_id"])["area_ha"]
                  .sum()
                  .reset_index()
                  .sort_values("area_ha", ascending=False))
    g_tot_path = OUT_TABLES / "deforestation_by_ring_total.csv"
    g_tot.to_csv(g_tot_path, index=False, encoding="utf-8")
    info(f"[OK] Tabela salva: {g_tot_path}")

    # 7) Preview de geometrias do overlay (para conferência visual)
    prev = inter.head(args.preview_n).to_crs(WGS84)
    prev_path = OUT_MAPS / "recorte_preview.geojson"
    prev.to_file(prev_path, driver="GeoJSON")
    info(f"[OK] Preview salvo: {prev_path}")

    # 8) Gráfico simples (barras) de área total por faixa
    plt.figure(figsize=(8, 5))
    plt.bar(g_tot["ring_id"], g_tot["area_ha"])
    plt.ylabel("Área desmatada (ha)")
    plt.xlabel("Faixa de distância das estradas")
    plt.title(f"PRODES em Roraima por faixa de distância\nPeríodo: "
              f"{args.year_min or int(inter[year_col].min())}–{args.year_max or int(inter[year_col].max())}"
              + (f" | Classe: {', '.join(args.class_keep)}" if args.class_keep else ""))
    plt.xticks(rotation=0)
    fig_path = OUT_FIGS / "area_por_faixa.png"
    plt.tight_layout()
    plt.savefig(fig_path, dpi=160)
    plt.close()
    info(f"[OK] Gráfico salvo: {fig_path}")

    # 9) Resumo no console
    print("\n[Resumo] Área total por faixa (ha):")
    print(g_tot.to_string(index=False))

if __name__ == "__main__":
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        main()
