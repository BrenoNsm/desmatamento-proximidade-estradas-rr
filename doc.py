#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
doc.py — Gera um PDF com análises do desmatamento (PRODES) por distância de estradas em Roraima.

Entradas esperadas (projeto):
- data/processed/intersection/intersections.duckdb            # agregados globais (by_ring_year)
- data/processed/intersection/inter_prodes_rings.parquet      # geometrias (GeoParquet) PRODES ∩ anéis
- data/processed/buffers/buffer_rings.shp                     # anéis (opcional, só p/ extents)
- data/processed/roraima_aoi.geojson                          # AOI (opcional)
- data/external/ibge_municipal/RR_Municipios_2024.shp         # municípios IBGE

Saída (padrão):
- reports/relatorio_roraima.pdf
"""

from pathlib import Path
import argparse
import io
import sys
import math
import datetime as dt

import duckdb
import pandas as pd
import geopandas as gpd
from shapely.validation import make_valid

# ReportLab
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak, KeepTogether
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import cm

# Matplotlib (gráficos)
import matplotlib
matplotlib.use("Agg")  # backend headless
import matplotlib.pyplot as plt

# Tamanhos padrão p/ imagens no PDF (caber no frame)
PAGE_IMG_W = 16*cm      # ~ largura útil
PAGE_IMG_H = 9*cm       # ~ proporção 16:9
PAGE_IMG_H_TALL = 18*cm # para gráficos “facetados” mais altos


# ----------------------------
# Caminhos do projeto
# ----------------------------
PROJ = Path(__file__).resolve().parent
DATA = PROJ / "data"
PROC = DATA / "processed"
INTER_DIR = PROC / "intersection"

DB_PATH = INTER_DIR / "intersections.duckdb"
PARQUET_PATH = INTER_DIR / "inter_prodes_rings.parquet"
MUN_PATH = DATA / "external" / "ibge_municipal" / "RR_Municipios_2024.shp"

WGS84 = "EPSG:4326"
EQUAL_AREA = "EPSG:5880"

ORDER_RINGS = ["0-5km", "5-10km", "10-20km", ">20km"]
RING_COLORS = {
    "0-5km":  "#1f77b4",
    "5-10km": "#2ca02c",
    "10-20km":"#ff7f0e",
    ">20km":  "#d62728",
}

# ----------------------------
# Utils
# ----------------------------
def fmt_int_br(x) -> str:
    try:
        return f"{int(round(float(x))):,}".replace(",", ".")
    except Exception:
        return str(x)

def fmt_float2_br(x) -> str:
    try:
        return f"{float(x):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(x)

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def read_duck(sql: str, params=None) -> pd.DataFrame:
    con = duckdb.connect(DB_PATH.as_posix(), read_only=True)
    try:
        df = con.execute(sql, params or {}).fetchdf()
    finally:
        con.close()
    return df

def load_municipios(path: Path) -> gpd.GeoDataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Shapefile de municípios não encontrado: {path}")
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        gdf = gdf.set_crs(WGS84)
    gdf = gdf.to_crs(WGS84)
    # detectar coluna de nome
    candidates = ["NM_MUN", "NM_MUNICIP", "NM_MUNICIPIO", "NOME_MUN", "NM_MUN_2024", "name"]
    name_col = next((c for c in candidates if c in gdf.columns), None)
    if name_col is None:
        gdf["MUN_NAME"] = gdf.index.astype(str)
        name_col = "MUN_NAME"
    gdf = gdf.rename(columns={name_col: "MUN_NAME"})
    gdf["MUN_NAME"] = gdf["MUN_NAME"].astype(str)
    return gdf[["MUN_NAME", "geometry"]]

def load_intersection_gpq(path: Path) -> gpd.GeoDataFrame:
    if not path.exists():
        raise FileNotFoundError(f"GeoParquet não encontrado: {path}")
    gdf = gpd.read_parquet(path)
    if gdf.crs is None:
        # GeoParquet correto carrega CRS, mas garantimos
        gdf = gdf.set_crs(EQUAL_AREA)
    return gdf

def draw_png(fig, width_px=1200, dpi=150):
    buf = io.BytesIO()
    fig.set_size_inches(width_px/dpi, (width_px/dpi)*0.56)  # 16:9 approx
    fig.savefig(buf, format="png", dpi=dpi, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf

# ----------------------------
# Gráficos
# ----------------------------
def plot_total_by_year(df_yr: pd.DataFrame) -> io.BytesIO:
    """df_yr: columns ['year','area_ha']"""
    fig, ax = plt.subplots()
    ax.plot(df_yr["year"], df_yr["area_ha"], marker="o")
    ax.set_xlabel("Ano")
    ax.set_ylabel("Área (ha)")
    ax.grid(True, alpha=.3)
    ax.set_title("Área total desmatada por ano (todas as faixas)")
    return draw_png(fig)

def plot_ring_bar(df_ring: pd.DataFrame) -> io.BytesIO:
    """df_ring: ['ring_id','area_ha']"""
    order = ORDER_RINGS
    colors = [RING_COLORS[r] for r in order]
    d = df_ring.set_index("ring_id").reindex(order)
    fig, ax = plt.subplots()
    ax.bar(d.index, d["area_ha"], color=colors)
    ax.set_xlabel("Faixa de distância")
    ax.set_ylabel("Área (ha)")
    ax.set_title("Área por faixa (acumulado no período)")
    ax.grid(True, axis="y", alpha=.3)
    return draw_png(fig)

def plot_muni_ring_stacked(df_mr: pd.DataFrame) -> io.BytesIO:
    """
    df_mr: ['MUN_NAME','ring_id','area_ha']
    barras empilhadas por município (x) com faixas como stack
    """
    muni_order = df_mr.groupby("MUN_NAME")["area_ha"].sum().sort_values(ascending=False).index.tolist()
    fig, ax = plt.subplots()
    bottom = None
    for rid in ORDER_RINGS:
        part = (df_mr[df_mr["ring_id"]==rid]
                .set_index("MUN_NAME").reindex(muni_order)["area_ha"]
                .fillna(0.0))
        ax.bar(muni_order, part, bottom=bottom, label=rid, color=RING_COLORS[rid])
        bottom = (bottom + part) if bottom is not None else part
    ax.set_xticks(range(len(muni_order)))
    ax.set_xticklabels(muni_order, rotation=45, ha="right")
    ax.set_ylabel("Área (ha)")
    ax.set_title("Município × Faixa (barras empilhadas)")
    ax.grid(True, axis="y", alpha=.3)
    ax.legend(title="Faixa")
    return draw_png(fig)

def plot_ts_facets(df_myr: pd.DataFrame, munis: list[str]) -> io.BytesIO:
    """
    df_myr: ['MUN_NAME','year','ring_id','area_ha'] — facetas simples: 2 colunas
    Renderizamos como um único gráfico “alto” com subplots por município (linhas), e cores por faixa.
    """
    n = len(munis)
    if n == 0:
        munis = df_myr["MUN_NAME"].unique().tolist()
        n = len(munis)
    rows = n
    fig, axes = plt.subplots(rows, 1, figsize=(12, max(3.2*rows, 3.2)), sharex=True)
    if rows == 1:
        axes = [axes]
    for ax, mun in zip(axes, munis):
        sub = df_myr[df_myr["MUN_NAME"]==mun].copy()
        for rid in ORDER_RINGS:
            part = sub[sub["ring_id"]==rid]
            if part.empty:
                continue
            ax.plot(part["year"], part["area_ha"], marker="o", label=rid, color=RING_COLORS[rid])
        ax.set_title(mun)
        ax.grid(True, alpha=.3)
        ax.set_ylabel("Área (ha)")
    axes[-1].set_xlabel("Ano")
    # legenda fora
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, title="Faixa", loc="upper center", ncol=4)
    fig.tight_layout(rect=(0,0,1,0.93))
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf

# ----------------------------
# Construção do PDF
# ----------------------------
def build_pdf(output_path: Path,
              years: tuple[int,int] | None,
              municipios_filtro: list[str] | None):

    # --- checagens
    if not DB_PATH.exists():
        print(f"[ERRO] DuckDB não encontrado: {DB_PATH}", file=sys.stderr); sys.exit(2)
    if not PARQUET_PATH.exists():
        print(f"[ERRO] GeoParquet não encontrado: {PARQUET_PATH}", file=sys.stderr); sys.exit(2)

    # --- datasets
    # agregados globais (rápido)
    yrs = read_duck("SELECT MIN(year) AS y0, MAX(year) AS y1 FROM by_ring_year;")
    y0, y1 = int(yrs["y0"].iloc[0]), int(yrs["y1"].iloc[0])

    if years is None:
        y_min, y_max = y0, y1
    else:
        y_min, y_max = max(y0, years[0]), min(y1, years[1])

    # agregados por ano/faixa (para resumo/figuras rápidas)
    by_ring_year = read_duck("""
        SELECT ring_id, year, area_ha
        FROM by_ring_year
        WHERE year BETWEEN ? AND ?
    """, [y_min, y_max])

    # total por faixa (período)
    by_ring = (by_ring_year.groupby("ring_id", as_index=False)["area_ha"].sum()
               .sort_values("ring_id"))
    # total por ano
    by_year = (by_ring_year.groupby("year", as_index=False)["area_ha"].sum()
               .sort_values("year"))

    # GeoParquet para detalhamento espacial (municipal)
    inter = load_intersection_gpq(PARQUET_PATH)
    # normaliza nomes de colunas
    year_col = next(c for c in inter.columns if c.lower()=="year")
    inter = inter.rename(columns={year_col: "year"})
    # filtro por ano
    inter = inter[(inter["year"]>=y_min) & (inter["year"]<=y_max)].copy()
    # garante CRS e área em ha correta
    if inter.crs is None:
        inter = inter.set_crs(EQUAL_AREA)
    elif str(inter.crs).lower() != EQUAL_AREA.lower():
        inter = inter.to_crs(EQUAL_AREA)
    # recalcula área para garantir
    inter["area_ha"] = inter.geometry.area / 10_000.0

    # Municípios (WGS84) e reprojeta p/ equal-area para sjoin robusto
    mun = load_municipios(MUN_PATH)
    mun = mun.to_crs(EQUAL_AREA)

    # spatial join
    # (para performance, recorte por bbox geral antes)
    bbox = mun.union_all().envelope
    inter_clip = gpd.clip(inter, bbox)
    inter_muni = gpd.sjoin(
        inter_clip[["ring_id","year","area_ha","geometry"]],
        mun[["MUN_NAME","geometry"]],
        how="inner",
        predicate="intersects"
    ).drop(columns=["index_right"])

    # filtro por municípios (opcional)
    if municipios_filtro:
        inter_muni = inter_muni[inter_muni["MUN_NAME"].isin(municipios_filtro)].copy()

    # agregados municipais
    muni_total = (inter_muni.groupby("MUN_NAME", as_index=False)["area_ha"].sum()
                  .sort_values("area_ha", ascending=False))
    muni_ring = (inter_muni.groupby(["MUN_NAME","ring_id"], as_index=False)["area_ha"].sum())
    muni_year_ring = (inter_muni.groupby(["MUN_NAME","year","ring_id"], as_index=False)["area_ha"].sum()
                      .sort_values(["MUN_NAME","year","ring_id"]))

    # rankings (Top 10)
    top_total = muni_total.head(10).copy()
    # último ano disponível do recorte
    last_year = y_max
    top_last_year = (muni_year_ring[muni_year_ring["year"]==last_year]
                     .groupby("MUN_NAME", as_index=False)["area_ha"].sum()
                     .sort_values("area_ha", ascending=False).head(10))

    # ---------------- PDF ----------------
    ensure_dir(output_path.parent)
    doc = SimpleDocTemplate(
        output_path.as_posix(),
        pagesize=A4,
        leftMargin=1.6*cm, rightMargin=1.6*cm,
        topMargin=1.2*cm, bottomMargin=1.2*cm
    )
    styles = getSampleStyleSheet()
    H1 = ParagraphStyle('H1', parent=styles['Heading1'], fontSize=18, spaceAfter=10)
    H2 = ParagraphStyle('H2', parent=styles['Heading2'], fontSize=14, spaceAfter=8)
    P  = styles['BodyText']

    story = []

    # Capa
    title = "Dinâmica do Desmatamento em Roraima em Função da Proximidade das Estradas"
    subt = f"Período analisado: {y_min}–{y_max}"
    hoje = dt.datetime.now().strftime("%d/%m/%Y %H:%M")
    story += [
        Spacer(1, 1.0*cm),
        Paragraph(title, H1),
        Paragraph(subt, P),
        Spacer(1, 0.2*cm),
        Paragraph("Fonte: INPE/PRODES, OSM (estradas), IBGE (municípios).", P),
        Spacer(1, 0.2*cm),
        Paragraph(f"Geração: {hoje}", P),
        PageBreak()
    ]

    # Sumário geral
    story += [Paragraph("1. Sumário geral", H1)]
    total_area = by_year["area_ha"].sum()
    n_munis = muni_total["MUN_NAME"].nunique()
    bullets = [
        f"Área total desmatada no período (todas as faixas): <b>{fmt_float2_br(total_area)}</b> ha",
        f"Anos: <b>{y_min}–{y_max}</b>",
        f"Municípios com ocorrência: <b>{fmt_int_br(n_munis)}</b>",
    ]
    for b in bullets:
        story += [Paragraph("• " + b, P)]
    story += [Spacer(1, 0.4*cm)]

    # gráfico total por ano
    img_total_by_year = plot_total_by_year(by_year.rename(columns={"area_ha":"area_ha","year":"year"}))
    story += [Image(img_total_by_year, width=PAGE_IMG_W, height=PAGE_IMG_H), Spacer(1, 0.5*cm)]


    # barras por faixa
    img_ring_bar = plot_ring_bar(by_ring.copy())
    story += [Image(img_ring_bar, width=PAGE_IMG_W, height=PAGE_IMG_H), PageBreak()]


    # 2. Rankings
    story += [Paragraph("2. Rankings de desmatamento (ha)", H1)]
    story += [Paragraph("2.1. Top 10 — acumulado no período", H2)]
    table_data = [["#","Município","Área (ha)"]]
    for i, row in enumerate(top_total.itertuples(index=False), start=1):
        table_data.append([i, row.MUN_NAME, fmt_float2_br(row.area_ha)])
    t = Table(table_data, hAlign="LEFT", colWidths=[1.2*cm, 9*cm, 4*cm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#E5E7EB")),
        ("TEXTCOLOR", (0,0), (-1,0), colors.black),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("ALIGN", (0,0), (0,-1), "CENTER"),
        ("ALIGN", (-1,1), (-1,-1), "RIGHT"),
        ("INNERGRID", (0,0), (-1,-1), 0.25, colors.HexColor("#D1D5DB")),
        ("BOX", (0,0), (-1,-1), 0.5, colors.HexColor("#9CA3AF")),
    ]))
    story += [t, Spacer(1, 0.5*cm)]

    story += [Paragraph(f"2.2. Top 10 — ano {last_year}", H2)]
    table_data = [["#","Município",f"Área (ha) {last_year}"]]
    for i, row in enumerate(top_last_year.itertuples(index=False), start=1):
        table_data.append([i, row.MUN_NAME, fmt_float2_br(row.area_ha)])
    t = Table(table_data, hAlign="LEFT", colWidths=[1.2*cm, 9*cm, 4*cm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#E5E7EB")),
        ("FONTNAME", (0,0), (-1,0), "Times New Roman"),
        ("ALIGN", (0,0), (0,-1), "CENTER"),
        ("ALIGN", (-1,1), (-1,-1), "RIGHT"),
        ("INNERGRID", (0,0), (-1,-1), 0.25, colors.HexColor("#D1D5DB")),
        ("BOX", (0,0), (-1,-1), 0.5, colors.HexColor("#9CA3AF")),
    ]))
    story += [t, PageBreak()]

    # 3. Detalhe municipal (agregado e gráficos)
    story += [Paragraph("3. Detalhe por município", H1)]
    # tabela geral por município
    muni_tbl = [["Município","Área (ha) (período)"]]
    for row in muni_total.itertuples(index=False):
        muni_tbl.append([row.MUN_NAME, fmt_float2_br(row.area_ha)])
    t = Table(muni_tbl, hAlign="LEFT", colWidths=[10*cm, 4.5*cm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#E5E7EB")),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("ALIGN", (-1,1), (-1,-1), "RIGHT"),
        ("INNERGRID", (0,0), (-1,-1), 0.25, colors.HexColor("#D1D5DB")),
        ("BOX", (0,0), (-1,-1), 0.5, colors.HexColor("#9CA3AF")),
    ]))
    story += [t, Spacer(1, 0.4*cm)]

    # gráfico barras empilhadas (top N p/ caber)
    top_munis_list = muni_total.head(12)["MUN_NAME"].tolist()
    img_muni_stack = plot_muni_ring_stacked(muni_ring[muni_ring["MUN_NAME"].isin(top_munis_list)].copy())
    story += [Image(img_muni_stack, width=PAGE_IMG_W, height=PAGE_IMG_H), PageBreak()]


    # Facetas por município (séries) em blocos para não ficar gigante
    chunk_size = 4
    muni_list = muni_total["MUN_NAME"].tolist()
    if municipios_filtro:
        # respeita ordem do filtro, se vieram poucos
        muni_list = [m for m in municipios_filtro if m in muni_list]
    for i in range(0, len(muni_list), chunk_size):
        chunk = muni_list[i:i+chunk_size]
        story += [Paragraph(f"3.{i//chunk_size+1} Séries temporais — municípios {i+1}–{i+len(chunk)}", H2)]
        img = plot_ts_facets(muni_year_ring[muni_year_ring["MUN_NAME"].isin(chunk)].copy(), chunk)
        story += [Image(img, width=PAGE_IMG_W, height=PAGE_IMG_H_TALL), PageBreak()]


    # 4. Metodologia resumida
    story += [Paragraph("4. Metodologia (resumo)", H1)]
    metod = (
        """O recorte espacial corresponde ao estado de Roraima, conforme limites do IBGE (2024). A rede viária foi obtida do OpenStreetMap e utilizada como referência para a construção de anéis de distância exclusivos em torno das estradas, definidos nas faixas de 0–5 km, 5–10 km, 10–20 km e acima de 20 km. Os polígonos anuais de desmatamento do PRODES, no período de 2008 a 2024, foram recortados para o limite estadual e reprojetados para o sistema de área equivalente EPSG:5880. As interseções entre os polígonos do PRODES e os anéis de distância foram então calculadas, e as áreas resultantes expressas em hectares (1 ha = 10.000 m²). Finalmente, os dados foram agregados por ano, por faixa de distância e por município, conforme a divisão oficial do IBGE (2024)."""
    )
    story += [Paragraph(metod, P)]
    story += [Spacer(1, 0.2*cm)]
    story += [Paragraph("Obs.: este relatório foi gerado automaticamente a partir dos arquivos do projeto. "
                        "Para reproduzir, verifique a existência dos arquivos DuckDB e GeoParquet descritos no cabeçalho.", P)]

    # build
    doc.build(story)
    print(f"[OK] PDF gerado em: {output_path}")
    

# === NOVO: gerar PDF em memória (bytes) para o Streamlit ===
def build_pdf_bytes(years: tuple[int,int] | None, municipios_filtro: list[str] | None) -> bytes:
    """
    Gera o PDF em memória e retorna bytes (para usar no Streamlit).
    Usa o mesmo conteúdo do build_pdf(), mas em buffer BytesIO.
    """
    import io
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DuckDB não encontrado: {DB_PATH}")
    if not PARQUET_PATH.exists():
        raise FileNotFoundError(f"GeoParquet não encontrado: {PARQUET_PATH}")

    # Reutiliza a lógica de build_pdf, mas monta 'story' aqui e grava em buffer.
    # (Copiamos o corpo de build_pdf até a parte de doc.build(story))
    # --- INÍCIO bloco reaproveitado ---
    # anos limites
    yrs = read_duck("SELECT MIN(year) AS y0, MAX(year) AS y1 FROM by_ring_year;")
    y0, y1 = int(yrs["y0"].iloc[0]), int(yrs["y1"].iloc[0])
    if years is None:
        y_min, y_max = y0, y1
    else:
        y_min, y_max = max(y0, years[0]), min(y1, years[1])

    by_ring_year = read_duck("""
        SELECT ring_id, year, area_ha
        FROM by_ring_year
        WHERE year BETWEEN ? AND ?
    """, [y_min, y_max])
    by_ring = (by_ring_year.groupby("ring_id", as_index=False)["area_ha"].sum()
               .sort_values("ring_id"))
    by_year = (by_ring_year.groupby("year", as_index=False)["area_ha"].sum()
               .sort_values("year"))

    inter = load_intersection_gpq(PARQUET_PATH)
    year_col = next(c for c in inter.columns if c.lower()=="year")
    inter = inter.rename(columns={year_col: "year"})
    inter = inter[(inter["year"]>=y_min) & (inter["year"]<=y_max)].copy()
    if inter.crs is None:
        inter = inter.set_crs(EQUAL_AREA)
    elif str(inter.crs).lower() != EQUAL_AREA.lower():
        inter = inter.to_crs(EQUAL_AREA)
    inter["area_ha"] = inter.geometry.area / 10_000.0

    mun = load_municipios(MUN_PATH).to_crs(EQUAL_AREA)
    bbox = mun.union_all().envelope
    inter_clip = gpd.clip(inter, bbox)
    inter_muni = gpd.sjoin(
        inter_clip[["ring_id","year","area_ha","geometry"]],
        mun[["MUN_NAME","geometry"]],
        how="inner",
        predicate="intersects"
    ).drop(columns=["index_right"])
    if municipios_filtro:
        inter_muni = inter_muni[inter_muni["MUN_NAME"].isin(municipios_filtro)].copy()

    muni_total = (inter_muni.groupby("MUN_NAME", as_index=False)["area_ha"].sum()
                  .sort_values("area_ha", ascending=False))
    muni_ring = (inter_muni.groupby(["MUN_NAME","ring_id"], as_index=False)["area_ha"].sum())
    muni_year_ring = (inter_muni.groupby(["MUN_NAME","year","ring_id"], as_index=False)["area_ha"].sum()
                      .sort_values(["MUN_NAME","year","ring_id"]))

    top_total = muni_total.head(10).copy()
    last_year = y_max
    top_last_year = (muni_year_ring[muni_year_ring["year"]==last_year]
                     .groupby("MUN_NAME", as_index=False)["area_ha"].sum()
                     .sort_values("area_ha", ascending=False).head(10))
    # --- FIM bloco de dados ---

    # === estilos / tamanhos ===
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import cm
    PAGE_IMG_W = 16*cm
    PAGE_IMG_H = 9*cm
    PAGE_IMG_H_TALL = 18*cm

    styles = getSampleStyleSheet()
    H1 = ParagraphStyle('H1', parent=styles['Heading1'], fontSize=18, spaceAfter=10)
    H2 = ParagraphStyle('H2', parent=styles['Heading2'], fontSize=14, spaceAfter=8)
    P  = styles['BodyText']

    # === figuras ===
    img_total_by_year = plot_total_by_year(by_year.rename(columns={"area_ha":"area_ha","year":"year"}))
    img_ring_bar = plot_ring_bar(by_ring.copy())
    top_munis_list = muni_total.head(12)["MUN_NAME"].tolist()
    img_muni_stack = plot_muni_ring_stacked(muni_ring[muni_ring["MUN_NAME"].isin(top_munis_list)].copy())

    # === story ===
    story = []
    title = "Dinâmica do Desmatamento em Roraima em Função da Proximidade das Estradas"
    subt = f"Período analisado: {y_min}–{y_max}"

    import datetime as dt
    hoje = dt.datetime.now().strftime("%d/%m/%Y %H:%M")
    story += [
        Spacer(1, 1.0*cm),
        Paragraph(title, H1),
        Paragraph(subt, P),
        Spacer(1, 0.2*cm),
        Paragraph("Fonte: INPE/PRODES, OSM (estradas), IBGE (municípios). Agregados: DuckDB; Geometrias: GeoParquet.", P),
        Spacer(1, 0.2*cm),
        Paragraph(f"Geração: {hoje}", P),
        PageBreak()
    ]

    total_area = by_year["area_ha"].sum()
    n_munis = muni_total["MUN_NAME"].nunique()
    story += [Paragraph("1. Sumário geral", H1)]
    bullets = [
        f"Área total desmatada (todas as faixas): <b>{fmt_float2_br(total_area)}</b> ha",
        f"Anos: <b>{y_min}–{y_max}</b>",
        f"Municípios com ocorrência: <b>{fmt_int_br(n_munis)}</b>",
    ]
    for b in bullets:
        story += [Paragraph("• " + b, P)]
    story += [Spacer(1, 0.4*cm)]
    story += [Image(img_total_by_year, width=PAGE_IMG_W, height=PAGE_IMG_H), Spacer(1, 0.5*cm)]
    story += [Image(img_ring_bar, width=PAGE_IMG_W, height=PAGE_IMG_H), PageBreak()]

    story += [Paragraph("2. Rankings de desmatamento (ha)", H1)]
    story += [Paragraph("2.1. Top 10 — acumulado no período", H2)]
    table_data = [["#","Município","Área (ha)"]]
    for i, row in enumerate(top_total.itertuples(index=False), start=1):
        table_data.append([i, row.MUN_NAME, fmt_float2_br(row.area_ha)])
    from reportlab.platypus import Table
    t = Table(table_data, hAlign="LEFT", colWidths=[1.2*cm, 9*cm, 4*cm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#E5E7EB")),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("ALIGN", (0,0), (0,-1), "CENTER"),
        ("ALIGN", (-1,1), (-1,-1), "RIGHT"),
        ("INNERGRID", (0,0), (-1,-1), 0.25, colors.HexColor("#D1D5DB")),
        ("BOX", (0,0), (-1,-1), 0.5, colors.HexColor("#9CA3AF")),
    ]))
    story += [t, Spacer(1, 0.5*cm)]

    story += [Paragraph(f"2.2. Top 10 — ano {y_max}", H2)]
    table_data = [["#","Município",f"Área (ha) {y_max}"]]
    for i, row in enumerate(top_last_year.itertuples(index=False), start=1):
        table_data.append([i, row.MUN_NAME, fmt_float2_br(row.area_ha)])
    t = Table(table_data, hAlign="LEFT", colWidths=[1.2*cm, 9*cm, 4*cm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#E5E7EB")),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("ALIGN", (0,0), (0,-1), "CENTER"),
        ("ALIGN", (-1,1), (-1,-1), "RIGHT"),
        ("INNERGRID", (0,0), (-1,-1), 0.25, colors.HexColor("#D1D5DB")),
        ("BOX", (0,0), (-1,-1), 0.5, colors.HexColor("#9CA3AF")),
    ]))
    story += [t, PageBreak()]

    story += [Paragraph("3. Detalhe por município", H1)]
    muni_tbl = [["Município","Área (ha) (período)"]]
    for row in muni_total.itertuples(index=False):
        muni_tbl.append([row.MUN_NAME, fmt_float2_br(row.area_ha)])
    t = Table(muni_tbl, hAlign="LEFT", colWidths=[10*cm, 4.5*cm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#E5E7EB")),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("ALIGN", (-1,1), (-1,-1), "RIGHT"),
        ("INNERGRID", (0,0), (-1,-1), 0.25, colors.HexColor("#D1D5DB")),
        ("BOX", (0,0), (-1,-1), 0.5, colors.HexColor("#9CA3AF")),
    ]))
    story += [t, Spacer(1, 0.4*cm)]

    story += [Image(img_muni_stack, width=PAGE_IMG_W, height=PAGE_IMG_H), PageBreak()]

    # facetas por município em blocos
    chunk_size = 4
    muni_list = muni_total["MUN_NAME"].tolist()
    if municipios_filtro:
        muni_list = [m for m in municipios_filtro if m in muni_list]
    for i in range(0, len(muni_list), chunk_size):
        chunk = muni_list[i:i+chunk_size]
        story += [Paragraph(f"Séries temporais — municípios {i+1}–{i+len(chunk)}", H2)]
        img = plot_ts_facets(muni_year_ring[muni_year_ring["MUN_NAME"].isin(chunk)].copy(), chunk)
        story += [Image(img, width=PAGE_IMG_W, height=PAGE_IMG_H_TALL), PageBreak()]

    metod = (
        "Os polígonos anuais de desmatamento do PRODES foram intersectados com anéis exclusivos de distância às "
        "estradas do OpenStreetMap (0–5 km, 5–10 km, 10–20 km e >20 km), todos em projeção equivalente (EPSG:5880). "
        "As áreas foram calculadas em hectares (1 ha = 10.000 m²). Em seguida, agregamos por ano, por faixa de distância e "
        "por município (IBGE)."
    )
    story += [Paragraph("Metodologia (resumo)", H1), Paragraph(metod, P)]

    # === monta PDF em memória ===
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=1.6*cm, rightMargin=1.6*cm, topMargin=1.2*cm, bottomMargin=1.2*cm
    )
    doc.build(story)
    buf.seek(0)
    return buf.read()


# ----------------------------
# CLI
# ----------------------------
def main():
    ap = argparse.ArgumentParser(description="Gera relatório PDF (PRODES × anéis) para Roraima")
    ap.add_argument("--out", type=str, default=str(PROJ / "reports" / "relatorio_roraima.pdf"),
                    help="Caminho do PDF de saída (padrão: reports/relatorio_roraima.pdf)")
    ap.add_argument("--years", nargs=2, type=int, metavar=("ANO_MIN","ANO_MAX"),
                    help="Recorte de anos (ex.: --years 2018 2024)")
    ap.add_argument("--municipios", nargs="*", type=str,
                    help="Lista de municípios para filtrar (opcional). Use nomes conforme o shapefile do IBGE.")
    args = ap.parse_args()

    out = Path(args.out)
    years = tuple(args.years) if args.years else None
    munis = args.municipios if args.municipios else None

    build_pdf(out, years, munis)

if __name__ == "__main__":
    main()
