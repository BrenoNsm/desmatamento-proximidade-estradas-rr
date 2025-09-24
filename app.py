# app.py — DuckDB + GeoParquet (sem amostragem) com cards, rótulos e gráficos
# Requisitos (venv):
#   pip install streamlit streamlit-folium duckdb geopandas folium pyarrow shapely altair
# Execução:
#   streamlit run app.py --server.fileWatcherType=none

from pathlib import Path
import streamlit as st
import duckdb
import pandas as pd
import geopandas as gpd
import folium
from streamlit_folium import st_folium
import altair as alt

# -------- formatos e paleta --------
def fmt_int_br(x: float) -> str:
    try:
        return f"{int(round(float(x))):,}".replace(",", ".")
    except Exception:
        return str(x)

def fmt_float2_br(x: float) -> str:
    try:
        return f"{float(x):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(x)

# ordem e cores fixas por faixa (gráficos + mapa)
ORDER_RINGS = ["0-5km", "5-10km", "10-20km", ">20km"]
RING_COLORS = {
    "0-5km":  "#1f77b4",  # azul
    "5-10km": "#2ca02c",  # verde
    "10-20km":"#ff7f0e",  # laranja
    ">20km":  "#d62728",  # vermelho
}


# -----------------------
# Caminhos do projeto
# -----------------------
PROJ = Path(__file__).resolve().parent
DATA = PROJ / "data"
PROC = DATA / "processed"
INTER_DIR = PROC / "intersection"
DB_PATH = INTER_DIR / "intersections.duckdb"              # criado por scripts/06_build_duckdb.py
PARQUET_PATH = INTER_DIR / "inter_prodes_rings.parquet"   # criado por scripts/04_intersection.py ou 05_precompute_intersections.py
RINGS_PATH = PROC / "buffers" / "buffer_rings.shp"
AOI_PATH = PROC / "roraima_aoi.geojson"
WGS84 = "EPSG:4326"

st.set_page_config(page_title="Roraima — Desmatamento (PRODES) vs. Proximidade de Estradas", layout="wide")

# ---------- Municípios (IBGE) ----------
MUN_PATH = DATA / "external" / "ibge_municipal" / "RR_Municipios_2024.shp"

@st.cache_data(show_spinner=False)
def load_municipios(path: Path) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        gdf = gdf.set_crs(WGS84)
    gdf = gdf.to_crs(WGS84)
    # tenta achar a coluna do nome
    candidates = ["NM_MUN", "NM_MUNICIP", "NM_MUNICIPIO", "NOME_MUN", "NM_MUN_2024", "name"]
    name_col = next((c for c in candidates if c in gdf.columns), None)
    if name_col is None:
        # cria um nome genérico se não achar
        gdf["MUN_NAME"] = gdf.index.astype(str)
        name_col = "MUN_NAME"
    gdf = gdf.rename(columns={name_col: "MUN_NAME"})
    gdf["MUN_NAME"] = gdf["MUN_NAME"].astype(str)
    return gdf[["MUN_NAME", "geometry"]]

# ... depois de rings = load_gdf(...), aoi = load_gdf(...)
municipios = None
if MUN_PATH.exists():
    municipios = load_municipios(MUN_PATH)
else:
    st.warning(f"Shapefile de municípios não encontrado: {MUN_PATH}")

# -----------------------
# Helpers
# -----------------------

def duck_query(sql: str, params=None) -> pd.DataFrame:
    con = duckdb.connect(DB_PATH.as_posix(), read_only=True)
    try:
        df = con.execute(sql, params or {}).fetchdf()
    finally:
        con.close()
    return df

@st.cache_data(show_spinner=False)
def load_gdf(path: Path) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        gdf = gdf.set_crs(WGS84)
    return gdf

@st.cache_data(show_spinner=False)
def load_intersection_parquet(path: Path) -> gpd.GeoDataFrame:
    return gpd.read_parquet(path)  # GeoParquet mantém geometria/CRS

def sanitize_for_folium(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    df = gdf.copy()
    for col in df.columns:
        if col == "geometry":
            continue
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d")
        if col.lower() == "year":
            try:
                df[col] = df[col].astype(float).round().astype(int)
            except Exception:
                pass
    return df

# -----------------------
# Cargas fixas
# -----------------------
if not DB_PATH.exists():
    st.error(f"Banco DuckDB não encontrado: {DB_PATH}\nRode: python scripts/06_build_duckdb.py")
    st.stop()
if not PARQUET_PATH.exists():
    st.error(f"Parquet não encontrado: {PARQUET_PATH}\nRode: python scripts/04_intersection.py (ou 05_precompute_intersections.py)")
    st.stop()

rings = load_gdf(RINGS_PATH).to_crs(WGS84)
aoi = load_gdf(AOI_PATH).to_crs(WGS84)

# anos disponíveis via DuckDB
yrs = duck_query("SELECT MIN(year) AS y0, MAX(year) AS y1 FROM by_ring_year;")
y0, y1 = int(yrs["y0"].iloc[0]), int(yrs["y1"].iloc[0])

rings_all = rings["ring_id"].astype(str).sort_values().tolist()
default_years = (max(y1 - 2, y0), y1)
default_rings = [r for r in rings_all if r.startswith("0-5")] or rings_all[:1]

# -----------------------
# Sidebar com FORM (evita rerun a cada mudança)
# -----------------------
with st.sidebar:
    st.subheader("Filtros")
    
    mun_sel = []
    if municipios is not None:
        mun_opts = sorted(municipios["MUN_NAME"].unique().tolist())
        mun_sel = st.multiselect("Municípios (opcional)", mun_opts, default=[])


    if "applied_filters" not in st.session_state:
        st.session_state.applied_filters = {
            "years": default_years,
            "rings": default_rings,
             "municipios": mun_sel if mun_sel else [],
        }

    with st.form("filters_form", clear_on_submit=False):
        years = st.slider("Anos (intervalo)", y0, y1, value=st.session_state.applied_filters["years"])
        rings_sel = st.multiselect("Faixas (anéis)", rings_all, default=st.session_state.applied_filters["rings"])
        submitted = st.form_submit_button("Aplicar filtros", use_container_width=True, type="primary")

    if submitted:
        st.session_state.applied_filters = {
            "years": (int(years[0]), int(years[1])),
            "rings": rings_sel if rings_sel else rings_all,
             "municipios": mun_sel if mun_sel else [],
        }
    


# filtros aplicados
ymin, ymax = st.session_state.applied_filters["years"]
rings_sel = st.session_state.applied_filters["rings"]
mun_sel = st.session_state.applied_filters.get("municipios", [])

# ==========================
# Relatório (PDF) — gerar e baixar/abrir
# ==========================
st.markdown("### Relatório (PDF)")

import base64
try:
    import doc  # importa seu doc.py

    # pega filtros atuais direto da session_state
    ymin, ymax = st.session_state.applied_filters["years"]
    mun_sel = st.session_state.applied_filters.get("municipios", [])

    if st.button("Gerar PDF com filtros atuais", type="primary"):
        with st.spinner("Gerando relatório em PDF..."):
            pdf_bytes = doc.build_pdf_bytes(
                years=(ymin, ymax),
                municipios_filtro=mun_sel if mun_sel else None
            )
        st.success("Relatório gerado!")

        # Botão para baixar
        st.download_button(
            "Baixar PDF",
            data=pdf_bytes,
            file_name="relatorio_roraima.pdf",
            mime="application/pdf",
            use_container_width=True
        )

        # Link para abrir em nova guia
        b64 = base64.b64encode(pdf_bytes).decode("utf-8")
        st.markdown(
            f"<a href='data:application/pdf;base64,{b64}' target='_blank' "
            f"style='text-decoration:none; padding:8px 12px; background:#2563eb; "
            f"color:#fff; border-radius:8px; display:inline-block; margin-top:8px;'>"
            f"Abrir em nova guia</a>",
            unsafe_allow_html=True
        )
except Exception as e:
    st.warning(f"Não foi possível gerar o PDF a partir do app: {e}")



# -----------------------
# Consultas rápidas no DuckDB (agregados)
# -----------------------
by_ring_year = duck_query("""
    SELECT ring_id, year, area_ha
    FROM by_ring_year
    WHERE year BETWEEN ? AND ?
      AND ring_id IN (SELECT * FROM UNNEST(?))
    ORDER BY year, ring_id;
""", [ymin, ymax, rings_sel])

by_ring = duck_query("""
    SELECT ring_id, SUM(area_ha) AS area_ha
    FROM by_ring_year
    WHERE year BETWEEN ? AND ?
      AND ring_id IN (SELECT * FROM UNNEST(?))
    GROUP BY 1
    ORDER BY ring_id;
""", [ymin, ymax, rings_sel])

# -----------------------
# Geometrias (SEM AMOSTRAGEM) para o mapa
# -----------------------
inter_all = load_intersection_parquet(PARQUET_PATH)
year_col = next(c for c in inter_all.columns if c.lower() == "year")
inter_f = inter_all[
    inter_all[year_col].between(ymin, ymax)
    & inter_all["ring_id"].astype(str).isin(rings_sel)
].copy()

# mantém colunas essenciais e projeta
keep_cols = [c for c in ["ring_id", "year", "area_ha", "geometry"] if c in inter_f.columns]
inter_map = inter_f[keep_cols].to_crs(WGS84) if not inter_f.empty else gpd.GeoDataFrame(geometry=[], crs=WGS84)
inter_safe = sanitize_for_folium(inter_map)

# ---------- Filtro espacial por município (se selecionado) ----------
using_muni_filter = municipios is not None and len(mun_sel) > 0

# vamos precisar dessas variáveis mais tarde
inter_muni = gpd.GeoDataFrame(geometry=[], crs=WGS84)

if using_muni_filter and not inter_map.empty:
    # pega apenas os municípios escolhidos e leva o nome junto
    mun_gdf = municipios[municipios["MUN_NAME"].isin(mun_sel)][["MUN_NAME", "geometry"]].copy()

    # sjoin (intersects) – rápido com índice espacial
    inter_muni = gpd.sjoin(
        inter_map,                      # geometria já em WGS84
        mun_gdf,
        how="inner",
        predicate="intersects"
    ).drop(columns=["index_right"])

    # Se não houver nada, evita quebrar
    if inter_muni.empty:
        st.info("Nenhuma feição da interseção cai nos municípios selecionados.")
        inter_safe_filtered = inter_muni
        # zera agregados
        by_ring = pd.DataFrame({"ring_id": [], "area_ha": []})
        by_ring_year = pd.DataFrame({"ring_id": [], "year": [], "area_ha": []})
    else:
        # recalcula agregados para a seleção
        by_ring = (
            inter_muni.groupby("ring_id", as_index=False)["area_ha"].sum()
            .sort_values("ring_id")
        )
        by_ring_year = (
            inter_muni.groupby(["ring_id", "year"], as_index=False)["area_ha"].sum()
            .sort_values(["year","ring_id"])
        )

        # geometria para o mapa (só selecionados)
        inter_safe_filtered = inter_muni.copy()
else:
    # mantém o que já tínhamos (sem filtro por município)
    inter_safe_filtered = inter_map.copy()

# Atualiza o inter_safe usado no mapa
inter_safe = sanitize_for_folium(inter_safe_filtered.to_crs(WGS84)) if not inter_safe_filtered.empty else inter_safe_filtered


# --- indicadores (3 cards lado a lado) ---
st.markdown("""
<style>
.cards-row{
  display:grid;
  grid-template-columns: repeat(3, minmax(220px, 1fr));
  gap:14px; margin: 6px 0 18px 0;
}
.card{padding:12px 16px; border-radius:12px; color:#fff;}
.card .t{font-size:.9rem; opacity:.9; margin:0 0 6px 0;}
.card .v{font-size:1.6rem; font-weight:800; margin:0;}
.bg-blue {background: linear-gradient(135deg,#2563eb,#1d4ed8);}
.bg-green{background: linear-gradient(135deg,#059669,#047857);}
.bg-purple{background: linear-gradient(135deg,#7c3aed,#6d28d9);}
</style>
""", unsafe_allow_html=True)

n_feats = int(len(inter_safe))
total_ha = float(by_ring["area_ha"].sum()) if not by_ring.empty else 0.0

st.markdown(
    f"""
<div class="cards-row">
  <div class="card bg-blue">
    <div class="t">Feições (no mapa)</div>
    <div class="v">{fmt_int_br(n_feats)}</div>
  </div>
  <div class="card bg-green">
    <div class="t">Área total (ha)</div>
    <div class="v">{fmt_int_br(total_ha)}</div>
  </div>
  <div class="card bg-purple">
    <div class="t">Anos</div>
    <div class="v">{ymin}–{ymax}</div>
  </div>
</div>
""",
    unsafe_allow_html=True,
)

# -----------------------
# Tabelas com rótulos amigáveis
# -----------------------
label_map = {"ring_id": "Faixa de distância", "area_ha": "Área (ha)", "year": "Ano"}
by_ring_disp = by_ring.rename(columns=label_map)
by_ring_year_disp = by_ring_year.rename(columns=label_map)

if "Faixa de distância" in by_ring_disp.columns:
    by_ring_disp["Faixa de distância"] = pd.Categorical(by_ring_disp["Faixa de distância"], ORDER_RINGS, ordered=True)
    by_ring_disp = by_ring_disp.sort_values("Faixa de distância")
    by_ring_disp["Área (ha)"] = by_ring_disp["Área (ha)"].apply(fmt_float2_br)

if {"Faixa de distância","Ano"}.issubset(by_ring_year_disp.columns):
    by_ring_year_disp["Faixa de distância"] = pd.Categorical(by_ring_year_disp["Faixa de distância"], ORDER_RINGS, ordered=True)
    by_ring_year_disp = by_ring_year_disp.sort_values(["Ano","Faixa de distância"])
    by_ring_year_disp["Área (ha)"] = by_ring_year_disp["Área (ha)"].apply(fmt_float2_br)

st.markdown("### Área por faixa (ha)")
if by_ring_disp.empty:
    st.warning("Sem dados para os filtros escolhidos.")
else:
    st.dataframe(by_ring_disp, use_container_width=True)

st.markdown("### Série por faixa e ano (ha)")
if by_ring_year_disp.empty:
    st.info("Sem dados na série temporal para os filtros.")
else:
    st.dataframe(by_ring_year_disp, use_container_width=True)

# -----------------------
# Gráficos (Altair)
# -----------------------
import altair as alt

# paleta fixa para o Altair
# paleta fixa para as faixas
domain = ORDER_RINGS
range_  = [RING_COLORS[r] for r in domain]

# tema um pouco maior
alt.data_transformers.disable_max_rows()
base_cfg = alt.themes.get()
alt.themes.enable('none')
def _cfg(c):
    return (
        c.configure_axis(labelFontSize=12, titleFontSize=13, gridColor="#2a2a2a")
         .configure_legend(labelFontSize=12, titleFontSize=13)
         .configure_view(strokeOpacity=0)
    )
# ---------------- Área por faixa (barras) ----------------
st.markdown("### Área por faixa (gráfico)")
if not by_ring.empty:
    _ring_plot = by_ring.rename(columns={"ring_id":"Faixa de distância","area_ha":"Área (ha)"}).copy()
    _ring_plot["Faixa de distância"] = pd.Categorical(_ring_plot["Faixa de distância"], ORDER_RINGS, ordered=True)
    chart_bar = (
        alt.Chart(_ring_plot, height=320)
        .mark_bar()
        .encode(
            x=alt.X("Faixa de distância:N", sort=ORDER_RINGS, title="Faixa de distância das estradas"),
            y=alt.Y("Área (ha):Q", title="Área (ha)"),
            color=alt.Color("Faixa de distância:N", scale=alt.Scale(domain=domain, range=range_), legend=None),
            tooltip=[alt.Tooltip("Faixa de distância:N", title="Faixa"),
                     alt.Tooltip("Área (ha):Q", title="Área (ha)", format=",.2f")]
        )
    )
    st.altair_chart(_cfg(chart_bar), use_container_width=True)
else:
    st.warning("Sem dados para o gráfico de faixas.")

# ---------------- Série total por ano ----------------
st.markdown("### Série temporal — total por ano (todas as faixas)")
if not by_ring_year.empty:
    series_total = (
        by_ring_year.groupby("year", as_index=False)["area_ha"].sum()
                    .rename(columns={"year":"Ano","area_ha":"Área (ha)"})
    )
    chart_line_total = (
        alt.Chart(series_total, height=380)
        .mark_line(point=True)
        .encode(
            x=alt.X("Ano:O", title="Ano"),
            y=alt.Y("Área (ha):Q", title="Área (ha)"),
            tooltip=[alt.Tooltip("Ano:O", title="Ano"),
                     alt.Tooltip("Área (ha):Q", title="Área (ha)", format=",.2f")]
        )
    )
    st.altair_chart(_cfg(chart_line_total), use_container_width=True)
else:
    st.info("Sem dados para a série temporal total.")

# ---------------- Facetas por faixa (2x2, grandes) ----------------
st.markdown("### Série temporal por faixa (facetas)")
if not by_ring_year.empty:
    _byry = by_ring_year.rename(columns={"ring_id":"Faixa de distância","year":"Ano","area_ha":"Área (ha)"}).copy()
    _byry["Faixa de distância"] = pd.Categorical(_byry["Faixa de distância"], ORDER_RINGS, ordered=True)
    facet_chart = (
        alt.Chart(_byry)
        .mark_line(point=True)
        .encode(
            x=alt.X("Ano:O", title="Ano"),
            y=alt.Y("Área (ha):Q", title="Área (ha)"),
            color=alt.Color("Faixa de distância:N", scale=alt.Scale(domain=domain, range=range_), legend=None),
            tooltip=[alt.Tooltip("Faixa de distância:N", title="Faixa"),
                     alt.Tooltip("Ano:O", title="Ano"),
                     alt.Tooltip("Área (ha):Q", title="Área (ha)", format=",.2f")]
        )
        .properties(width=520, height=280)   # cada painel GRANDE
        .facet(facet=alt.Facet("Faixa de distância:N", sort=ORDER_RINGS, title=None), columns=2)
        .resolve_scale(y='independent')      # cada faixa com seu eixo Y
    )
    st.altair_chart(_cfg(facet_chart), use_container_width=True)
else:
    st.info("Sem dados para as facetas por faixa.")


# -----------------------
# Mapa
# -----------------------
# -----------------------
# Mapa (robusto, com basemap único ativo e vetores leves)
# -----------------------
st.markdown("### Mapa interativo")
b = aoi.total_bounds
center = [(b[1] + b[3]) / 2, (b[0] + b[2]) / 2]

# limite seguro para ligar a camada pesada automaticamente
MAX_FEATURES_AUTO_SHOW = 6000
force_full = st.toggle("Forçar renderização completa da interseção no mapa (pode ficar pesado)", value=False)

# Folium/Leaflet otimizações
m = folium.Map(
    location=center,
    zoom_start=6,
    control_scale=True,
    tiles=None,              # vamos controlar quais tiles ficam ativos
    prefer_canvas=True       # desenha vetores em canvas, MUITO mais leve
)

# ---- Basemaps (apenas UM ativo por padrão) ----
folium.TileLayer("OpenStreetMap", name="OpenStreetMap", show=True).add_to(m)
folium.TileLayer("CartoDB positron", name="cartodbpositron", show=False).add_to(m)
folium.TileLayer("CartoDB Voyager", name="CartoDB Voyager", show=False).add_to(m)
folium.TileLayer(
    tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{z}/{x}/{y}",
    attr="Tiles © Esri — Source: Esri, HERE, Garmin, FAO, NOAA, USGS | © OpenStreetMap contributors",
    name="Esri Streets",
    show=False
).add_to(m)
folium.TileLayer(
    tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{x}/{y}",
    attr="Tiles © Esri — World Imagery",
    name="Esri Imagery (satélite)",
    show=False
).add_to(m)

# ---- AOI ----
folium.GeoJson(
    aoi.__geo_interface__, name="Roraima",
    style_function=lambda x: {"color": "#222", "fill": False, "weight": 2},
    smooth_factor=1.2
).add_to(m)

# ---- Destaque dos municípios selecionados ----
if using_muni_filter:
    folium.GeoJson(
        municipios[municipios["MUN_NAME"].isin(mun_sel)].__geo_interface__,
        name="Município(s) selecionado(s)",
        style_function=lambda x: {"color":"#ff9800", "weight": 2, "fill": False, "dashArray": "6,4"},
        tooltip=folium.GeoJsonTooltip(fields=["MUN_NAME"], aliases=["Município"])
    ).add_to(m)

# ---- ANÉIS com cores fixas ----
rings_plot = rings[rings["ring_id"].astype(str).isin(rings_sel)].copy()
def _ring_style(feat):
    rid = feat["properties"].get("ring_id")
    color = RING_COLORS.get(rid, "#2b8cbe")
    return {"color": color, "fillColor": color, "fillOpacity": 0.20, "weight": 1}

folium.GeoJson(
    rings_plot.__geo_interface__, name="Faixas (anéis)",
    style_function=_ring_style,
    tooltip=folium.GeoJsonTooltip(fields=["ring_id"], aliases=["Faixa"]),
    smooth_factor=1.2
).add_to(m)

# ---- RODOVIAS (OSM) — BR/ref e name no tooltip ----
try:
    roads_path = PROC / "roads_rr.shp"
    roads_wgs = gpd.read_file(roads_path).to_crs(WGS84)
    # filtra classes principais para não sobrecarregar
    if "fclass" in roads_wgs.columns:
        roads_wgs = roads_wgs[roads_wgs["fclass"].isin(["motorway", "trunk", "primary", "secondary"])].copy()
    # simplifica SÓ para exibir
    roads_wgs["geometry"] = roads_wgs.geometry.simplify(0.00010, preserve_topology=True)

    fields, aliases = [], []
    for field, alias in [("ref", "BR/Ref"), ("name", "Nome")]:
        if field in roads_wgs.columns:
            fields.append(field); aliases.append(alias)

    folium.GeoJson(
        data=roads_wgs.__geo_interface__,
        name="Rodovias (OSM)",
        style_function=lambda x: {"color": "#444", "weight": 1.5, "opacity": 0.9},
        tooltip=folium.GeoJsonTooltip(fields=fields, aliases=aliases, sticky=False),
        smooth_factor=1.2
    ).add_to(m)
except Exception as e:
    st.warning(f"Não foi possível carregar 'roads_rr.shp' para rótulos de BRs: {e}")

# ---- Interseção PRODES × anéis (camada pesada) ----
if not inter_safe.empty:
    inter_draw = inter_safe.copy()
    # simplifica só para o mapa; ajusta o valor se quiser mais/menos detalhe
    inter_draw["geometry"] = inter_draw.geometry.simplify(0.00020, preserve_topology=True)
    if "area_ha" in inter_draw.columns:
        inter_draw["area_ha_fmt"] = inter_draw["area_ha"].apply(fmt_float2_br)

    fields = []; aliases = []
    if "ring_id" in inter_draw.columns: fields.append("ring_id"); aliases.append("Faixa")
    if "year" in inter_draw.columns:    fields.append("year");    aliases.append("Ano")
    if "area_ha_fmt" in inter_draw.columns: fields.append("area_ha_fmt"); aliases.append("Área (ha)")

    # liga automaticamente só se for “leve”; caso contrário, deixa desligado (show=False)
    auto_show = (len(inter_draw) <= MAX_FEATURES_AUTO_SHOW) or force_full
    if not auto_show:
        st.info(f"Muitos polígonos para o navegador ({len(inter_draw):,}): deixei a camada **desligada** por padrão. "
                f"Se quiser ver, marque **Forçar renderização completa** acima."
               .replace(",", "."))

    folium.GeoJson(
        data=inter_draw.to_json(),
        name=f"Interseção PRODES × anéis (n={len(inter_draw)})",
        style_function=lambda x: {"color": "#e31a1c", "fillColor": "#fb9a99", "fillOpacity": 0.35, "weight": 0.5},
        tooltip=folium.GeoJsonTooltip(fields=fields, aliases=aliases),
        smooth_factor=1.0,
        show=auto_show
    ).add_to(m)
else:
    folium.map.Marker(location=center, tooltip="Sem polígonos para os filtros.").add_to(m)

folium.LayerControl(collapsed=False).add_to(m)
st_folium(m, height=650, use_container_width=True)

# ==========================
# Detalhe por município
# ==========================
if using_muni_filter and not inter_muni.empty:
    st.markdown("## Detalhe por município")

    # --- agregados ---
    muni_total = (
        inter_muni.groupby("MUN_NAME", as_index=False)["area_ha"].sum()
        .sort_values("area_ha", ascending=False)
        .rename(columns={"MUN_NAME": "Município", "area_ha": "Área (ha)"})
    )
    muni_ring = (
        inter_muni.groupby(["MUN_NAME", "ring_id"], as_index=False)["area_ha"].sum()
        .rename(columns={"MUN_NAME": "Município", "ring_id": "Faixa", "area_ha": "Área (ha)"})
    )
    muni_year_ring = (
        inter_muni.groupby(["MUN_NAME", "year", "ring_id"], as_index=False)["area_ha"].sum()
        .rename(columns={"MUN_NAME": "Município", "year": "Ano", "ring_id": "Faixa", "area_ha": "Área (ha)"})
    )

    # formata números na tabela total
    muni_total_fmt = muni_total.copy()
    muni_total_fmt["Área (ha)"] = muni_total_fmt["Área (ha)"].apply(fmt_float2_br)

    c1, c2 = st.columns([1, 2])
    with c1:
        st.markdown("**Área total por município (ha)**")
        st.dataframe(muni_total_fmt, use_container_width=True, height=300)

        # downloads
        st.download_button(
            "Baixar CSV — total por município",
            muni_total.to_csv(index=False).encode("utf-8"),
            file_name="area_total_por_municipio.csv",
            mime="text/csv",
            use_container_width=True
        )
        st.download_button(
            "Baixar CSV — município × faixa",
            muni_ring.to_csv(index=False).encode("utf-8"),
            file_name="area_por_municipio_faixa.csv",
            mime="text/csv",
            use_container_width=True
        )
        st.download_button(
            "Baixar CSV — município × ano × faixa",
            muni_year_ring.to_csv(index=False).encode("utf-8"),
            file_name="area_por_municipio_ano_faixa.csv",
            mime="text/csv",
            use_container_width=True
        )

    # paleta por faixa
    domain = ORDER_RINGS
    range_  = [RING_COLORS[r] for r in domain]

    with c2:
        st.markdown("**Barras por município × faixa (empilhado)**")
        _bar = muni_ring.copy()
        _bar["Faixa"] = pd.Categorical(_bar["Faixa"], ORDER_RINGS, ordered=True)
        chart_muni_bar = (
            alt.Chart(_bar, height=320)
            .mark_bar()
            .encode(
                x=alt.X("Município:N", sort=muni_total["Município"].tolist()),
                y=alt.Y("Área (ha):Q"),
                color=alt.Color("Faixa:N", scale=alt.Scale(domain=domain, range=range_), title="Faixa"),
                tooltip=[
                    alt.Tooltip("Município:N"),
                    alt.Tooltip("Faixa:N"),
                    alt.Tooltip("Área (ha):Q", format=",.2f")
                ]
            )
        )
        st.altair_chart(
            chart_muni_bar.configure_axis(labelFontSize=12, titleFontSize=13, gridColor="#2a2a2a")
                          .configure_legend(labelFontSize=12, titleFontSize=13)
                          .configure_view(strokeOpacity=0),
            use_container_width=True
        )

    st.markdown("**Série temporal por município (facetas)**")
    _ts = muni_year_ring.copy()
    _ts["Faixa"] = pd.Categorical(_ts["Faixa"], ORDER_RINGS, ordered=True)

    ncols = 2 if len(mun_sel) > 1 else 1
    chart_ts = (
        alt.Chart(_ts)
        .mark_line(point=True)
        .encode(
            x=alt.X("Ano:O"),
            y=alt.Y("Área (ha):Q"),
            color=alt.Color("Faixa:N", scale=alt.Scale(domain=domain, range=range_), title="Faixa"),
            tooltip=[alt.Tooltip("Município:N"), alt.Tooltip("Ano:O"), alt.Tooltip("Faixa:N"),
                     alt.Tooltip("Área (ha):Q", format=",.2f")]
        )
        .properties(width=520 if ncols==2 else 900, height=260)
        .facet(facet="Município:N", columns=ncols)
        .resolve_scale(y="independent")
    )
    st.altair_chart(
        chart_ts.configure_axis(labelFontSize=12, titleFontSize=13, gridColor="#2a2a2a")
                .configure_legend(labelFontSize=12, titleFontSize=13)
                .configure_view(strokeOpacity=0),
        use_container_width=True
    )



st.caption("Fonte: OSM (estradas), IBGE (UF), INPE/PRODES. Agregados: DuckDB. Geometrias: GeoParquet. (Sem amostragem.)")
