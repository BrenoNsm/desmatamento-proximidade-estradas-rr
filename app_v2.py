"""Explorador científico: agregados SQL e mapa WebGL sob demanda."""
from pathlib import Path
import json
import duckdb
import geopandas as gpd
import pandas as pd
import pydeck as pdk
import streamlit as st

OUT = Path(__file__).resolve().parent / "data" / "v2"
st.set_page_config(page_title="Desmatamento e estradas | Pesquisa RR", layout="wide")
st.title("Desmatamento e proximidade das estradas")
st.caption("Roraima • Explorador científico • PRODES × OSM × IBGE")
if not (OUT / "manifest.json").exists():
    st.error("Execute primeiro: python pipeline_v2.py --config research_v2.json")
    st.stop()
version = (OUT / "manifest.json").stat().st_mtime_ns

@st.cache_data
def tables(version):
    with duckdb.connect(str(OUT / "research.duckdb"), read_only=True) as con:
        return con.sql("SELECT * FROM annual").df(), con.sql("SELECT * FROM zones").df()

@st.cache_data
def map_data(name, version, municipalities, rings, years):
    path = OUT / f"{name}.parquet"
    filters = []
    if name in ("fragments", "zones", "municipalities"):
        filters.append(("NM_MUN", "in", list(municipalities)))
    if name in ("fragments", "zones"):
        filters.append(("ring_id", "in", list(rings)))
    if name == "fragments":
        filters.extend([("year", ">=", years[0]), ("year", "<=", years[1])])
    df = gpd.read_parquet(path, filters=filters or None)
    # Visual simplification only; never used for statistical calculations.
    df.geometry = df.geometry.simplify(0.00015, preserve_topology=True)
    return json.loads(df.to_json(drop_id=True))

annual, zones = tables(version)
if annual.empty:
    st.warning("Sem registros de desmatamento no conjunto processado.")
    st.stop()
ring_order = ["0-5km","5-10km","10-20km",">20km"]
munis = sorted(zones.NM_MUN.unique())
with st.sidebar:
    st.header("Filtros")
    with st.form("filters"):
        selected = st.multiselect("Municípios", munis, default=munis)
        years = st.slider("Período", int(annual.year.min()), int(annual.year.max()),
                          (int(annual.year.min()), int(annual.year.max()))) if annual.year.nunique()>1 else (int(annual.year.min()),)*2
        rings = st.multiselect("Distância das estradas", ring_order, default=ring_order)
        st.form_submit_button("Aplicar", type="primary")
if not selected or not rings:
    st.info("Selecione pelo menos um município e uma faixa.")
    st.stop()
f = annual[annual.NM_MUN.isin(selected) & annual.ring_id.isin(rings) & annual.year.between(*years)]
z = zones[zones.NM_MUN.isin(selected) & zones.ring_id.isin(rings)]
summary = z.groupby("ring_id", as_index=False).zone_ha.sum().merge(
    f.groupby("ring_id", as_index=False).area_ha.sum(), on="ring_id", how="left").fillna({"area_ha":0})
summary["percentual_territorial"] = 100 * summary.area_ha / summary.zone_ha
a,b,c = st.columns(3)
a.metric("Área desmatada (ha)", f"{f.area_ha.sum():,.2f}")
b.metric("Área das faixas (ha)", f"{z.zone_ha.sum():,.2f}")
c.metric("Municípios selecionados", len(selected))
maptab, stats, methods = st.tabs(["Mapa", "Indicadores", "Metodologia"])
with maptab:
    layers_selected = st.multiselect("Camadas", ["Municípios","Faixas","Desmatamento","Estradas"], default=["Municípios","Faixas"])
    st.caption("Carregue o desmatamento somente quando necessário. A camada de estradas mostra toda a área de processamento.")
    layers = []
    for label,name,color in [("Municípios","municipalities",[0,0,0,0]),("Faixas","zones",[220,160,65,85]),("Desmatamento","fragments",[190,50,40,170]),("Estradas","roads",[70,70,70,255])]:
        if label not in layers_selected:
            continue
        data = map_data(name, version, tuple(selected), tuple(rings), tuple(years))
        layers.append(pdk.Layer("GeoJsonLayer", data, id=name, pickable=True,
            filled=name!="roads", stroked=True, get_fill_color=color,
            get_line_color=[80,90,80,180], line_width_min_pixels=1))
    st.pydeck_chart(pdk.Deck(layers=layers, initial_view_state=pdk.ViewState(latitude=2,longitude=-61,zoom=5.6),
        map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
        tooltip={"text":"{NM_MUN} {ring_id} {year}"}), height=650)
with stats:
    st.subheader("Área por faixa")
    st.bar_chart(summary.set_index("ring_id")["area_ha"])
    st.subheader("Proporção da área territorial da faixa")
    st.bar_chart(summary.set_index("ring_id")["percentual_territorial"])
    st.caption("Proporção acumulada no período; não é taxa anual nem controla disponibilidade histórica de floresta.")
    st.line_chart(f.groupby("year").area_ha.sum())
    st.dataframe(summary, hide_index=True)
    st.download_button("Exportar agregados CSV", f.to_csv(index=False).encode("utf-8"), "agregados.csv", "text/csv")
with methods:
    st.write("A malha viária representa o momento de aquisição; associação espacial não implica causalidade temporal.")
    st.json(json.loads((OUT / "manifest.json").read_text(encoding="utf-8")))
