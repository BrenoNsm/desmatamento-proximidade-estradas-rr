"""Research API and local web application (no Streamlit)."""
from pathlib import Path
from functools import lru_cache
import json
import geopandas as gpd
import pandas as pd
import numpy as np
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, Response
from spatial_stats import moran, association

ROOT=Path(__file__).resolve().parent
app=FastAPI(title="GeoForest Lab",version="3.0.0-experimental")

@lru_cache(maxsize=4)
def load(resolution,stamp):
    p=ROOT/"data/v3"/str(resolution)
    return gpd.read_parquet(p/"grid.parquet"),pd.read_parquet(p/"annual.parquet"),json.loads((p/"manifest.json").read_text())

def dataset(resolution):
    p=ROOT/"data/v3"/str(resolution)/"manifest.json"
    if not p.exists():
        raise HTTPException(404,f"Execute python research_grid.py --cell-km {resolution}")
    return load(resolution,p.stat().st_mtime_ns)

@app.get("/")
def index():
    return FileResponse(ROOT/"web_v3.html")

@app.get("/api/meta")
def meta(resolution:int=5):
    g,a,m=dataset(resolution)
    return {"years":m["years"],"cells":len(g),"manifest":m,
            "resolutions":[r for r in (2,5,10,20) if (ROOT/"data/v3"/str(r)/"manifest.json").exists()]}

def filtered(resolution,start,end):
    g,a,m=dataset(resolution)
    if start>end or start<min(m["years"]) or end>max(m["years"]):
        raise HTTPException(422,"Período fora da série disponível")
    selected=a[a.year.between(start,end)]
    if selected.empty:
        raise HTTPException(422,"Sem anos observados no período")
    g=g.copy()
    g["area_ha"]=g.cell_id.map(selected.groupby("cell_id").area_ha.sum()).fillna(0)
    g["intensity"]=100*g.area_ha/g.territory_ha
    return g,selected,m

@app.get("/api/analysis")
def analysis(resolution:int=5,start:int=2008,end:int=2024):
    g,a,m=filtered(resolution,start,end)
    g["band"]=pd.cut(g.distance_km,[-np.inf,1,2,5,10,20,50,np.inf],labels=["0–1","1–2","2–5","5–10","10–20","20–50",">50"])
    bands=g.groupby("band",observed=False).agg(area_ha=("area_ha","sum"),territory_ha=("territory_ha","sum"),cells=("cell_id","size")).reset_index()
    bands["intensity"]=100*bands.area_ha/bands.territory_ha.replace(0,np.nan)
    annual=a.groupby("year").area_ha.sum().reset_index()
    annual["intensity"]=100*annual.area_ha/g.territory_ha.sum()
    heat=a.merge(g[["cell_id","band","territory_ha"]],on="cell_id").groupby(["year","band"],observed=True).agg(area_ha=("area_ha","sum"),territory_ha=("territory_ha","sum")).reset_index()
    heat["intensity"]=100*heat.area_ha/heat.territory_ha
    return {"summary":{"area_ha":float(g.area_ha.sum()),"territory_ha":float(g.territory_ha.sum()),"intensity":float(100*g.area_ha.sum()/g.territory_ha.sum()),"cells":len(g),"affected":int((g.area_ha>0).sum())},
            "annual":json.loads(annual.to_json(orient="records")),"bands":json.loads(bands.to_json(orient="records")),
            "heat":json.loads(heat.to_json(orient="records")),"association":association(g.distance_km,g.intensity)}

@app.get("/api/grid")
def grid(resolution:int=5,start:int=2008,end:int=2024):
    g,a,m=filtered(resolution,start,end)
    return Response(g.to_json(drop_id=True),media_type="application/geo+json")

@app.get("/api/moran")
def spatial(resolution:int=5,start:int=2008,end:int=2024,permutations:int=Query(199,ge=99,le=999)):
    g,a,m=filtered(resolution,start,end)
    result=moran(g.intensity,g.i,g.j,permutations=permutations)
    for row in result["local"]:
        row["cell_id"]=g.iloc[row.pop("index")].cell_id
    return result

@app.get("/api/export")
def export(resolution:int=5,start:int=2008,end:int=2024):
    g,a,m=filtered(resolution,start,end)
    data=a.merge(g.drop(columns=["geometry","area_ha","intensity"]),on="cell_id")
    data["intensity"]=100*data.area_ha/data.territory_ha
    return Response(data.to_csv(index=False),media_type="text/csv",headers={"Content-Disposition":"attachment; filename=research.csv"})
