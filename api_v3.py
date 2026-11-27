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

def environmental_sample(resolution,start,end):
    import hashlib
    g,a,m=filtered(resolution,start,end)
    folder=ROOT/"data/v3"/str(resolution)
    path=folder/"environment.parquet"
    if not path.exists():
        raise HTTPException(404,f"Execute python -u environment_inpe_v3.py --cell-km {resolution}")
    with (folder/"grid.parquet").open("rb") as f:
        digest=hashlib.file_digest(f,"sha256").hexdigest()
    manifest=json.loads((folder/"environment_manifest.json").read_text())
    if manifest["grid_sha256"]!=digest:
        raise HTTPException(409,"Grade alterada; reprocesse a cobertura")
    if "increment_sha256" in manifest:
        with (ROOT/"data/v2/fragments.parquet").open("rb") as f:
            if hashlib.file_digest(f,"sha256").hexdigest()!=manifest["increment_sha256"]:
                raise HTTPException(409,"Incrementos alterados; reprocesse a cobertura")
    e=pd.read_parquet(path)
    d=a.merge(g.drop(columns=["geometry","area_ha","intensity"]),on="cell_id").merge(e,on=["cell_id","year"],how="left",validate="one_to_one")
    if d.cover_year.isna().any():
        missing=sorted(map(int,d.loc[d.cover_year.isna(),"year"].unique()))
        raise HTTPException(422,f"Cobertura ausente para anos PRODES: {missing}")
    return d

@app.get("/api/environment")
def environment(resolution:int=5,start:int=2008,end:int=2024):
    d=environmental_sample(resolution,start,end)
    good=(d.coverage_fraction>=.95)&(d.forest_ha>=1)
    v=d[good].copy()
    v["band"]=pd.cut(v.distance_km,[-np.inf,1,2,5,10,20,50,np.inf],labels=["0–1","1–2","2–5","5–10","10–20","20–50",">50"])
    annual=v.groupby("year").agg(area_ha=("area_ha","sum"),forest_ha=("forest_ha","sum")).reset_index()
    annual["ratio_pct"]=100*annual.area_ha/annual.forest_ha
    bands=v.groupby("band",observed=True).agg(area_ha=("area_ha","sum"),forest_ha=("forest_ha","sum")).reset_index()
    bands["ratio_pct"]=100*bands.area_ha/bands.forest_ha
    return {"annual":json.loads(annual.to_json(orient="records")),"bands":json.loads(bands.to_json(orient="records")),
            "n":int(good.sum()),"excluded":int((~good).sum()),"excluded_area_ha":float(d.loc[~good,"area_ha"].sum()),
            "provider":str(d.provider.iloc[0]) if "provider" in d else "MapBiomas",
            "note":"Razão de incremento PRODES por proxy de disponibilidade florestal anterior. Consulte o manifesto ambiental para identificar a fonte e as limitações. Faixas agregadas por hectare-ano."}

@app.get("/api/model")
def model(resolution:int=5,start:int=2008,end:int=2024):
    from model_v3 import fit_model
    try:
        return fit_model(environmental_sample(resolution,start,end),resolution)
    except (ValueError, ArithmeticError) as e:
        raise HTTPException(422,str(e)) from e

@app.get("/api/environment/export")
def environmental_export(resolution:int=5,start:int=2008,end:int=2024):
    d=environmental_sample(resolution,start,end)
    return Response(d.to_csv(index=False),media_type="text/csv",headers={"Content-Disposition":"attachment; filename=environment.csv"})

def territorial_data(resolution):
    from download_prodes_v2 import sha
    folder=ROOT/"data/v3"/str(resolution)
    target=folder/"territory"
    manifest_path=target/"manifest.json"
    if not manifest_path.exists():
        raise HTTPException(404,f"Execute python -u territory_v3.py --cell-km {resolution}")
    manifest=json.loads(manifest_path.read_text())
    if manifest["grid_sha256"]!=sha(folder/"grid.parquet") or manifest["increment_sha256"]!=sha(ROOT/"data/v2/fragments.parquet"):
        raise HTTPException(409,"Grade ou incrementos alterados; reprocesse o território")
    return target,manifest

@app.get("/api/territory")
def territory(resolution:int=5,start:int=2008,end:int=2024):
    from territory_v3 import LABELS
    g,a,m=filtered(resolution,start,end)
    target,manifest=territorial_data(resolution)
    cov=pd.read_parquet(target/"covariates.parquet")
    data=pd.read_parquet(target/"annual.parquet")
    data=data[data.year.between(start,end)]
    annual=data.groupby(["year","group"],as_index=False).agg(area_ha=("area_ha","sum"),territory_ha=("territory_ha","sum"))
    annual["intensity"]=100*annual.area_ha/annual.territory_ha
    summary=annual.groupby("group",as_index=False).agg(area_ha=("area_ha","sum"),territory_ha=("territory_ha","first"))
    summary["intensity"]=100*summary.area_ha/summary.territory_ha
    for frame in (annual,summary):frame["label"]=frame.group.map(LABELS)
    distance=data.merge(g[["cell_id","distance_km"]],on="cell_id",validate="many_to_one")
    distance["band"]=pd.cut(distance.distance_km,[-np.inf,1,2,5,10,20,50,np.inf],labels=["0–1","1–2","2–5","5–10","10–20","20–50",">50"])
    distance=distance.groupby(["group","band"],observed=True,as_index=False).agg(area_ha=("area_ha","sum"),territory_ha_year=("territory_ha","sum"))
    distance["intensity"]=100*distance.area_ha/distance.territory_ha_year
    distance["label"]=distance.group.map(LABELS)
    return {"summary":json.loads(summary.to_json(orient="records")),"annual":json.loads(annual.to_json(orient="records")),
        "distance":json.loads(distance.to_json(orient="records")),
        "totals":{k:float(cov[k].sum()) for k in ("indigenous_ha","conservation_ha","protected_ha","overlap_ha","residual_ha")},
        "manifest":manifest}

@app.get("/api/territory/layer/{role}")
def territorial_layer(role:str,resolution:int=5):
    if role not in ("indigenous","conservation","residual"):raise HTTPException(404,"Camada desconhecida")
    target,m=territorial_data(resolution)
    df=gpd.read_parquet(target/(role+".parquet"))
    # Simplification is visual only; all statistics use full geometry.
    df.geometry=df.geometry.simplify(.00015,preserve_topology=True)
    return Response(df.to_json(drop_id=True),media_type="application/geo+json")

@app.get("/api/territory/export")
def territorial_export(resolution:int=5,start:int=2008,end:int=2024):
    g,a,m=filtered(resolution,start,end)
    target,manifest=territorial_data(resolution)
    d=pd.read_parquet(target/"annual.parquet")
    d=d[d.year.between(start,end)].merge(g.drop(columns=["geometry","area_ha","intensity","territory_ha"]),on="cell_id",validate="many_to_one")
    return Response(d.to_csv(index=False),media_type="text/csv",headers={"Content-Disposition":"attachment; filename=territorial.csv"})

@app.get("/api/territory/models")
def territorial_models(resolution:int=5,start:int=2008,end:int=2024):
    from model_v3 import compare_territorial_models
    d=environmental_sample(resolution,start,end)
    target,manifest=territorial_data(resolution)
    d=d.merge(pd.read_parquet(target/"covariates.parquet"),on="cell_id",validate="many_to_one")
    try:return compare_territorial_models(d,resolution)
    except (ValueError,ArithmeticError,np.linalg.LinAlgError) as e:raise HTTPException(422,str(e)) from e
