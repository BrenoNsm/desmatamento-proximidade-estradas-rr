"""Research API and local web application (no Streamlit)."""
from pathlib import Path
from functools import lru_cache
import json
import geopandas as gpd
import pandas as pd
import numpy as np
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
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

FRONTEND = ROOT / "frontend" / "dist"
if (FRONTEND / "assets").is_dir():
    app.mount("/assets", StaticFiles(directory=FRONTEND / "assets"), name="frontend-assets")

@app.get("/")
def index():
    return FileResponse(FRONTEND/"index.html" if (FRONTEND/"index.html").exists() else ROOT/"web_v3.html")

@app.get("/api/meta")
def meta(resolution:int=5):
    g,a,m=dataset(resolution)
    folder=ROOT/"data/v3"/str(resolution)
    env_path=folder/"environment_manifest.json"
    env_years=[]
    if env_path.exists() and (folder/"environment.parquet").exists():
        from download_prodes_v2 import sha
        env_manifest=json.loads(env_path.read_text())
        if env_manifest.get("grid_sha256")==sha(folder/"grid.parquet"):
            env_years=env_manifest.get("years",[])
    return {"years":m["years"],"cells":len(g),"manifest":m,"environment_years":env_years,
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
def model(resolution:int=5,start:int=2008,end:int=2024,exposure:str="forest"):
    from model_v3 import fit_model
    try:
        if exposure=="forest":
            d=environmental_sample(resolution,start,end)
        elif exposure=="territory":
            g,a,m=filtered(resolution,start,end)
            d=a.merge(g.drop(columns=["geometry","area_ha","intensity"]),on="cell_id",validate="many_to_one")
            d["forest_ha"]=d.territory_ha
            d["coverage_fraction"]=1.0
        else:raise HTTPException(422,"Exposição inválida")
        result=fit_model(d,resolution)
        result["exposure"]=exposure
        if exposure=="territory":result["note"]="PPML com offset de área territorial, não florestal; efeitos anuais e erros agrupados em blocos de 50 km. Associação exploratória, não causal."
        return result
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


@lru_cache(maxsize=2)
def road_geojson(stamp, detail):
    path=ROOT/"data/v2/roads.parquet"
    roads=gpd.read_parquet(path).to_crs(4326)
    roads=roads[roads.geom_type.isin(["LineString","MultiLineString"])].copy()
    if "fclass" in roads.columns:
        roads["road_class"]=roads.fclass.fillna("unknown").astype(str)
    else:
        roads["road_class"]="unknown"
    major={"motorway","trunk","primary","secondary","motorway_link","trunk_link","primary_link","secondary_link"}
    roads["major"]=roads.road_class.isin(major)
    if detail=="major" and roads.road_class.ne("unknown").any():
        roads=roads[roads.major].copy()
    cols=["road_class","major","geometry"]
    roads=roads[cols].copy()
    # Simplify only the display copy. The analysis continues to use original roads.
    roads.geometry=roads.geometry.simplify(.0001 if detail=="major" else .000025,preserve_topology=True)
    return roads.to_json(drop_id=True)

@app.get("/api/roads")
def roads(detail:str="all"):
    if detail not in ("major","all"):
        raise HTTPException(422,"Detalhe deve ser major ou all")
    path=ROOT/"data/v2/roads.parquet"
    if not path.exists():
        raise HTTPException(404,"Malha viária processada não encontrada em data/v2/roads.parquet")
    return Response(road_geojson(path.stat().st_mtime_ns,detail),media_type="application/geo+json",headers={"Cache-Control":"no-cache"})

@app.get("/api/proximity")
def proximity(resolution:int=5,start:int=2008,end:int=2024):
    g,a,m=filtered(resolution,start,end)
    import duckdb
    with duckdb.connect(str(ROOT/"data/v2/research.duckdb"),read_only=True) as con:
        zones=con.execute("SELECT ring_id,SUM(zone_ha) AS territory_ha FROM zones GROUP BY ring_id").df()
        annual=con.execute("SELECT year,ring_id,SUM(area_ha) AS area_ha FROM annual WHERE year BETWEEN ? AND ? GROUP BY year,ring_id",[start,end]).df()
    order=["0-5km","5-10km","10-20km",">20km"]
    years=sorted(map(int,a.year.unique()))
    base=pd.MultiIndex.from_product([years,order],names=["year","ring_id"]).to_frame(index=False)
    annual=base.merge(annual,on=["year","ring_id"],how="left").merge(zones,on="ring_id",how="left").fillna({"area_ha":0,"territory_ha":0})
    annual["intensity"]=100*annual.area_ha/annual.territory_ha.replace(0,np.nan)
    summary=annual.groupby("ring_id",as_index=False).agg(area_ha=("area_ha","sum"),territory_ha=("territory_ha","first")).set_index("ring_id").reindex(order).reset_index()
    summary["intensity"]=100*summary.area_ha/summary.territory_ha.replace(0,np.nan)
    summary["share_pct"]=100*summary.area_ha/summary.area_ha.sum() if summary.area_ha.sum()>0 else 0
    return {"bands":json.loads(summary.to_json(orient="records")),"annual":json.loads(annual.to_json(orient="records"))}

@lru_cache(maxsize=1)
def buffer_geojson(stamp):
    z=gpd.read_parquet(ROOT/"data/v2/zones.parquet")
    z=z[["ring_id","geometry"]].dissolve(by="ring_id").reset_index().to_crs(4326)
    z.geometry=z.geometry.simplify(.00015,preserve_topology=True)
    return z.to_json(drop_id=True)

@app.get("/api/proximity/map")
def proximity_map():
    p=ROOT/"data/v2/zones.parquet"
    return Response(buffer_geojson(p.stat().st_mtime_ns),media_type="application/geo+json")

@app.get("/api/environment/map")
def environment_map(resolution:int=5,start:int=2008,end:int=2024):
    d=environmental_sample(resolution,start,end)
    g,a,m=filtered(resolution,start,end)
    year=int(d.year.max())
    last=d[d.year==year].set_index("cell_id")
    g["forest_pct"]=100*g.cell_id.map(last.forest_ha)/g.territory_ha
    g["forest_ha"]=g.cell_id.map(last.forest_ha)
    # Exposure-normalized annual value, masked using the same eligibility rule.
    ratio=100*last.area_ha/last.forest_ha.replace(0,np.nan)
    ratio=ratio.where((last.coverage_fraction>=.95)&(last.forest_ha>=1))
    g["forest_ratio"]=g.cell_id.map(ratio)
    return Response(g.to_json(drop_id=True),media_type="application/geo+json")

@app.get("/api/proximity/export")
def proximity_export(resolution:int=5,start:int=2008,end:int=2024):
    data=proximity(resolution,start,end)
    return Response(pd.DataFrame(data["annual"]).to_csv(index=False),media_type="text/csv",headers={"Content-Disposition":"attachment; filename=proximity.csv"})

@app.get("/api/landcover")
def landcover(resolution:int=5):
    from landcover_v3 import sha
    folder=ROOT/"data/v3"/str(resolution)
    target=folder/"landcover"
    if not (target/"manifest.json").exists():
        return {"available":False,"message":"MapBiomas ainda não processado para esta grade."}
    m=json.loads((target/"manifest.json").read_text())
    if m["grid_sha256"]!=sha(folder/"grid.parquet"):
        raise HTTPException(409,"Grade alterada; reprocesse MapBiomas")
    c=pd.read_parquet(target/"coverage.parquet")
    t=pd.read_parquet(target/"transitions.parquet")
    annual=c.groupby(["year","code"],as_index=False).area_ha.sum()
    transitions=t.groupby(["from_code","to_code"],as_index=False).area_ha.sum()
    return {"available":True,"manifest":m,"annual":json.loads(annual.to_json(orient="records")),
            "transitions":json.loads(transitions.to_json(orient="records"))}

@app.get("/api/landcover/export")
def landcover_export(resolution:int=5,kind:str="coverage"):
    if kind not in ("coverage","transitions"):raise HTTPException(422,"Tipo inválido")
    if not landcover(resolution)["available"]:raise HTTPException(404,"MapBiomas não processado")
    p=ROOT/"data/v3"/str(resolution)/"landcover"/(kind+".parquet")
    return Response(pd.read_parquet(p).to_csv(index=False),media_type="text/csv",headers={"Content-Disposition":f"attachment; filename=mapbiomas-{kind}.csv"})

@app.get("/api/landcover/map")
def landcover_map(resolution:int=5,year:int=2024,code:int=1):
    info=landcover(resolution)
    if not info["available"]:raise HTTPException(404,info["message"])
    manifest=info["manifest"]
    if year not in manifest["years"] or str(code) not in manifest["groups"]:
        raise HTTPException(422,"Ano ou classe não processados")
    g,_,_=dataset(resolution)
    g=g[["cell_id","territory_ha","distance_km","road_density","geometry"]].copy()
    c=pd.read_parquet(ROOT/"data/v3"/str(resolution)/"landcover/coverage.parquet")
    c=c[c.year==year]
    total=c.groupby("cell_id").area_ha.sum()
    chosen=c[c.code==code].groupby("cell_id").area_ha.sum()
    missing=c[c.code.isin([0,10])].groupby("cell_id").area_ha.sum()
    g["sampled_ha"]=g.cell_id.map(total)
    g["cover_ha"]=g.cell_id.map(chosen).fillna(0).where(g.sampled_ha>0)
    g["cover_pct"]=100*g.cover_ha/g.sampled_ha.where(g.sampled_ha>0)
    g["missing_pct"]=100*g.cell_id.map(missing).fillna(0)/g.sampled_ha.where(g.sampled_ha>0)
    # For recognized classes, do not depict an entirely unclassified cell as zero coverage.
    if code not in (0,10):
        g.loc[g.missing_pct>=99.999999,["cover_pct","cover_ha"]]=np.nan
    return Response(g.to_json(drop_id=True),media_type="application/geo+json")

@app.get("/api/landcover/changes-map")
def landcover_changes_map(resolution:int=5,from_code:int=1,to_code:int=3):
    info=landcover(resolution)
    if not info["available"]:raise HTTPException(404,info["message"])
    if from_code in (0,10) or to_code in (0,10) or from_code==to_code or any(str(k) not in info["manifest"]["groups"] for k in (from_code,to_code)):
        raise HTTPException(422,"Selecione duas classes reconhecidas e diferentes")
    g,_,_=dataset(resolution)
    g=g[["cell_id","territory_ha","distance_km","road_density","geometry"]].copy()
    t=pd.read_parquet(ROOT/"data/v3"/str(resolution)/"landcover/transitions.parquet")
    valid=t[~t.from_code.isin([0,10])&~t.to_code.isin([0,10])]
    total=valid.groupby("cell_id").area_ha.sum()
    chosen=valid[(valid.from_code==from_code)&(valid.to_code==to_code)].groupby("cell_id").area_ha.sum()
    sampled=t.groupby("cell_id").area_ha.sum()
    g["sampled_ha"]=g.cell_id.map(total)
    g["cover_ha"]=g.cell_id.map(chosen).fillna(0).where(g.sampled_ha>0)
    g["cover_pct"]=100*g.cover_ha/g.sampled_ha.where(g.sampled_ha>0)
    g["missing_pct"]=100*(1-g.sampled_ha.fillna(0)/g.cell_id.map(sampled).where(lambda x:x>0))
    return Response(g.to_json(drop_id=True),media_type="application/geo+json")
