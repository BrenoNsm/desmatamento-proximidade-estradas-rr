"""Local MapBiomas exploration. No requests to Earth Engine or NVIDIA."""
import json
from pathlib import Path
import geopandas as gpd
import pandas as pd
import numpy as np
from fastapi import HTTPException
from landcover_v3 import sha

ROOT=Path(__file__).resolve().parent

def inputs(resolution):
    folder=ROOT/"data/v3"/str(resolution)
    target=folder/"landcover"
    if not (target/"manifest.json").exists():raise HTTPException(404,"MapBiomas não processado nesta resolução")
    m=json.loads((target/"manifest.json").read_text())
    if m["grid_sha256"]!=sha(folder/"grid.parquet"):raise HTTPException(409,"Grade alterada; reprocesse MapBiomas")
    g=gpd.read_parquet(folder/"grid.parquet").to_crs(4326)
    return folder,target,m,g

def municipalities():
    p=ROOT/"data/v2"/"municipalities.parquet"
    if not p.exists():return gpd.GeoDataFrame(columns=["CD_MUN","NM_MUN","geometry"],geometry="geometry",crs=4326)
    return gpd.read_parquet(p).to_crs(4326)[["CD_MUN","NM_MUN","geometry"]].assign(CD_MUN=lambda x:x.CD_MUN.astype(str))

def assigned(g,mun):
    """Assign each entire cell to municipality of its interior representative point."""
    if mun.empty:return pd.Series(index=g.index,dtype="object")
    points=g[["cell_id","geometry"]].copy()
    points.geometry=points.geometry.representative_point()
    joined=gpd.sjoin(points,mun[["CD_MUN","geometry"]],how="left",predicate="intersects")
    lookup=joined.sort_values("CD_MUN").drop_duplicates("cell_id").set_index("cell_id").CD_MUN
    return g.cell_id.map(lookup)

def metadata(resolution):
    _,_,m,g=inputs(resolution)
    mun=municipalities()
    return {"years":m["years"],"transition":m["transition"],"groups":m["groups"],"asset":m["asset"],"scale_m":m.get("scale_m",30),"municipalities":json.loads(mun.drop(columns="geometry").sort_values("NM_MUN").to_json(orient="records")),"boundaries":json.loads(mun.to_json(drop_id=True)),"bounds":g.total_bounds.tolist()}

def explore(resolution,year,mode,code,from_code,to_code,municipality):
    _,target,m,g=inputs(resolution)
    codes={int(c) for c in m["groups"]};known=codes-{0,10}
    if year not in m["years"] or code not in codes or mode not in ("dominant","coverage","change"):raise HTTPException(422,"Indicador ou ano inválido")
    if mode=="change" and (from_code not in known or to_code not in known or from_code==to_code):raise HTTPException(422,"Selecione classes diferentes e reconhecidas")
    mun=municipalities()
    g=g[["cell_id","territory_ha","distance_km","road_density","geometry"]].copy()
    g["municipality"]=assigned(g,mun)
    if municipality:
        if municipality not in set(mun.CD_MUN):raise HTTPException(422,"Município desconhecido")
        g=g[g.municipality==municipality].copy()
    c=pd.read_parquet(target/"coverage.parquet")
    c=c[c.year==year]
    total=c.groupby("cell_id").area_ha.sum()
    missing=c[c.code.isin([0,10])].groupby("cell_id").area_ha.sum()
    g["sampled_ha"]=g.cell_id.map(total)
    g["missing_pct"]=100*g.cell_id.map(missing).fillna(0)/g.sampled_ha.where(g.sampled_ha>0)
    if mode=="dominant":
        # Include unknown classes in dominance, so a mostly unclassified cell is not shown as forest.
        dominant=c.sort_values(["area_ha","code"],ascending=[False,True]).drop_duplicates("cell_id").set_index("cell_id")
        g["dominant_code"]=g.cell_id.map(dominant.code)
        g["cover_ha"]=g.cell_id.map(dominant.area_ha)
    elif mode=="coverage":
        chosen=c[c.code==code].groupby("cell_id").area_ha.sum()
        g["cover_ha"]=g.cell_id.map(chosen).fillna(0).where(g.sampled_ha>0)
        if code in known:g.loc[g.missing_pct>=99.999999,"cover_ha"]=np.nan
    else:
        t=pd.read_parquet(target/"transitions.parquet")
        valid=t[t.from_code.isin(known)&t.to_code.isin(known)]
        denominator=valid.groupby("cell_id").area_ha.sum()
        sampled=t.groupby("cell_id").area_ha.sum()
        chosen=valid[(valid.from_code==from_code)&(valid.to_code==to_code)].groupby("cell_id").area_ha.sum()
        g["sampled_ha"]=g.cell_id.map(denominator)
        g["cover_ha"]=g.cell_id.map(chosen).fillna(0).where(g.sampled_ha>0)
        g["missing_pct"]=100*(1-g.sampled_ha.fillna(0)/g.cell_id.map(sampled).where(lambda x:x>0))
    g["cover_pct"]=100*g.cover_ha/g.sampled_ha.where(g.sampled_ha>0)
    return g

def profile(resolution,cell_id):
    _,target,m,g=inputs(resolution)
    if cell_id not in set(g.cell_id):raise HTTPException(404,"Célula desconhecida")
    c=pd.read_parquet(target/"coverage.parquet")
    c=c[c.cell_id==cell_id][["year","code","area_ha"]]
    t=pd.read_parquet(target/"transitions.parquet")
    t=t[t.cell_id==cell_id][["from_code","to_code","area_ha"]]
    return {"annual":json.loads(c.to_json(orient="records")),"transitions":json.loads(t.to_json(orient="records")),"period":m["transition"]}
