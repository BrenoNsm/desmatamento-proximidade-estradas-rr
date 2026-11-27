"""Territorial context from the public INPE WFS; exact disjoint area accounting."""
import argparse
import json
from pathlib import Path
from datetime import datetime, timezone
import geopandas as gpd
import pandas as pd
from shapely import union_all
from pipeline_v2 import AREA, polygons
from environment_inpe_v3 import ENDPOINT, catalogue, union_by_cell
from download_prodes_v2 import acquire_prodes, sha

ROOT=Path(__file__).resolve().parent
LAYERS={"indigenous":"indigenous_area_amazon_biome","conservation":"conservation_units_amazon_biome","residual":"residual_biome"}
GROUPS=("indigenous_only","conservation_only","both","outside")
LABELS={"indigenous_only":"Somente TI","conservation_only":"Somente UC","both":"TI e UC","outside":"Fora de TI/UC mapeadas"}

def partition(grid, indigenous, conservation, residual):
    grid=grid.to_crs(AREA)
    ti=union_by_cell(grid,indigenous)
    uc=union_by_cell(grid,conservation)
    res=union_by_cell(grid,residual)
    rows=[]; zones=[]
    for row in grid.itertuples():
        cell=row.geometry; empty=cell.difference(cell)
        a=ti.get(row.cell_id,empty); b=uc.get(row.cell_id,empty); r=res.get(row.cell_id,empty)
        pieces={"indigenous_only":a.difference(b),"conservation_only":b.difference(a),
                "both":a.intersection(b),"outside":cell.difference(a.union(b))}
        total=cell.area/10000
        areas={k:v.area/10000 for k,v in pieces.items()}
        if abs(sum(areas.values())-total)>max(.001,total*1e-7):
            raise ValueError(f"Partição territorial inconsistente: {row.cell_id}")
        rows.append({"cell_id":row.cell_id,"indigenous_ha":a.area/10000,
            "conservation_ha":b.area/10000,"protected_ha":a.union(b).area/10000,
            "overlap_ha":a.intersection(b).area/10000,"residual_ha":r.area/10000,
            "indigenous_pct":100*a.area/cell.area,"conservation_pct":100*b.area/cell.area,
            "protected_pct":100*a.union(b).area/cell.area,"residual_pct":100*r.area/cell.area})
        for k,v in pieces.items():
            if v.area>0:zones.append({"cell_id":row.cell_id,"group":k,"territory_ha":areas[k],"geometry":v})
    return pd.DataFrame(rows),polygons(gpd.GeoDataFrame(zones,crs=AREA))

def annual_by_zone(fragments,zones):
    source=polygons(fragments.to_crs(AREA))[["year","geometry"]]
    result=[]
    for year,v in source.groupby("year"):
        print(f"[TERRITÓRIO] Interseção PRODES {year}",flush=True)
        cut=gpd.overlay(v,zones[["cell_id","group","geometry"]],how="intersection",keep_geom_type=True)
        cut["area_ha"]=cut.geometry.area/10000
        expected=v.geometry.area.sum()/10000
        if abs(cut.area_ha.sum()-expected)>max(.05,expected*1e-4):
            raise ValueError(f"Conservação de área falhou em {year}")
        agg=cut.groupby(["cell_id","group"],as_index=False).area_ha.sum()
        base=zones.groupby(["cell_id","group"],as_index=False).territory_ha.sum()
        base=base.merge(agg,on=["cell_id","group"],how="left").fillna({"area_ha":0})
        base["year"]=int(year);result.append(base)
    return pd.concat(result,ignore_index=True)

def main():
    p=argparse.ArgumentParser();p.add_argument("--cell-km",type=int,default=5,choices=[2,5,10,20]);args=p.parse_args()
    folder=ROOT/"data/v3"/str(args.cell_km)
    grid=gpd.read_parquet(folder/"grid.parquet")
    fragments=ROOT/"data/v2/fragments.parquet"
    layers=catalogue();inputs={};sources={}
    for role,short in LAYERS.items():
        name="prodes-amazon-nb:"+short
        if name not in layers:raise ValueError(f"Camada ausente no catálogo: {name}")
        print(f"[INPE] {role}: {name}",flush=True)
        path,receipt=acquire_prodes({"wfs":ENDPOINT,"layer":name},ROOT/"data/downloads_v3/inpe"/role)
        raw=gpd.read_file(path)
        if raw.crs is None:raise ValueError(f"CRS ausente: {role}")
        inputs[role]=polygons(raw.to_crs(AREA))
        sources[role]=dict(receipt,catalogue_title=layers[name])
    print("[TERRITÓRIO] Calculando frações e sobreposições por célula",flush=True)
    cov,zones=partition(grid,**inputs)
    annual=annual_by_zone(gpd.read_parquet(fragments),zones)
    # Publish separate products; keep existing environmental model and inputs intact.
    target=folder/"territory";target.mkdir(exist_ok=True)
    for name,df in [("covariates",cov),("annual",annual)]:
        tmp=target/(name+".tmp");df.to_parquet(tmp,index=False);tmp.replace(target/(name+".parquet"))
    for role,df in inputs.items():
        clipped=polygons(gpd.clip(df[["geometry"]],grid.to_crs(AREA).geometry.union_all()))
        tmp=target/(role+".tmp");clipped.to_crs(4326).to_parquet(tmp,index=False);tmp.replace(target/(role+".parquet"))
    manifest={"grid_sha256":sha(folder/"grid.parquet"),"increment_sha256":sha(fragments),
        "sources":sources,"created_utc":datetime.now(timezone.utc).isoformat(),
        "note":"Limites TI/UC do snapshot INPE, sem histórico de vigência; residual sem atribuição anual, separado de incrementos e do modelo."}
    tmp=target/"manifest.tmp";tmp.write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding="utf-8");tmp.replace(target/"manifest.json")
    print("[OK] Camadas territoriais disponíveis",flush=True)

if __name__=="__main__":main()
