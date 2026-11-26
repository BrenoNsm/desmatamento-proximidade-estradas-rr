"""Local PRODES forest eligibility proxy. No credentials or raster downloads."""
import argparse
import hashlib
import json
import re
import unicodedata
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely import union_all
from pipeline_v2 import AREA, polygons
from download_prodes_v2 import acquire_prodes, sha

ROOT=Path(__file__).resolve().parent
ENDPOINT="https://terrabrasilis.dpi.inpe.br/geoserver/prodes-amazon-nb/ows"
# Only explicitly named baseline classes; never substitute annual or current cumulative data.
ALIASES={
 "baseline": {"accumulated_deforestation_2007_biome","deforestation_2007_biome","deforestation_2007","accumulated_deforestation_2007"},
 "nonforest":{"no_forest_biome","non_forest_biome","non_forest","nonforest_biome","nonforest"},
 "water":{"hydrography_biome","hydrography","hydro_biome","hydro"},
 "biome":{"amazon_biome_border","amazon_biome","amazon_biome_boundary","biome_boundary","biome_amazon","amazon_biome_limit","limit_amazon_biome"}
}

def catalogue():
    url=ENDPOINT+"?"+urllib.parse.urlencode({"service":"WFS","version":"2.0.0","request":"GetCapabilities"})
    with urllib.request.urlopen(url,timeout=120) as r:
        root=ET.fromstring(r.read())
    layers={}
    for node in root.findall(".//{*}FeatureType"):
        name=node.findtext("{*}Name")
        if name: layers[name]=node.findtext("{*}Title") or ""
    if not layers: raise RuntimeError("Catálogo WFS vazio")
    return layers

def resolve(layers, overrides):
    out={}
    for role,aliases in ALIASES.items():
        if overrides.get(role):
            name=overrides[role]
            if name not in layers: raise ValueError(f"Camada inexistente: {name}")
            out[role]=name;continue
        candidates=[name for name in layers if name.split(":")[-1].lower() in aliases]
        if len(candidates)!=1:
            raise ValueError(f"Camada {role} não identificada univocamente: {candidates}. Execute --list-layers e informe --{role} com o nome oficial.")
        out[role]=candidates[0]
    return out

def union_by_cell(grid, features):
    """Union inside each cell prevents double subtraction from overlapping masks."""
    result={}
    if features.empty:return result
    features=polygons(features.to_crs(grid.crs))
    for row in grid.itertuples():
        idx=features.sindex.query(row.geometry,predicate="intersects")
        if len(idx):
            result[row.cell_id]=union_all(features.geometry.iloc[idx].array).intersection(row.geometry)
    return result

def reconstruct(grid, domain, masks, increments, years):
    grid=grid.to_crs(AREA)
    domain=domain.to_crs(AREA).geometry.union_all()
    masks=masks.to_crs(AREA)
    increments=increments.to_crs(AREA)
    covered=union_by_cell(grid,masks)
    # Include all increments since 2008, not just the user-selected years.
    result=[]
    for pos,row in enumerate(grid.itertuples(),1):
        if pos==1 or pos%500==0 or pos==len(grid):
            print(f"[INPE] Reconstrução {pos}/{len(grid)} células",flush=True)
        cell=row.geometry
        eligible=cell.intersection(domain)
        initial=eligible.difference(covered.get(row.cell_id,cell.difference(cell)))
        prior=initial.difference(initial)
        idx=increments.sindex.query(cell,predicate="intersects")
        local=increments.iloc[idx]
        groups={int(y):union_all(v.geometry.array).intersection(initial) for y,v in local.groupby("year")}
        for year in range(2008,max(years)+1):
            forest=initial.difference(prior).area/10000
            if year in years:
                result.append({"cell_id":row.cell_id,"year":year,"cover_year":year-1,
                    "sampled_ha":cell.area/10000,"valid_ha":eligible.area/10000,
                    "forest_ha":forest,"open_native_ha":np.nan,
                    "initial_eligible_ha":initial.area/10000})
            if year in groups:prior=prior.union(groups[year])
    return pd.DataFrame(result)

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--cell-km",type=int,default=5)
    p.add_argument("--start",type=int)
    p.add_argument("--end",type=int)
    p.add_argument("--list-layers",action="store_true")
    p.add_argument("--check-only",action="store_true")
    for role in ALIASES:p.add_argument("--"+role)
    args=p.parse_args()
    layers=catalogue()
    print("[INPE] Camadas publicadas:",flush=True)
    for name,title in sorted(layers.items()):print(f"{name} | {title}",flush=True)
    if args.list_layers:return
    selected=resolve(layers,vars(args))
    print("[INPE] Seleção:",selected,flush=True)
    folder=ROOT/"data/v3"/str(args.cell_km)
    grid=gpd.read_parquet(folder/"grid.parquet")
    annual=pd.read_parquet(folder/"annual.parquet")
    years=[int(y) for y in sorted(annual.year.unique()) if (args.start is None or y>=args.start) and (args.end is None or y<=args.end)]
    if not years or min(years)<2008:raise ValueError("Período deve começar em 2008 ou depois")
    inputs={};provenance={}
    for role,name in selected.items():
        print(f"[INPE] Adquirindo {role}: {name}",flush=True)
        path,receipt=acquire_prodes({"wfs":ENDPOINT,"layer":name},ROOT/"data/downloads_v3/inpe"/role)
        inputs[role]=polygons(gpd.read_file(path).to_crs(AREA))
        provenance[role]=receipt
    baseline=inputs["baseline"]
    yearcol=next((c for c in baseline if c.lower()=="year"),None)
    classcol=next((c for c in baseline if c.lower()=="class_name"),None)
    if yearcol is not None:
        if set(pd.to_numeric(baseline[yearcol],errors="raise").unique())!={2007}:
            raise ValueError("Máscara acumulada não corresponde exclusivamente a 2007")
    elif classcol is not None:
        if set(baseline[classcol].astype(str).str.lower().unique())!={"d2007"}:
            raise ValueError("Classe de máscara incompatível com d2007")
    elif selected["baseline"] == "prodes-amazon-nb:accumulated_deforestation_2007_biome" and "2007" in layers[selected["baseline"]]:
        print("[INPE] Baseline 2007 identificado pelo nome e título oficiais da camada (sem atributo temporal)",flush=True)
    else:raise ValueError("Não foi possível comprovar data da máscara acumulada")
    provenance["baseline"]["catalogue_title"]=layers[selected["baseline"]]
    provenance["baseline"]["date_evidence"]="year attribute" if yearcol is not None else ("class_name attribute" if classcol is not None else "official layer name and title")
    if args.check_only:
        print("[OK] Camadas adquiridas e máscara de 2007 verificada");return
    increments=polygons(gpd.read_parquet(ROOT/"data/v2/fragments.parquet").to_crs(AREA))[["year","geometry"]]
    if not set(range(2008,max(years)+1)).issubset(set(increments.year.unique())):
        raise ValueError("Série histórica incompleta desde 2008; não é seguro reconstruir disponibilidade")
    masks=gpd.GeoDataFrame(pd.concat([inputs[k][["geometry"]] for k in ("baseline","nonforest","water")],ignore_index=True),crs=AREA)
    print("[INPE] Reconstruindo disponibilidade por célula e ano",flush=True)
    data=reconstruct(grid,inputs["biome"],masks,increments,years)
    data["coverage_fraction"]=data.valid_ha/data.sampled_ha.replace(0,np.nan)
    data["source_url"]=ENDPOINT
    data["provider"]="PRODES eligibility proxy"
    data["retrieved_utc"]=datetime.now(timezone.utc).isoformat()
    data=data.merge(annual,on=["cell_id","year"],how="left",validate="one_to_one")
    data["increment_exceeds_proxy"]=data.area_ha>data.forest_ha+0.01
    # Exclude inconsistent observations rather than truncate or silently repair.
    data.loc[data.increment_exceeds_proxy,"coverage_fraction"]=0
    data=data.drop(columns="area_ha")
    manifest={"provider":"PRODES eligibility proxy","grid_sha256":sha(folder/"grid.parquet"),"years":years,
       "baseline_year":2007,"sources":provenance,"increment_sha256":sha(ROOT/"data/v2/fragments.parquet"),
       "inconsistent_observations":int(data.increment_exceeds_proxy.sum()),
       "limitations":"Proxy sem regeneração, resíduos e nuvens históricas; não é cobertura florestal observada. Domínio do bioma e máscaras atuais podem ter revisões."}
    tmp=folder/"environment.tmp";data.to_parquet(tmp,index=False);tmp.replace(folder/"environment.parquet")
    (folder/"environment_manifest.json").write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding="utf-8")
    print("[OK] Proxy histórico PRODES disponível",flush=True)

if __name__=="__main__":main()
