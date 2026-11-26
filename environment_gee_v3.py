"""Aggregate official MapBiomas C11 coverage in Earth Engine; no raster downloads."""
import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import os
import ee
import geopandas as gpd
import pandas as pd
import numpy as np
from environment_v3 import FOREST, OPEN_NATIVE

ROOT=Path(__file__).resolve().parent
ASSET="projects/mapbiomas-public/assets/brazil/lulc/collection11/mapbiomas_brazil_collection11_coverage_v3"
SCALE=30

def aggregate(image, grid, batch_size=200):
    # Unmasked pixel area allows valid-coverage fractions to be audited.
    area=ee.Image.pixelArea().divide(10000)
    valid=image.mask().unmask(0).gt(0).And(image.unmask(0).neq(0)).And(image.unmask(0).neq(27))
    forest=image.remap(FOREST,[1]*len(FOREST),0).unmask(0).And(valid)
    opened=image.remap(OPEN_NATIVE,[1]*len(OPEN_NATIVE),0).unmask(0).And(valid)
    bands=ee.Image.cat([area.rename("sampled_ha"),area.multiply(valid).rename("valid_ha"),
                        area.multiply(forest).rename("forest_ha"),area.multiply(opened).rename("open_native_ha")])
    records=[]
    for start in range(0,len(grid),batch_size):
        part=grid.iloc[start:start+batch_size][["cell_id","geometry"]]
        features=json.loads(part.to_json(drop_id=True))["features"]
        fc=ee.FeatureCollection(features)
        reduced=bands.reduceRegions(collection=fc,reducer=ee.Reducer.sum(),
            scale=SCALE,crs=image.projection(),tileScale=4)
        reduced=reduced.map(lambda f:ee.Feature(None,f.toDictionary(["cell_id","sampled_ha","valid_ha","forest_ha","open_native_ha"])))
        rows=reduced.getInfo()["features"]
        records.extend([r["properties"] for r in rows])
        print(f"[GEE] {len(records)}/{len(grid)} células",flush=True)
    result=pd.DataFrame(records)
    if len(result)!=len(grid) or result.cell_id.duplicated().any() or set(result.cell_id)!=set(grid.cell_id):
        raise ValueError("Resultado incompleto ou IDs duplicados")
    cols=["sampled_ha","valid_ha","forest_ha","open_native_ha"]
    for c in cols:
        result[c]=pd.to_numeric(result[c],errors="raise").fillna(0)
    if (result[cols]<0).any().any() or result.valid_ha.sum()<=0:
        raise ValueError("Cobertura inválida")
    if (result.forest_ha+result.open_native_ha>result.valid_ha+0.01).any():
        raise ValueError("Áreas classificadas excedem área válida")
    return result

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--project",default=os.getenv("EE_PROJECT"))
    p.add_argument("--authenticate",action="store_true")
    p.add_argument("--cell-km",type=int,default=5)
    p.add_argument("--start",type=int)
    p.add_argument("--end",type=int)
    p.add_argument("--check-only",action="store_true")
    p.add_argument("--refresh",action="store_true")
    p.add_argument("--batch-size",type=int,default=200)
    args=p.parse_args()
    if not args.project: p.error("Informe --project com ID do projeto Google Cloud registrado no Earth Engine")
    if args.batch_size<1: p.error("--batch-size deve ser positivo")
    if args.authenticate: ee.Authenticate()
    ee.Initialize(project=args.project)
    image=ee.Image(ASSET)
    names=image.bandNames().getInfo()
    print(f"[GEE] Asset acessível: {ASSET}",flush=True)
    folder=ROOT/"data/v3"/str(args.cell_km)
    annual=pd.read_parquet(folder/"annual.parquet")
    years=[int(y) for y in sorted(annual.year.unique()) if (args.start is None or y>=args.start) and (args.end is None or y<=args.end)]
    if not years: raise ValueError("Nenhum ano PRODES no período")
    missing=[y-1 for y in years if f"classification_{y-1}" not in names]
    if missing: raise ValueError(f"Anos MapBiomas indisponíveis: {missing}")
    print(f"[GEE] Bandas disponíveis para {years}",flush=True)
    if args.check_only: return
    grid=gpd.read_parquet(folder/"grid.parquet").to_crs(4326)
    with (folder/"grid.parquet").open("rb") as f:
        grid_hash=hashlib.file_digest(f,"sha256").hexdigest()
    cache=folder/"mapbiomas11_gee_v3"/grid_hash
    cache.mkdir(parents=True,exist_ok=True)
    for year in years:
        path=cache/f"{year}.parquet"
        if path.exists() and not args.refresh:
            print(f"[CACHE] MapBiomas GEE {year-1}",flush=True);continue
        print(f"[MAPBIOMAS GEE] {year-1} → PRODES {year}",flush=True)
        result=aggregate(image.select(f"classification_{year-1}"),grid,args.batch_size)
        result["year"]=year
        result["cover_year"]=year-1
        result["collection"]=11
        result["coverage_fraction"]=result.valid_ha/result.sampled_ha.replace(0,np.nan)
        result["source_url"]="ee://"+ASSET
        result["retrieved_utc"]=datetime.now(timezone.utc).isoformat()
        tmp=path.with_suffix(".tmp");result.to_parquet(tmp,index=False);tmp.replace(path)
    all_data=pd.concat([pd.read_parquet(f) for f in sorted(cache.glob("*.parquet"))],ignore_index=True)
    tmp=folder/"environment.tmp";all_data.to_parquet(tmp,index=False);tmp.replace(folder/"environment.parquet")
    manifest={"collection":11,"asset":ASSET,"provider":"Earth Engine","grid_sha256":grid_hash,
        "pixel_m":SCALE,"forest_codes":FOREST,"open_native_codes":OPEN_NATIVE,
        "years":sorted(map(int,all_data.year.unique())),"area_method":"ee.Image.pixelArea; native image CRS; reduceRegions weighted sum"}
    (folder/"environment_manifest.json").write_text(json.dumps(manifest,indent=2),encoding="utf-8")
    print("[OK] Cobertura histórica disponível",flush=True)

if __name__=="__main__":
    main()
