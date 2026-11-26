"""Reproducible regular-grid analysis; all areas in regional equal-area CRS."""
from pathlib import Path
import argparse
import json
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import box
from pipeline_v2 import AREA, DIST, polygons

ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / "data/v2"
OUT = ROOT / "data/v3"

def build_grid(municipalities, roads, fragments, cell_km=5):
    if cell_km not in (2, 5, 10, 20):
        raise ValueError("Resolution must be 2, 5, 10 or 20 km")
    size = cell_km * 1000
    aoi = municipalities.to_crs(AREA).geometry.union_all()
    x0,y0,x1,y1 = aoi.bounds
    rows = []
    for i in range(int(np.floor(x0/size)), int(np.ceil(x1/size))):
        for j in range(int(np.floor(y0/size)), int(np.ceil(y1/size))):
            rows.append((f"{i}:{j}",i,j,box(i*size,j*size,(i+1)*size,(j+1)*size)))
    grid = gpd.GeoDataFrame(rows, columns=["cell_id","i","j","geometry"], crs=AREA)
    grid = gpd.clip(grid, aoi, keep_geom_type=True)
    grid = grid[grid.geometry.area > 0].copy()
    grid["territory_ha"] = grid.geometry.area / 10000
    # Distance of the centroid of the clipped cell; centroid may be outside a concave cell.
    centers = grid.geometry.centroid.to_crs(DIST)
    roads_d = roads.to_crs(DIST)[["geometry"]]
    near = gpd.sjoin_nearest(gpd.GeoDataFrame({"cell_id":grid.cell_id},geometry=centers,crs=DIST), roads_d, distance_col="distance_m")
    grid["distance_km"] = grid.cell_id.map(near.groupby("cell_id").distance_m.min()) / 1000
    roads_a = roads.to_crs(AREA)[["geometry"]]
    lengths = gpd.overlay(roads_a, grid[["cell_id","geometry"]], how="intersection",keep_geom_type=True)
    lengths["length_km"] = lengths.geometry.length / 1000
    grid["road_density"] = grid.cell_id.map(lengths.groupby("cell_id").length_km.sum()).fillna(0)/(grid.territory_ha/100)
    source = polygons(fragments.to_crs(AREA))[["year","geometry"]]
    years = sorted(source.year.unique())
    print(f"[GRADE] {len(grid)} células, {len(years)} anos",flush=True)
    results=[]
    for year in years:
        print(f"[GRADE] {year}",flush=True)
        cut = gpd.overlay(source[source.year==year],grid[["cell_id","geometry"]],how="intersection",keep_geom_type=True)
        cut["area_ha"] = cut.geometry.area/10000
        agg=cut.groupby("cell_id").area_ha.sum()
        expected=source.loc[source.year==year].geometry.area.sum()/10000
        if abs(agg.sum()-expected)>max(.05,expected*0.0001):
            raise ValueError(f"Conservação de área falhou: {year}")
        results.append(pd.DataFrame({"cell_id":grid.cell_id,"year":int(year),"area_ha":grid.cell_id.map(agg).fillna(0)}))
    annual=pd.concat(results,ignore_index=True)
    return grid,annual

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--cell-km",type=int,default=5)
    args=parser.parse_args()
    OUT.mkdir(parents=True,exist_ok=True)
    grid,annual=build_grid(*(gpd.read_parquet(SOURCE/f"{n}.parquet") for n in ("municipalities","roads","fragments")),cell_km=args.cell_km)
    target=OUT/str(args.cell_km)
    target.mkdir(exist_ok=True)
    grid.to_crs(4326).to_parquet(target/"grid.parquet",index=False)
    annual.to_parquet(target/"annual.parquet",index=False)
    (target/"manifest.json").write_text(json.dumps({"cell_km":args.cell_km,"source":json.loads((SOURCE/"manifest.json").read_text()),"years":sorted(map(int,annual.year.unique()))},ensure_ascii=False,indent=2),encoding="utf-8")

if __name__=="__main__":
    main()
