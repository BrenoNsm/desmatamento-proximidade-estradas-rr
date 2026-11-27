"""Independent MapBiomas C11 products. No writes to PRODES environment files."""
import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import geopandas as gpd
import numpy as np

ROOT=Path(__file__).resolve().parent
ASSET="projects/mapbiomas-public/assets/brazil/lulc/collection11/mapbiomas_brazil_collection11_coverage_v3"
SOURCE="https://brasil.mapbiomas.org/iniciativas-e-produtos/cobertura-e-uso-da-terra/cobertura-30m/cobertura/"
# Analytical aggregation of the official C11 legend; not the official level-1 hierarchy.
GROUPS={
 1:("Florestas arbóreas",[3,5,6,49,1]),
 2:("Savanas e vegetação aberta",[4,7,10,11,12,77,84,50,32]),
 3:("Pastagem",[15]),
 4:("Agricultura",[18,19,39,20,40,62,41,36,46,47,35,48]),
 5:("Silvicultura e mosaicos",[9,21,14]),
 6:("Urbanização",[24]),
 7:("Mineração",[30]),
 8:("Água e aquicultura",[26,33,31]),
 9:("Outras áreas classificadas",[22,23,25,29,75,91]),
 10:("Código não reconhecido",[]),
 0:("Sem classificação válida",[0,27])
}
def sha(path):
    with path.open("rb") as f:return hashlib.file_digest(f,"sha256").hexdigest()

def grouped(image):
    pairs=[(code,k) for k,(_,codes) in GROUPS.items() for code in codes]
    return image.unmask(0).remap([x[0] for x in pairs],[x[1] for x in pairs],10).rename("class").toInt16()

def parse_rows(rows, ids):
    seen=set(); out=[]
    for feature in rows:
        p=feature["properties"]; cell=p["cell_id"]
        if cell in seen:raise ValueError("ID duplicado")
        seen.add(cell)
        for entry in p.get("groups") or []:
            code=int(entry["code"]); area=float(entry["sum"])
            if not np.isfinite(area) or area<0:raise ValueError("Área inválida")
            out.append({"cell_id":cell,"code":code,"area_ha":area})
    if seen!=set(ids):raise ValueError("Resposta incompleta")
    return pd.DataFrame(out,columns=["cell_id","code","area_ha"])

def reduce(image,grid,batch_size,cache):
    import ee
    cache.mkdir(parents=True,exist_ok=True)
    chunks=[]
    for pos in range(0,len(grid),batch_size):
        part=grid.iloc[pos:pos+batch_size][["cell_id","geometry"]]
        # Cache keys include the batch size and grid hash in parent path.
        path=cache/f"{pos}.parquet"
        if path.exists():chunk=pd.read_parquet(path)
        else:
            fc=ee.FeatureCollection(json.loads(part.to_json(drop_id=True))["features"])
            bands=ee.Image.pixelArea().divide(10000).rename("ha").addBands(image.rename("code"))
            res=bands.reduceRegions(collection=fc,reducer=ee.Reducer.sum().group(groupField=1,groupName="code"),crs=image.projection(),scale=30,tileScale=4)
            res=res.map(lambda f:ee.Feature(None,f.toDictionary(["cell_id","groups"])))
            chunk=parse_rows(res.getInfo()["features"],part.cell_id)
            tmp=path.with_suffix(".tmp");chunk.to_parquet(tmp,index=False);tmp.replace(path)
        chunks.append(chunk)
        print(f"[GEE] {min(pos+batch_size,len(grid))}/{len(grid)} células",flush=True)
    return pd.concat(chunks,ignore_index=True)

def main():
    import ee
    p=argparse.ArgumentParser()
    p.add_argument("--project",default=os.getenv("EE_PROJECT"))
    p.add_argument("--cell-km",type=int,default=5,choices=[2,5,10,20])
    p.add_argument("--start",type=int,default=2020);p.add_argument("--end",type=int,default=2024)
    p.add_argument("--batch-size",type=int,default=100)
    p.add_argument("--authenticate",action="store_true")
    p.add_argument("--check-only",action="store_true")
    a=p.parse_args()
    if not a.project or a.start>a.end or a.batch_size<1:p.error("Informe projeto, período válido e lote positivo")
    if a.authenticate:ee.Authenticate()
    ee.Initialize(project=a.project)
    asset=ee.Image(ASSET)
    years=list(range(a.start,a.end+1))
    bands=set(asset.bandNames().getInfo())
    if any(f"classification_{y}" not in bands for y in years):raise ValueError("Ano indisponível no asset")
    print("[OK] Asset e bandas disponíveis",flush=True)
    if a.check_only:return
    folder=ROOT/"data/v3"/str(a.cell_km)
    grid_path=folder/"grid.parquet";grid=gpd.read_parquet(grid_path).to_crs(4326)
    grid_hash=sha(grid_path)
    config_hash=hashlib.sha256(json.dumps([ASSET,GROUPS,30,a.batch_size],sort_keys=True).encode()).hexdigest()[:16]
    target=folder/"landcover";target.mkdir(exist_ok=True)
    cache=target/"cache"/grid_hash/config_hash
    images={y:grouped(asset.select(f"classification_{y}")) for y in years}
    records=[]
    for y in years:
        print(f"[COBERTURA] {y}",flush=True)
        r=reduce(images[y],grid,a.batch_size,cache/str(y));r["year"]=y;records.append(r)
    coverage=pd.concat(records,ignore_index=True)
    # True pixel-level endpoint transitions, not differences of class totals.
    print(f"[TRANSIÇÕES] {a.start} → {a.end}",flush=True)
    transition=images[a.start].multiply(100).add(images[a.end]).toInt16()
    trans=reduce(transition,grid,a.batch_size,cache/f"transition-{a.start}-{a.end}")
    trans["from_code"]=trans.code//100;trans["to_code"]=trans.code%100
    trans=trans.drop(columns="code")
    if coverage.empty or trans.empty:raise ValueError("Resultado vazio")
    # Compare the sampled raster area with the vector area for every cell.
    expected=grid.set_index("cell_id").territory_ha
    for _,frame in coverage.groupby("year"):
        totals=frame.groupby("cell_id").area_ha.sum().reindex(expected.index,fill_value=0)
        if abs(totals.sum()-expected.sum())/expected.sum()>.01:raise ValueError("Área amostrada diverge do território em mais de 1%")
    manifest={"asset":ASSET,"source":SOURCE,"collection":11,"grid_sha256":grid_hash,"years":years,
       "transition":[a.start,a.end],"groups":GROUPS,"scale_m":30,"created_utc":datetime.now(timezone.utc).isoformat(),
       "method":"Soma de pixelArea em hectares; grupos analíticos; transição pixel a pixel entre extremos; classes desconhecidas e sem dados explícitas."}
    for name,df in [("coverage",coverage),("transitions",trans)]:
        tmp=target/f"{name}.tmp";df.to_parquet(tmp,index=False);tmp.replace(target/f"{name}.parquet")
    tmp=target/"manifest.tmp";tmp.write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding="utf-8");tmp.replace(target/"manifest.json")
    print("[OK] MapBiomas salvo separadamente; PRODES preservado",flush=True)

if __name__=="__main__":main()
