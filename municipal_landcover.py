"""MapBiomas municipal statistics, directly reduced over IBGE boundaries."""
import argparse
import hashlib
import json
import os
from datetime import datetime,timezone
from pathlib import Path
import geopandas as gpd
import pandas as pd
from landcover_v3 import ASSET,GROUPS,SOURCE,sha,grouped,reduce
from pipeline_v2 import AREA

ROOT=Path(__file__).resolve().parent
BOUNDARY=ROOT/"data/v2/municipalities.parquet"
TARGET=ROOT/"data/v3/municipal"

def main():
    import ee
    p=argparse.ArgumentParser();p.add_argument("--project",default=os.getenv("EE_PROJECT"));p.add_argument("--start",type=int,default=2020);p.add_argument("--end",type=int,default=2025);p.add_argument("--batch-size",type=int,default=1);a=p.parse_args()
    if not a.project or a.start>=a.end or a.batch_size<1:p.error("Informe projeto, período e lote válidos")
    ee.Initialize(project=a.project)
    boundary_hash=sha(BOUNDARY)
    mun=gpd.read_parquet(BOUNDARY)
    mun["CD_MUN"]=mun.CD_MUN.astype(str)
    mun=mun.sort_values("CD_MUN").reset_index(drop=True)
    if mun.CD_MUN.duplicated().any():raise ValueError("Município duplicado")
    expected=mun.to_crs(AREA).geometry.area/10000
    territories=dict(zip(mun.CD_MUN,expected))
    grid=mun[["CD_MUN","geometry"]].rename(columns={"CD_MUN":"cell_id"}).to_crs(4326)
    asset=ee.Image(ASSET)
    years=list(range(a.start,a.end+1))
    bands=set(asset.bandNames().getInfo())
    if any(f"classification_{y}" not in bands for y in years):raise ValueError("Ano indisponível")
    config=hashlib.sha256(json.dumps([ASSET,GROUPS,30,a.batch_size],sort_keys=True).encode()).hexdigest()[:16]
    cache=TARGET/"cache"/boundary_hash/config
    images={y:grouped(asset.select(f"classification_{y}")) for y in years}
    annual=[]
    for y in years:
        print("[MUNICÍPIOS]",y,flush=True)
        frame=reduce(images[y],grid,a.batch_size,cache/str(y))
        frame["year"]=y;annual.append(frame)
    coverage=pd.concat(annual,ignore_index=True)
    trans=reduce(images[a.start].multiply(100).add(images[a.end]).toInt16(),grid,a.batch_size,cache/f"transition-{a.start}-{a.end}")
    trans["from_code"]=trans.code//100;trans["to_code"]=trans.code%100;trans=trans.drop(columns="code")
    for frame in [*([v for _,v in coverage.groupby("year")]),trans]:
        totals=frame.groupby("cell_id").area_ha.sum().reindex(territories,fill_value=0)
        for k,v in totals.items():
            if abs(v-territories[k])/territories[k]>.01:raise ValueError(f"Área amostrada difere mais de 1%: {k}")
    manifest={"asset":ASSET,"source":SOURCE,"collection":11,"boundary_sha256":boundary_hash,"years":years,"transition":[a.start,a.end],"groups":GROUPS,"scale_m":30,"territory_ha":territories,"created_utc":datetime.now(timezone.utc).isoformat(),"method":"Redução direta da área de pixels de 30 m dentro dos polígonos IBGE, não agregação de células de 5 km."}
    TARGET.mkdir(parents=True,exist_ok=True)
    for name,df in [("coverage",coverage),("transitions",trans)]:
        tmp=TARGET/(name+".tmp");df.rename(columns={"cell_id":"municipality"}).to_parquet(tmp,index=False);tmp.replace(TARGET/(name+".parquet"))
    tmp=TARGET/"manifest.tmp";tmp.write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding="utf-8");tmp.replace(TARGET/"manifest.json")
    print("[OK] Estatísticas municipais salvas em",TARGET,flush=True)

if __name__=="__main__":main()
