"""Monthly state-level monitoring, independent of annual land cover."""
import argparse
import calendar
import json
from datetime import date,datetime,timezone,timedelta
from pathlib import Path
import geopandas as gpd
from shapely.geometry import mapping
from landcover_v3 import sha

ROOT=Path(__file__).resolve().parent
SOURCES={"fire":{"asset":"FIRMS","url":"https://developers.google.com/earth-engine/datasets/catalog/FIRMS","unit":"pixel-dias com detecção (MODIS, confiança >= 30%)","band":"confidence","scale":1000},
"rain":{"asset":"UCSB-CHG/CHIRPS/DAILY","url":"https://developers.google.com/earth-engine/datasets/catalog/UCSB-CHG_CHIRPS_DAILY","unit":"mm","band":"precipitation","scale":5566}}

def months(start,end):
    d=start.replace(day=1)
    while d<end:
        n=(d.replace(day=28)+timedelta(days=4)).replace(day=1)
        yield max(d,start),min(n,end)
        d=n

def main():
    import ee
    p=argparse.ArgumentParser();p.add_argument("--project",required=True);p.add_argument("--cell-km",type=int,default=5);p.add_argument("--start",default="2020-01-01");p.add_argument("--end",default=datetime.now(timezone.utc).date().isoformat(),help="Data exclusiva UTC");p.add_argument("--check-only",action="store_true");a=p.parse_args()
    start,end=date.fromisoformat(a.start),date.fromisoformat(a.end)
    if start>=end:p.error("Período inválido")
    ee.Initialize(project=a.project)
    grid_path=ROOT/"data/v3"/str(a.cell_km)/"grid.parquet"
    grid_hash=sha(grid_path)
    grid=gpd.read_parquet(grid_path).to_crs(4326)
    geom=ee.Geometry(mapping(grid.geometry.union_all()),geodesic=False)
    target=ROOT/"data/v3"/str(a.cell_km)/"monitoring";target.mkdir(parents=True,exist_ok=True)
    all_data={}
    for kind,source in SOURCES.items():
        collection=ee.ImageCollection(source["asset"]).filterDate(start.isoformat(),end.isoformat())
        stamps=collection.aggregate_array("system:time_start").getInfo()
        dates={datetime.fromtimestamp(s/1000,timezone.utc).date() for s in stamps}
        if not dates:raise ValueError("Coleção sem datas: "+kind)
        print("[CATÁLOGO]",kind,min(dates),max(dates),flush=True)
        if a.check_only:continue
        rows=[]
        for lo,hi in months(start,end):
            days=sorted(d for d in dates if lo<=d<hi)
            expected=(hi-lo).days
            value=None
            if days:
                part=collection.filterDate(lo.isoformat(),hi.isoformat()).select(source["band"])
                projection=ee.Image(part.first()).projection()
                if kind=="fire":
                    image=ee.ImageCollection.fromImages([collection.filterDate(d.isoformat(),(d+timedelta(days=1)).isoformat()).select("confidence").max().gte(30).unmask(0).rename("value") for d in days]).sum()
                    result=image.reduceRegion(reducer=ee.Reducer.sum(),geometry=geom,crs=projection,scale=source["scale"],maxPixels=1e10,tileScale=4).getInfo()
                else:
                    image=part.sum().rename("value")
                    area=ee.Image.pixelArea().updateMask(image.mask())
                    sums=image.multiply(area).rename("weighted").addBands(area.rename("area")).reduceRegion(reducer=ee.Reducer.sum(),geometry=geom,crs=projection,scale=source["scale"],maxPixels=1e9,tileScale=4).getInfo()
                    result={"value":sums["weighted"]/sums["area"] if sums.get("area") else None}
                value=result.get("value")
            rows.append({"month":lo.strftime("%Y-%m"),"start":lo.isoformat(),"end_exclusive":hi.isoformat(),"days_available":len(days),"days_expected":expected,"complete":len(days)==calendar.monthrange(lo.year,lo.month)[1] and lo.day==1 and expected==calendar.monthrange(lo.year,lo.month)[1],"value":value})
            print(kind,lo,value,len(days),"/",expected,flush=True)
        all_data[kind]={**source,"last_date":max(dates).isoformat(),"rows":rows}
    if a.check_only:return
    obj={"available":True,"grid_sha256":grid_hash,"created_utc":datetime.now(timezone.utc).isoformat(),"start":start.isoformat(),"end_exclusive":end.isoformat(),"sources":all_data}
    tmp=target/"monitoring.tmp";tmp.write_text(json.dumps(obj,ensure_ascii=False,allow_nan=False),encoding="utf-8");tmp.replace(target/"monitoring.json")
    print("[OK] Monitoramento atualizado",flush=True)

if __name__=="__main__":main()
