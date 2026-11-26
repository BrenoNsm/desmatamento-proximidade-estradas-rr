"""MapBiomas collection 11: automatic regional window reads, annual grid coverage."""
import argparse
import hashlib
import json
import urllib.request
import urllib.error
from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import pandas as pd
import geopandas as gpd
import rasterio
from rasterio.features import rasterize
from rasterio.transform import from_origin
from rasterio.vrt import WarpedVRT
from rasterio.enums import Resampling
from rasterio.windows import Window
from shapely.geometry import box
from pipeline_v2 import AREA

ROOT=Path(__file__).resolve().parent
URLS={11:"https://storage.googleapis.com/mapbiomas-public/initiatives/brasil/collection11/lulc/coverage/brazil_coverage/brazil_coverage-col11_{year}.tif",10:"https://storage.googleapis.com/mapbiomas-public/initiatives/brasil/collection_10/lulc/coverage/brazil_coverage_{year}.tif"}
FOREST=[3,6,5,49]
OPEN_NATIVE=[4,7,11,12,77,84,50,32]
PIXEL=30

def remote_raster(url):
    """Check the actual object with a bounded GET, not a directory listing or HEAD."""
    req=urllib.request.Request(url,headers={"Range":"bytes=0-15","User-Agent":"GeoForestLab/3.0"})
    try:
        with urllib.request.urlopen(req,timeout=60) as response:
            status=response.status
            signature=response.read(4)
            if status!=206:
                raise RuntimeError(f"Servidor não oferece leitura parcial HTTP (status {status}): {url}")
            if signature not in (b"II*\x00",b"MM\x00*",b"II+\x00",b"MM\x00+"):
                raise RuntimeError(f"Resposta não é TIFF: {url}")
            print(f"[HTTP] GET Range {status}; TIFF confirmado",flush=True)
    except urllib.error.HTTPError as e:
        detail=e.read(500).decode("utf-8",errors="replace")
        raise RuntimeError(f"MapBiomas retornou HTTP {e.code} no GET direto do arquivo oficial: {url}\n{detail}") from e
    return "/vsicurl?use_head=no&list_dir=no&empty_dir=yes&url="+url

def aggregate(grid, source):
    grid=grid.to_crs(AREA).reset_index(drop=True)
    x0,y0,x1,y1=grid.total_bounds
    left=np.floor(x0/PIXEL)*PIXEL
    top=np.ceil(y1/PIXEL)*PIXEL
    width=int(np.ceil((x1-left)/PIXEL))
    height=int(np.ceil((top-y0)/PIXEL))
    transform=from_origin(left,top,PIXEL,PIXEL)
    counts=np.zeros((4,len(grid)+1),dtype=np.int64)
    with rasterio.open(source) as src:
        with WarpedVRT(src,crs=AREA,transform=transform,width=width,height=height,
                       resampling=Resampling.nearest,nodata=0) as vrt:
            for row in range(0,height,1024):
                for col in range(0,width,1024):
                    w=Window(col,row,min(1024,width-col),min(1024,height-row))
                    t=vrt.window_transform(w)
                    idx=grid.sindex.query(box(*rasterio.windows.bounds(w,transform)),predicate="intersects")
                    if not len(idx): continue
                    ids=rasterize([(grid.geometry.iloc[k],int(k)+1) for k in idx],
                        out_shape=(int(w.height),int(w.width)),transform=t,fill=0,dtype="int32")
                    a=vrt.read(1,window=w,masked=True)
                    valid=(ids>0)&~np.ma.getmaskarray(a)&(a.data!=0)&(a.data!=27)
                    for k,mask in enumerate([ids>0,valid,valid&np.isin(a.data,FOREST),valid&np.isin(a.data,OPEN_NATIVE)]):
                        counts[k]+=np.bincount(ids[mask],minlength=len(grid)+1)
    scale=PIXEL**2/10000
    return pd.DataFrame({"cell_id":grid.cell_id,"sampled_ha":counts[0,1:]*scale,
        "valid_ha":counts[1,1:]*scale,"forest_ha":counts[2,1:]*scale,
        "open_native_ha":counts[3,1:]*scale})

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--cell-km",type=int,default=5)
    p.add_argument("--start",type=int)
    p.add_argument("--end",type=int)
    p.add_argument("--refresh",action="store_true")
    p.add_argument("--collection",type=int,choices=(10,11),default=10)
    p.add_argument("--check-only",action="store_true")
    args=p.parse_args()
    folder=ROOT/"data/v3"/str(args.cell_km)
    grid=gpd.read_parquet(folder/"grid.parquet")
    annual=pd.read_parquet(folder/"annual.parquet")
    with (folder/"grid.parquet").open("rb") as f:
        grid_hash=hashlib.file_digest(f,"sha256").hexdigest()
    years=[int(y) for y in sorted(annual.year.unique()) if (args.start is None or y>=args.start) and (args.end is None or y<=args.end) and 1986<=y<=(2026 if args.collection==11 else 2025)]
    if not years: raise ValueError("Nenhum ano com cobertura anterior disponível")
    cache=folder/f"mapbiomas{args.collection}"/grid_hash
    cache.mkdir(parents=True,exist_ok=True)
    if args.check_only:
        for year in years:
            print(f"[VERIFICANDO] Coleção {args.collection}, {year-1}",flush=True)
            remote_raster(URLS[args.collection].format(year=year-1))
        return
    for year in years:
        base=year-1
        path=cache/f"{year}.parquet"
        if path.exists() and not args.refresh:
            print(f"[CACHE] MapBiomas {base}",flush=True);continue
        url=URLS[args.collection].format(year=base)
        print(f"[MAPBIOMAS] {base} → PRODES {year}",flush=True)
        source=remote_raster(url)
        with rasterio.Env(GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR",CPL_VSIL_CURL_USE_HEAD="NO",
                          CPL_VSIL_CURL_ALLOWED_EXTENSIONS=".tif",GDAL_HTTP_MAX_RETRY="3",
                          GDAL_HTTP_RETRY_DELAY="2",GDAL_HTTP_USERAGENT="GeoForestLab/3.0"):
            result=aggregate(grid,source)
        if result.valid_ha.sum()<=0: raise ValueError(f"Raster sem dados válidos: {base}")
        result["year"]=year
        result["cover_year"]=base
        result["collection"]=args.collection
        result["coverage_fraction"]=result.valid_ha/result.sampled_ha.replace(0,np.nan)
        result["source_url"]=url
        result["retrieved_utc"]=datetime.now(timezone.utc).isoformat()
        tmp=path.with_suffix(".tmp")
        result.to_parquet(tmp,index=False);tmp.replace(path)
    all_data=pd.concat([pd.read_parquet(p) for p in sorted(cache.glob("*.parquet"))],ignore_index=True)
    tmp=folder/"environment.tmp"
    all_data.to_parquet(tmp,index=False);tmp.replace(folder/"environment.parquet")
    (folder/"environment_manifest.json").write_text(json.dumps({"collection":args.collection,"grid_sha256":grid_hash,"pixel_m":PIXEL,"forest_codes":FOREST,"open_native_codes":OPEN_NATIVE,"years":sorted(map(int,all_data.year.unique()))},indent=2))
    print("[OK] Cobertura histórica disponível",flush=True)

if __name__=="__main__":
    main()
