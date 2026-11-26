import numpy as np
import pandas as pd
import geopandas as gpd
import pytest
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import box
from pipeline_v2 import AREA
from environment_v3 import aggregate

def test_raster_aggregation(tmp_path):
    path=tmp_path/"cover.tif"
    with rasterio.open(path,"w",driver="GTiff",height=2,width=2,count=1,dtype="uint8",crs=AREA,transform=from_origin(0,60,30,30),nodata=0) as dst:
        dst.write(np.array([[3,4],[0,15]],dtype="uint8"),1)
    grid=gpd.GeoDataFrame({"cell_id":["a"]},geometry=[box(0,0,60,60)],crs=AREA)
    r=aggregate(grid,path).iloc[0]
    assert r.sampled_ha==pytest.approx(.36)
    assert r.valid_ha==pytest.approx(.27)
    assert r.forest_ha==pytest.approx(.09)
    assert r.open_native_ha==pytest.approx(.09)
