import geopandas as gpd
import pytest
from shapely.geometry import box
from pipeline_v2 import AREA
from environment_inpe_v3 import reconstruct,resolve

def test_reconstruct_overlaps_and_prior_years():
    grid=gpd.GeoDataFrame({"cell_id":["a"]},geometry=[box(0,0,1000,1000)],crs=AREA)
    domain=grid[["geometry"]]
    masks=gpd.GeoDataFrame(geometry=[box(0,0,200,1000),box(100,0,300,1000)],crs=AREA)
    increments=gpd.GeoDataFrame({"year":[2008,2009]},geometry=[box(300,0,400,1000),box(350,0,450,1000)],crs=AREA)
    r=reconstruct(grid,domain,masks,increments,[2008,2009,2010])
    assert r.forest_ha.tolist()==pytest.approx([70,60,55])

def test_catalogue_requires_all_roles():
    with pytest.raises(ValueError):
        resolve({"x:yearly_deforestation_biome":"annual"}, {})
