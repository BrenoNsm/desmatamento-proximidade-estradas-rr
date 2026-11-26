import geopandas as gpd
import pytest
from shapely.geometry import box, LineString
from pipeline_v2 import DIST, rings_from_roads, hectares

def test_rings_partition_and_area():
    aoi = gpd.GeoDataFrame(geometry=[box(0,0,40000,40000)], crs=DIST)
    roads = gpd.GeoDataFrame(geometry=[LineString([(-1000,0),(-1000,40000)])], crs=DIST)
    rings = rings_from_roads(roads, aoi)
    assert rings.geometry.union_all().symmetric_difference(aoi.geometry.iloc[0]).area < 1
    assert rings.geometry.area.sum() == pytest.approx(aoi.geometry.area.sum(), rel=1e-8)
    assert rings.loc[rings.ring_id=="0-5km"].geometry.area.sum() > 0

def test_municipal_split():
    p = gpd.GeoDataFrame(geometry=[box(0,0,1000,1000)], crs=DIST)
    m = gpd.GeoDataFrame({"CD_MUN":["A","B"]},geometry=[box(0,0,500,1000),box(500,0,1000,1000)],crs=DIST)
    result = gpd.overlay(p,m,how="intersection")
    assert hectares(result).sum() == pytest.approx(hectares(p).sum(), rel=1e-5)
    assert len(result) == 2
