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


def test_clip_removes_boundary_contacts_and_preserves_polygon_parts():
    from shapely.geometry import GeometryCollection, Point
    from pipeline_v2 import polygons
    aoi = gpd.GeoDataFrame(geometry=[box(0,0,1000,1000)], crs=DIST)
    source = gpd.GeoDataFrame({"year":[2020,2021,2022]}, geometry=[
        box(100,100,300,300), box(1000,100,1200,300),
        box(1000,1000,1200,1200)], crs=DIST)
    cleaned = polygons(gpd.clip(source,aoi))
    assert cleaned.geom_type.isin(["Polygon","MultiPolygon"]).all()
    assert set(cleaned.year) == {2020}
    mixed = gpd.GeoDataFrame({"year":[2023]}, geometry=[
        GeometryCollection([box(0,0,100,100),LineString([(0,0),(0,200)]),Point(300,300)])], crs=DIST)
    result = polygons(mixed)
    assert len(result) == 1
    assert result.geometry.area.sum() == pytest.approx(10000)
