import geopandas as gpd
import pytest
from shapely.geometry import box
from pipeline_v2 import AREA
from territory_v3 import partition,annual_by_zone

def test_disjoint_protection_and_residual():
    grid=gpd.GeoDataFrame({"cell_id":["a"]},geometry=[box(0,0,1000,1000)],crs=AREA)
    ti=gpd.GeoDataFrame(geometry=[box(0,0,600,1000),box(0,0,200,1000)],crs=AREA)
    uc=gpd.GeoDataFrame(geometry=[box(400,0,800,1000)],crs=AREA)
    residual=gpd.GeoDataFrame(geometry=[box(0,0,100,1000)],crs=AREA)
    cov,zones=partition(grid,ti,uc,residual)
    assert cov.iloc[0].protected_ha==pytest.approx(80)
    assert cov.iloc[0].overlap_ha==pytest.approx(20)
    assert cov.iloc[0].residual_ha==pytest.approx(10)
    assert zones.groupby("group").territory_ha.sum().to_dict()==pytest.approx({"indigenous_only":40,"conservation_only":20,"both":20,"outside":20})
    f=gpd.GeoDataFrame({"year":[2020]},geometry=[box(350,0,850,1000)],crs=AREA)
    annual=annual_by_zone(f,zones)
    assert annual.area_ha.sum()==pytest.approx(50)
    assert annual.set_index("group").area_ha.to_dict()==pytest.approx({"indigenous_only":5,"both":20,"conservation_only":20,"outside":5})
