import pytest
from landcover_v3 import GROUPS,parse_rows

def test_groups_disjoint():
    codes=[c for _,v in GROUPS.values() for c in v]
    assert len(codes)==len(set(codes))

def test_parse():
    f=[{"properties":{"cell_id":"a","groups":[{"code":1,"sum":12.5},{"code":0,"sum":1}]}}]
    assert parse_rows(f,["a"]).area_ha.sum()==13.5
    with pytest.raises(ValueError):parse_rows(f,["a","b"])
