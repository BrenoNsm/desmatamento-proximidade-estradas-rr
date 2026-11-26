import numpy as np
import pytest
from spatial_stats import weights,moran,association
def test_rook_weights():
    w,n=weights([0,0,1,1,9],[0,1,0,1,9])
    assert list(n)==[2,2,2,2,0]
    assert np.allclose(w.sum(axis=1).A.ravel(),[1,1,1,1,0])
def test_moran_reproducible():
    x=[1,2,9,10,0]
    a=moran(x,[0,0,1,1,9],[0,1,0,1,9],99)
    b=moran(x,[0,0,1,1,9],[0,1,0,1,9],99)
    assert a==b
    assert a["global"]["n"]==4
    assert a["global"]["islands"]==1
    assert all(0<v["p"]<=1 and 0<v["q"]<=1 for v in a["local"])
def test_constant():
    assert moran([1,1,1],[0,1,2],[0,0,0],99)["global"] is None
    assert association([1,2,3],[3,2,1])["rho"]==pytest.approx(-1)
