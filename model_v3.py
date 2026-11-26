"""Exploratory PPML: PRODES hectares, lagged forest proxy, spatial-block clustered SE."""
import numpy as np
import pandas as pd
import statsmodels.api as sm
import statsmodels.formula.api as smf

def fit_model(data, resolution):
    d=data.copy()
    good=(d.coverage_fraction>=.95)&(d.forest_ha>=1)&np.isfinite(d.distance_km)&np.isfinite(d.road_density)
    sample=d.loc[good].copy()
    sample["log_distance"]=np.log1p(sample.distance_km)
    sample["log_density"]=np.log1p(sample.road_density)
    sample["block"]=(np.floor(sample.i*resolution/50).astype(int).astype(str)+":"+np.floor(sample.j*resolution/50).astype(int).astype(str))
    nblocks=sample.block.nunique()
    if nblocks<10 or len(sample)<100 or sample.area_ha.sum()<=0:
        raise ValueError("Amostra insuficiente: requer 100 observações, 10 blocos e incremento positivo")
    fit=smf.glm("area_ha ~ log_distance + log_density + C(year)",data=sample,
        family=sm.families.Poisson(),offset=np.log(sample.forest_ha)).fit(
        cov_type="cluster",cov_kwds={"groups":sample.block},maxiter=100)
    if not fit.converged: raise ValueError("Modelo não convergiu")
    ci=fit.conf_int()
    rows=[]
    for key in ("log_distance","log_density"):
        b=float(fit.params[key]);lo=float(ci.loc[key,0]);hi=float(ci.loc[key,1])
        rows.append({"term":key,"beta":b,"ratio":float(np.exp(b)),"lower":float(np.exp(lo)),
            "upper":float(np.exp(hi)),"p":float(fit.pvalues[key])})
    if not np.isfinite(pd.DataFrame(rows).select_dtypes("number").to_numpy()).all():
        raise ValueError("Estimativas não finitas; revise colinearidade e amostra")
    return {"coefficients":rows,"n":len(sample),"excluded":len(d)-len(sample),"blocks":nblocks,
        "years":sorted(map(int,sample.year.unique())),"deviance":float(fit.deviance),
        "excluded_area_ha":float(d.loc[~good,"area_ha"].sum()),
        "note":"PPML com efeitos fixos anuais, offset log da cobertura florestal MapBiomas t-1, erros agrupados em blocos de 50 km. Associação exploratória; não regressão espacial estrutural nem inferência causal."}
