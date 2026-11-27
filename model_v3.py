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
        "note":"PPML com efeitos fixos anuais, offset log da proxy de disponibilidade florestal t-1, erros agrupados em blocos de 50 km. Associação exploratória; não regressão espacial estrutural nem inferência causal."}

def compare_territorial_models(data,resolution):
    """Compare nested PPML specifications on exactly the same observations."""
    d=data.copy()
    cols=["distance_km","road_density","indigenous_pct","conservation_pct","overlap_ha","territory_ha","forest_ha","area_ha"]
    good=(d.coverage_fraction>=.95)&(d.forest_ha>=1)&(d.territory_ha>0)&(d.area_ha>=0)&np.isfinite(d[cols]).all(axis=1)
    sample=d.loc[good].copy()
    sample["log_distance"]=np.log1p(sample.distance_km)
    sample["log_density"]=np.log1p(sample.road_density)
    # Exclusive fractions avoid double-counting and distinguish overlap.
    overlap=100*sample.overlap_ha/sample.territory_ha
    sample["ti_only_10pp"]=(sample.indigenous_pct-overlap)/10
    sample["uc_only_10pp"]=(sample.conservation_pct-overlap)/10
    sample["both_10pp"]=overlap/10
    sample["block"]=(np.floor(sample.i*resolution/50).astype(int).astype(str)+":"+np.floor(sample.j*resolution/50).astype(int).astype(str))
    blocks=sample.block.nunique()
    if blocks<10 or len(sample)<100 or sample.area_ha.sum()<=0:raise ValueError("Amostra insuficiente para comparação")
    base=["log_distance","log_density"]
    extra=[x for x in ("ti_only_10pp","uc_only_10pp","both_10pp") if sample[x].std()>1e-10]
    if not extra:raise ValueError("Sem variação nas frações territoriais")
    results=[]
    for name,terms in [("M1 — viário",base),("M2 — viário e território",base+extra)]:
        formula="area_ha ~ "+" + ".join(terms)+" + C(year)"
        fit=smf.glm(formula,data=sample,family=sm.families.Poisson(),offset=np.log(sample.forest_ha))
        if np.linalg.matrix_rank(fit.exog)<fit.exog.shape[1]:raise ValueError("Matriz de regressão sem posto completo")
        fit=fit.fit(cov_type="cluster",cov_kwds={"groups":sample.block},maxiter=100)
        if not fit.converged:raise ValueError(f"{name} não convergiu")
        ci=fit.conf_int(); rows=[]
        for term in terms:
            rows.append({"term":term,"beta":float(fit.params[term]),"ratio":float(np.exp(fit.params[term])),
                         "lower":float(np.exp(ci.loc[term,0])),"upper":float(np.exp(ci.loc[term,1])),
                         "p":float(fit.pvalues[term])})
        if not np.isfinite(pd.DataFrame(rows).select_dtypes("number").to_numpy()).all():raise ValueError("Estimativas não finitas")
        results.append({"name":name,"formula":formula,"deviance":float(fit.deviance),"coefficients":rows})
    return {"models":results,"n":len(sample),"blocks":blocks,"excluded":len(d)-len(sample),
            "excluded_area_ha":float(d.loc[~good,"area_ha"].sum()),"years":sorted(map(int,sample.year.unique())),
            "note":"Mesma amostra, offset florestal e efeitos anuais. TI/UC em frações exclusivas por 10 pontos percentuais; referência fora de TI/UC mapeadas. Limites atuais aplicados retrospectivamente: associação descritiva, não efeito de proteção. Residual excluído. Deviance apenas ajuste interno, não desempenho preditivo."}
