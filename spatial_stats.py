"""Explicit reproducible permutation tests with rook adjacency and FDR."""
import numpy as np
from scipy.sparse import csr_matrix
from scipy.stats import spearmanr
from statsmodels.stats.multitest import multipletests

def weights(i,j):
    lookup={(int(x),int(y)):k for k,(x,y) in enumerate(zip(i,j))}
    rows=[];cols=[]
    for k,(x,y) in enumerate(zip(i,j)):
        for dx,dy in ((1,0),(-1,0),(0,1),(0,-1)):
            n=lookup.get((x+dx,y+dy))
            if n is not None:
                rows.append(k);cols.append(n)
    w=csr_matrix((np.ones(len(rows)),(rows,cols)),shape=(len(i),len(i)))
    counts=np.asarray(w.sum(axis=1)).ravel()
    return w.multiply(np.divide(1.,counts,out=np.zeros_like(counts),where=counts>0)[:,None]).tocsr(),counts

def moran(values,i,j,permutations=999,seed=42):
    values=np.asarray(values,dtype=float)
    w,counts=weights(i,j)
    active=counts>0
    if active.sum()<3 or np.std(values[active])==0:
        return {"global":None,"local":[],"reason":"Variância nula ou poucos vizinhos"}
    w=w[active][:,active]; v=values[active]; z=(v-v.mean())/v.std()
    lag=w@z; local=z*lag; observed=float(local.mean())
    rng=np.random.default_rng(seed)
    sims=np.empty(permutations); exceed=np.zeros(len(z))
    # Global permutation null; local conditional randomization fixes focal value.
    for k in range(permutations):
        p=rng.permutation(z); sims[k]=np.mean(p*(w@p))
    for k in range(len(z)):
        degree=w.indptr[k+1]-w.indptr[k]
        others=np.delete(z,k)
        simulated=np.array([z[k]*rng.choice(others,degree,replace=False).mean() for _ in range(permutations)])
        center=simulated.mean()
        exceed[k]=np.count_nonzero(np.abs(simulated-center)>=abs(local[k]-center))
    p=(exceed+1)/(permutations+1)
    q=multipletests(p,method="fdr_bh")[1]
    expected=-1/(len(z)-1)
    gp=(1+np.count_nonzero(np.abs(sims-expected)>=abs(observed-expected)))/(permutations+1)
    classes=np.where((z>0)&(lag>0),"HH",np.where((z<0)&(lag<0),"LL",np.where(z>0,"HL","LH")))
    idx=np.flatnonzero(active)
    return {"global":{"I":observed,"expected":expected,"p":float(gp),"n":len(z),"islands":int((~active).sum()),"permutations":permutations,"seed":seed},
            "local":[{"index":int(ix),"I":float(val),"p":float(pv),"q":float(qv),"cluster":str(c) if qv<=.05 else "NS"} for ix,val,pv,qv,c in zip(idx,local,p,q,classes)]}

def association(distance, intensity):
    x=np.asarray(distance); y=np.asarray(intensity)
    valid=np.isfinite(x)&np.isfinite(y)
    x=x[valid];y=y[valid]
    if len(x)<3 or np.std(x)==0 or np.std(y)==0:
        return {"rho":None,"n":len(x)}
    return {"rho":float(spearmanr(x,y).statistic),"n":len(x),"note":"Descritivo; p-valor convencional omitido por dependência espacial."}
