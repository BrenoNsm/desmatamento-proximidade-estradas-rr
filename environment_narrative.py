"""Panorama: reproducible exploratory scenarios and cached NVIDIA interpretation."""
import argparse
import hashlib
import json
import os
import time
import fcntl
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

ROOT=Path(__file__).resolve().parent
MODEL="deepseek-ai/deepseek-v4.1-flash"
ENDPOINT="https://integrate.api.nvidia.com/v1/chat/completions"
VERSION=1

def scenarios(info):
    years=sorted(info["manifest"]["years"])
    if len(years)<3 or any(b-a!=1 for a,b in zip(years,years[1:])):
        return {"available":False,"reason":"São necessários pelo menos três anos consecutivos."}
    codes=sorted(int(c) for c in info["manifest"]["groups"])
    values={(r["year"],r["code"]):float(r["area_ha"]) for r in info["annual"]}
    totals=[sum(values.get((y,c),0) for c in codes) for y in years]
    if min(totals)<=0:return {"available":False,"reason":"Área amostrada inválida."}
    # Project shares rather than raw area; keep unknown/no-data groups fixed.
    shares={c:[values.get((y,c),0)/t for y,t in zip(years,totals)] for c in codes}
    known=[c for c in codes if c not in (0,10)]
    valid=1-sum(shares[c][-1] for c in codes if c not in known)
    slopes={c:(shares[c][-1]-shares[c][0])/(years[-1]-years[0]) for c in known}
    mean=sum(slopes.values())/len(known)
    slopes={c:v-mean for c,v in slopes.items()}
    rows=[]
    for c in codes:
        for y,t,v in zip(years,totals,shares[c]):
            rows.append({"code":c,"year":y,"observed":v*t,"stable":None,"trend":None,"accelerated":None})
        rows[-1].update(stable=shares[c][-1]*totals[-1],trend=shares[c][-1]*totals[-1],accelerated=shares[c][-1]*totals[-1])
    for step in range(1,4):
        estimates={}
        for name,factor in (("stable",0),("trend",1),("accelerated",1.5)):
            raw={c:max(0,shares[c][-1]+step*factor*slopes[c]) for c in known}
            total=sum(raw.values())
            estimates[name]={c:(raw[c]*valid/total if total>0 else shares[c][-1]) if c in known else shares[c][-1] for c in codes}
        for c in codes:
            rows.append({"code":c,"year":years[-1]+step,"observed":None,**{k:v[c]*totals[-1] for k,v in estimates.items()}})
    return {"available":True,"base_year":years[-1],"rows":rows,"method":"Extrapolação linear da participação entre primeiro e último ano, centrada para conservar a área reconhecida; truncamento em zero e renormalização. Cenários de estabilidade, tendência e tendência 50% mais intensa. Área total e parcela desconhecida fixadas no último ano. Não são intervalos de confiança nem probabilidades."}

def payload(info,resolution):
    m=info["manifest"]
    return {"territory":"Roraima","resolution_km":resolution,"source":"MapBiomas C11","years":m["years"],"transition":m["transition"],"groups":m["groups"],"annual":sorted(info["annual"],key=lambda v:(v["year"],v["code"])),"transitions":sorted(info["transitions"],key=lambda v:(v["from_code"],v["to_code"])),"asset":m["asset"],"method":m.get("method"),"scenarios":scenarios(info)}

def fingerprint(data):
    return hashlib.sha256(json.dumps(data,sort_keys=True,ensure_ascii=False,separators=(",",":"),allow_nan=False).encode()).hexdigest()

def read_analysis(info,resolution):
    if not info["available"]:return {"status":"unavailable"}
    data=payload(info,resolution)
    digest=fingerprint(data)
    p=ROOT/"data/v3"/str(resolution)/"landcover"/"narratives"/(digest+".json")
    base={"fingerprint":digest,"scenarios":data["scenarios"]}
    if not p.exists():return {"status":"pending",**base}
    return {"status":"ready",**base,**json.loads(p.read_text())}

def generate(info,resolution):
    data=payload(info,resolution)
    digest=fingerprint(data)
    folder=ROOT/"data/v3"/str(resolution)/"landcover"/"narratives"
    folder.mkdir(parents=True,exist_ok=True)
    target=folder/(digest+".json")
    lockpath=ROOT/"data"/"nvidia-rate.lock"
    with lockpath.open("a+") as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        if target.exists():return read_analysis(info,resolution)
        key=os.environ.get("NVIDIA_API_KEY")
        if not key:raise RuntimeError("Defina NVIDIA_API_KEY no terminal")
        lock.seek(0);raw=lock.read().strip()
        time.sleep(max(0,3-(time.time()-(float(raw) if raw else 0))))
        lock.seek(0);lock.truncate();lock.write(str(time.time()));lock.flush();os.fsync(lock.fileno())
        model=os.environ.get("NVIDIA_MODEL",MODEL)
        system="""Interprete estatísticas ambientais para público geral em português brasileiro. Use exclusivamente o JSON fornecido. Produza quatro parágrafos curtos de texto puro: paisagem no ano recente; mudanças entre extremos; explicação dos cenários futuros calculados e suas hipóteses; limitações e como interpretar. Diferencie estoque, transição e cenário. Não invente números, causas, municípios, previsões ou probabilidades. Não chame toda mudança de desmatamento. Indique denominadores de percentuais. Grupos analíticos sujeitos a validação; códigos 0 e 10 não reconhecidos. Não infira causalidade ou legalidade. Três anos não validam previsão."""
        body={"model":model,"messages":[{"role":"system","content":system},{"role":"user","content":json.dumps(data,ensure_ascii=False)}],"temperature":0.2,"max_tokens":4096,"stream":False}
        req=Request(ENDPOINT,data=json.dumps(body).encode(),headers={"Authorization":"Bearer "+key,"Content-Type":"application/json"})
        try:
            with urlopen(req,timeout=180) as response:result=json.load(response)
        except HTTPError as e:raise RuntimeError(f"NVIDIA HTTP {e.code}; nenhuma repetição automática. Verifique acesso ao modelo e cotas.") from None
        except (URLError,TimeoutError):raise RuntimeError("Falha de conexão com NVIDIA; nenhuma repetição automática.") from None
        choice=result["choices"][0];text=choice["message"].get("content")
        if choice.get("finish_reason")=="length" or not isinstance(text,str) or not text.strip():raise RuntimeError("Resposta vazia ou truncada; não armazenada")
        saved={"fingerprint":digest,"model":model,"created_utc":datetime.now(timezone.utc).isoformat(),"prompt_version":VERSION,"text":text.strip(),"years":data["years"]}
        tmp=target.with_suffix(".tmp");tmp.write_text(json.dumps(saved,ensure_ascii=False,indent=2),encoding="utf-8");tmp.replace(target)
        return {"status":"ready",**saved}

def main():
    p=argparse.ArgumentParser();p.add_argument("--cell-km",type=int,default=5);a=p.parse_args()
    from api_v3 import landcover
    info=landcover(a.cell_km)
    if not info["available"]:p.error("MapBiomas não processado")
    r=generate(info,a.cell_km);print("[OK] Análise disponível em cache:",r["fingerprint"])

if __name__=="__main__":main()
