"""Panorama: reproducible exploratory scenarios and cached NVIDIA interpretation."""
import argparse
import hashlib
import json
import os
import time
import fcntl
import math
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

ROOT=Path(__file__).resolve().parent
MODEL="nvidia/nemotron-3.5-lightning-30b-a3b"
ENDPOINT="https://integrate.api.nvidia.com/v1/chat/completions"
VERSION=3
SKILL=ROOT/'skills/humanize-portugues-br/SKILL.md'

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

def monitoring_context(resolution, grid_sha256):
    """Use local observations only; exclude timestamps unrelated to data content."""
    from statistics import mean
    path=ROOT/"data/v3"/str(resolution)/"monitoring"/"monitoring.json"
    if not path.exists():
        return {"available":False,"reason":"Monitoramento mensal ainda não processado."}
    doc=json.loads(path.read_text(encoding="utf-8"))
    if doc.get("grid_sha256")!=grid_sha256:
        return {"available":False,"reason":"Monitoramento pertence a outra versão da grade."}
    result={"available":True,"sources":{}}
    for kind in ("rain","fire"):
        source=doc.get("sources",{}).get(kind)
        if not source:continue
        rows=[]
        for row in sorted(source["rows"],key=lambda r:r["month"]):
            v=row.get("value")
            if v is not None and (type(v) not in (int,float) or not math.isfinite(v) or v<0):
                raise ValueError("Valor de monitoramento inválido: "+kind)
            rows.append({k:row.get(k) for k in ("month","start","end_exclusive","days_available","days_expected","complete","value")})
        complete=[r for r in rows if r["complete"] is True and r["value"] is not None]
        season=[]
        for month in range(1,13):
            sample=[r["value"] for r in complete if int(r["month"][5:7])==month]
            if sample:season.append({"month":month,"mean":mean(sample),"n_years":len(sample)})
        annual=[]
        for year in sorted({r["month"][:4] for r in rows}):
            sample=[r for r in complete if r["month"][:4]==year]
            if len({r["month"] for r in sample})==12:
                annual.append({"year":int(year),"total":sum(r["value"] for r in sample)})
        latest=next((r for r in reversed(rows) if r["value"] is not None),None)
        comparable=[]
        if latest:
            year=latest["month"][:4]
            common={r["month"][5:7] for r in complete if r["month"][:4]==year}
            if common:
                for y in sorted({r["month"][:4] for r in complete}):
                    sample=[r for r in complete if r["month"][:4]==y and r["month"][5:7] in common]
                    if {r["month"][5:7] for r in sample}==common:
                        comparable.append({"year":int(y),"months":sorted(common),"total":sum(r["value"] for r in sample)})
        result["sources"][kind]={
            "asset":source["asset"],"unit":source["unit"],"catalog_last_date":source["last_date"],
            "monthly":rows,"seasonal_mean_complete_months":season,"complete_annual_totals":annual,
            "same_month_comparison":comparable,"latest_available_month":latest,
            "highest_complete_month":max(complete,key=lambda r:r["value"]) if complete else None,
            "lowest_complete_month":min(complete,key=lambda r:r["value"]) if complete else None,
            "limitation":"Dias presentes no catálogo não garantem observação integral local. Meses incompletos não são comparáveis diretamente com completos. "
            +("Pixel-dias de fogo ativo, não hectares queimados nem incêndios únicos." if kind=="fire" else "Precipitação acumulada média espacial estimada, não pluviômetros individuais.")
        }
    if not result["sources"]:return {"available":False,"reason":"Nenhuma fonte de monitoramento disponível."}
    return result

def payload(info,resolution):
    m=info["manifest"]
    return {"territory":"Roraima","resolution_km":resolution,"source":"MapBiomas C11","years":m["years"],"transition":m["transition"],"groups":m["groups"],"annual":sorted(info["annual"],key=lambda v:(v["year"],v["code"])),"transitions":sorted(info["transitions"],key=lambda v:(v["from_code"],v["to_code"])),"asset":m["asset"],"method":m.get("method"),"scenarios":scenarios(info),"monitoring":monitoring_context(resolution,m["grid_sha256"])}

def fingerprint(data):
    return hashlib.sha256(json.dumps(data,sort_keys=True,ensure_ascii=False,separators=(",",":"),allow_nan=False).encode()).hexdigest()

def parse_output(text,data):
    raw=text.strip()
    if raw.startswith("```"):
        raw=raw.split("\n",1)[1].rsplit("```",1)[0].strip()
    try: obj=json.loads(raw)
    except (ValueError,IndexError):raise RuntimeError("Resposta IA fora do formato JSON; não armazenada") from None
    paragraphs=obj.get("paragraphs") if isinstance(obj,dict) else None
    if not isinstance(paragraphs,list) or len(paragraphs)!=6 or not all(isinstance(p,str) and p.strip() for p in paragraphs):
        raise RuntimeError("Parágrafos inválidos; não armazenados")
    rows=obj.get("forecast")
    years=data["years"]; base=max(years)
    codes={int(c) for c in data["groups"] if int(c) not in (0,10)}
    total=sum(float(r["area_ha"]) for r in data["annual"] if r["year"]==base and r["code"] in codes)
    expected={(c,y) for c in codes for y in range(base+1,base+4)}
    if not isinstance(rows,list):raise RuntimeError("Projeção IA ausente")
    clean=[];seen=set()
    for r in rows:
        if not isinstance(r,dict):raise RuntimeError("Linha IA inválida")
        c,y,v=r.get("code"),r.get("year"),r.get("area_ha")
        if type(c) is not int or type(y) is not int or (c,y) not in expected or (c,y) in seen or type(v) not in (int,float) or not math.isfinite(v) or not 0<=v<=total:
            raise RuntimeError("Projeção IA inválida; não armazenada")
        seen.add((c,y));clean.append({"code":c,"year":y,"area_ha":float(v)})
    if seen!=expected:raise RuntimeError("Projeção IA incompleta")
    for y in range(base+1,base+4):
        if abs(sum(r["area_ha"] for r in clean if r["year"]==y)-total)>max(1,total*.001):
            raise RuntimeError("Projeção IA não conserva área reconhecida (tolerância 0,1%)")
    assumptions=obj.get("assumptions")
    if not isinstance(assumptions,str) or not assumptions.strip():raise RuntimeError("Premissas IA ausentes")
    return "\n\n".join(p.strip() for p in paragraphs),clean,assumptions.strip()

def read_analysis(info,resolution):
    if not info["available"]:return {"status":"unavailable"}
    data=payload(info,resolution)
    digest=fingerprint(data)
    p=ROOT/"data/v3"/str(resolution)/"landcover"/"narratives"/(digest+".json")
    base={"fingerprint":digest,"scenarios":data["scenarios"]}
    if not p.exists():return {"status":"pending",**base}
    saved=json.loads(p.read_text())
    if saved.get("prompt_version")!=VERSION:return {"status":"pending",**base}
    return {"status":"ready",**base,**saved}

def generate(info,resolution):
    data=payload(info,resolution)
    digest=fingerprint(data)
    folder=ROOT/"data/v3"/str(resolution)/"landcover"/"narratives"
    folder.mkdir(parents=True,exist_ok=True)
    target=folder/(digest+".json")
    lockpath=ROOT/"data"/"nvidia-rate.lock"
    with lockpath.open("a+") as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        if target.exists() and read_analysis(info,resolution)['status']=='ready':return read_analysis(info,resolution)
        key=os.environ.get("NVIDIA_API_KEY")
        if not key:raise RuntimeError("Defina NVIDIA_API_KEY no terminal")
        lock.seek(0);raw=lock.read().strip()
        time.sleep(max(0,3-(time.time()-(float(raw) if raw else 0))))
        lock.seek(0);lock.truncate();lock.write(str(time.time()));lock.flush();os.fsync(lock.fileno())
        model=os.environ.get("NVIDIA_MODEL",MODEL)
        system=SKILL.read_text(encoding="utf-8")+"\n\n"+"""Interprete estatísticas ambientais para público geral em português brasileiro. Use exclusivamente o JSON fornecido. Produza seis parágrafos curtos de texto puro, nesta ordem: paisagem no ano recente; mudanças entre extremos; precipitação; fogo ativo; cenários futuros e suas hipóteses; limitações e como interpretar. Nos parágrafos de chuva e fogo, use os resumos calculados em monitoring, com números, unidades e datas efetivamente disponíveis. Compare anos completos ou os mesmos meses completos (same_month_comparison); nunca trate o acumulado parcial como ano completo. Explique a sazonalidade e a situação recente, identificando meses incompletos. Não confunda pixel-dias de detecção com hectares queimados ou incêndios únicos. Não atribua causalidade entre chuva, fogo e uso da terra. Se uma fonte estiver ausente, diga isso explicitamente, sem inventar resultados. Diferencie estoque, transição e cenário. Não invente números, causas, municípios, previsões ou probabilidades. Não chame toda mudança de desmatamento. Indique denominadores de percentuais. Grupos analíticos sujeitos a validação; códigos 0 e 10 não reconhecidos. Não infira causalidade ou legalidade. Uma série anual curta não valida previsão. Os indicadores recentes de chuva e fogo podem contextualizar a hipótese de cobertura, mas não são um modelo causal nem previsão meteorológica. Retorne exclusivamente um objeto JSON com paragraphs (lista de seis strings), assumptions (string explicando premissas do seu próprio cenário) e forecast (lista de objetos code, year, area_ha). Gere uma trajetória própria de IA para cada código reconhecido (exclua 0 e 10) para cada um dos três anos seguintes ao último observado. Use os dados históricos como base e os cenários matemáticos apenas como referência, não copie automaticamente. Preserve a soma das áreas reconhecidas do último ano em cada ano futuro, tolerância 0,1%, valores não negativos. Não invente evidência externa. Identifique sua trajetória como hipótese exploratória sem validação. A estrutura JSON é para a aplicação, não para o leitor."""
        body={"model":model,"messages":[{"role":"system","content":system},{"role":"user","content":json.dumps(data,ensure_ascii=False)}],"temperature":0.2,"max_tokens":8192,"stream":False}
        if model==MODEL:
            body["chat_template_kwargs"]={"enable_thinking":False}
        req=Request(ENDPOINT,data=json.dumps(body).encode(),headers={"Authorization":"Bearer "+key,"Content-Type":"application/json"})
        try:
            with urlopen(req,timeout=180) as response:result=json.load(response)
        except HTTPError as e:raise RuntimeError(f"NVIDIA HTTP {e.code}; nenhuma repetição automática. Verifique acesso ao modelo e cotas.") from None
        except (URLError,TimeoutError):raise RuntimeError("Falha de conexão com NVIDIA; nenhuma repetição automática.") from None
        choice=result["choices"][0];text=choice["message"].get("content")
        if choice.get("finish_reason")=="length" or not isinstance(text,str) or not text.strip():raise RuntimeError("Resposta vazia ou truncada; não armazenada")
        text,forecast,assumptions=parse_output(text,data)
        saved={"forecast":forecast,"assumptions":assumptions,"fingerprint":digest,"model":model,"created_utc":datetime.now(timezone.utc).isoformat(),"prompt_version":VERSION,"text":text.strip(),"years":data["years"],"monitoring_sources":{k:{"last_date":v["catalog_last_date"],"unit":v["unit"]} for k,v in data["monitoring"].get("sources",{}).items()}}
        tmp=target.with_suffix(".tmp");tmp.write_text(json.dumps(saved,ensure_ascii=False,indent=2),encoding="utf-8");tmp.replace(target)
        return {"status":"ready",**saved}

def main():
    p=argparse.ArgumentParser();p.add_argument("--cell-km",type=int,default=5);a=p.parse_args()
    from api_v3 import landcover
    info=landcover(a.cell_km)
    if not info["available"]:p.error("MapBiomas não processado")
    r=generate(info,a.cell_km);print("[OK] Análise disponível em cache:",r["fingerprint"])

if __name__=="__main__":main()
