"""Download regional PRODES via WFS, without silently accepting partial pages."""
import hashlib
import json
import time
import urllib.parse
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

def sha(path):
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()

def request_wfs(endpoint, params, attempts=6):
    url = endpoint + "?" + urllib.parse.urlencode(params)
    for attempt in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"User-Agent":"GeoForestLab/3.0"})
            with urllib.request.urlopen(req, timeout=180) as r:
                return r.read()
        except OSError as exc:
            if isinstance(exc, urllib.error.HTTPError) and exc.code not in (408,429,500,502,503,504):
                raise
            if attempt == attempts-1:
                raise RuntimeError(f"INPE WFS indisponível após {attempts} tentativas: {params.get('typeNames','catálogo')}. Último erro: {exc}. Execute novamente mais tarde; os caches concluídos foram preservados.") from exc
            delay = min(60, 5 * 2**attempt)
            print(f"[INPE] {exc}; nova tentativa {attempt+2}/{attempts} em {delay}s", flush=True)
            time.sleep(delay)

def acquire_prodes(spec, cache):
    cache.mkdir(parents=True, exist_ok=True)
    path = cache / "prodes_rr.geojson"
    receipt = cache / "wfs_receipt.json"
    if path.exists() and receipt.exists():
        saved = json.loads(receipt.read_text(encoding="utf-8"))
        if saved.get("spec") == spec and saved.get("sha256") == sha(path):
            print(f"[CACHE] {spec['layer']}", flush=True)
            return path, saved

    def request(params):
        return request_wfs(spec["wfs"], params)

    base = {"service":"WFS", "version":"2.0.0", "request":"GetFeature",
            "typeNames":spec["layer"], "srsName":"EPSG:4326",
            "bbox":"-65,-2.5,-58,6,urn:ogc:def:crs:OGC:1.3:CRS84"}
    hits = ET.fromstring(request(dict(base, resultType="hits")))
    if hits.tag.split("}")[-1] != "FeatureCollection":
        raise RuntimeError(f"INPE WFS retornou erro: {ET.tostring(hits, encoding='unicode')[:500]}")
    total = int(hits.attrib["numberMatched"])
    if total <= 0:
        raise RuntimeError("INPE WFS retornou zero feições")
    print(f"[DOWNLOAD] {spec['layer']}: {total} feições na região", flush=True)
    features, ids = [], set()
    while len(features) < total:
        page = json.loads(request(dict(base, outputFormat="application/json",
                                       count=1000, startIndex=len(features))))
        batch = page["features"]
        if not batch:
            raise RuntimeError("INPE WFS interrompeu a paginação")
        for feature in batch:
            key = feature.get("id")
            if key is None or key in ids:
                raise RuntimeError("Paginação WFS sem IDs únicos; não é seguro prosseguir")
            ids.add(key)
        features.extend(batch)
        print(f"[WFS] {len(features)}/{total}", flush=True)
    if len(features) != total:
        raise RuntimeError("Quantidade WFS divergente")
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps({"type":"FeatureCollection","features":features}), encoding="utf-8")
    temp.replace(path)
    saved = {"spec":spec, "sha256":sha(path), "features":total,
             "retrieved_utc":datetime.now(timezone.utc).isoformat()}
    receipt.write_text(json.dumps(saved), encoding="utf-8")
    return path, saved
