"""Pipeline v2: reproducible, local inputs or configured HTTPS archives."""
import argparse
import hashlib
import json
import shutil
import tempfile
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import pandas as pd
import duckdb
from shapely import make_valid
from pyproj import CRS

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "data" / "v2"
DIST = CRS.from_proj4("+proj=aeqd +lat_0=2 +lon_0=-61 +datum=WGS84 +units=m +no_defs")
AREA = CRS.from_proj4("+proj=laea +lat_0=2 +lon_0=-61 +datum=WGS84 +units=m +no_defs")
RANGES = [5, 10, 20]

def digest(path):
    with open(path, "rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()

def acquire(spec, name):
    if isinstance(spec, str):
        path = ROOT / spec
        if not path.exists():
            raise FileNotFoundError(path)
        files = sorted(path.parent.glob(path.stem + ".*")) if path.suffix == ".shp" else [path]
        return path, {"path": str(path), "sha256": {p.name: digest(p) for p in files}}
    if spec.get("wfs"):
        from download_prodes_v2 import acquire_prodes
        return acquire_prodes(spec, ROOT / "data" / "downloads_v2" / name)
    url = spec["url"]
    if not url.startswith("https://"):
        raise ValueError("Use HTTPS para fontes remotas")
    cache = ROOT / "data" / "downloads_v2" / name
    cache.mkdir(parents=True, exist_ok=True)
    receipt = cache / "receipt.json"
    if receipt.exists():
        saved = json.loads(receipt.read_text(encoding="utf-8"))
        path = cache / saved["sha256"] / saved["member"]
        if saved["url"] == url and path.is_file() and (not spec.get("sha256") or spec["sha256"] == saved["sha256"]):
            print(f"[CACHE] {name}", flush=True)
            return path, saved
    print(f"[DOWNLOAD] {name}: {url}", flush=True)
    with tempfile.TemporaryDirectory(dir=cache) as tmp:
        archive = Path(tmp) / "source.zip"
        with urllib.request.urlopen(url, timeout=120) as response, archive.open("wb") as f:
            shutil.copyfileobj(response, f)
        sha = digest(archive)
        if spec.get("sha256") and sha != spec["sha256"]:
            raise ValueError(f"Checksum incorreto: {name}")
        target = cache / sha
        if not target.exists():
            staging = Path(tmp) / "extracted"
            staging.mkdir()
            with zipfile.ZipFile(archive) as z:
                for entry in z.infolist():
                    dest = (staging / entry.filename).resolve()
                    if not dest.is_relative_to(staging.resolve()):
                        raise ValueError("Arquivo ZIP inseguro")
                z.extractall(staging)
            staging.rename(target)
    matches = list(target.rglob(spec["member"]))
    if len(matches) != 1:
        raise ValueError(f"Arquivo esperado no ZIP: {spec['member']}; encontrados: {matches}")
    path = matches[0]
    if not path.resolve().is_relative_to(target.resolve()) or not path.is_file():
        raise ValueError(f"Arquivo ausente no ZIP: {name}")
    saved = {"url": url, "sha256": sha, "member": str(path.relative_to(target))}
    receipt.write_text(json.dumps(saved), encoding="utf-8")
    return path, saved

def read(path):
    df = gpd.read_file(path)
    if df.crs is None:
        raise ValueError(f"CRS ausente: {path}")
    df = df[df.geometry.notna() & ~df.geometry.is_empty].copy()
    df.geometry = make_valid(df.geometry.array)
    return df

def polygons(df):
    df = df.explode(index_parts=False, ignore_index=True)
    return df[df.geom_type.isin(["Polygon", "MultiPolygon"]) & ~df.geometry.is_empty].copy()

def hectares(df):
    return df.to_crs(AREA).geometry.area / 10000

def rings_from_roads(roads, aoi, distances=RANGES):
    boundary = aoi.geometry.union_all()
    previous = boundary.difference(boundary)
    rows = []
    for low, high in zip([0] + distances[:-1], distances):
        parts = []
        for i in range(0, len(roads), 5000):
            parts.append(roads.geometry.iloc[i:i+5000].buffer(high*1000).union_all())
        current = gpd.GeoSeries(parts, crs=aoi.crs).union_all().intersection(boundary)
        current = current.union(previous)
        rows.append({"ring_id": f"{low}-{high}km", "geometry": current.difference(previous)})
        previous = current
    rows.append({"ring_id": f">{distances[-1]}km", "geometry": boundary.difference(previous)})
    return polygons(gpd.GeoDataFrame(rows, crs=aoi.crs))

def run(config):
    inputs, provenance = {}, {}
    for name in ("municipalities", "roads", "prodes"):
        inputs[name], provenance[name] = acquire(config[name], name)
    muni = read(inputs["municipalities"])
    if not {"CD_MUN","NM_MUN"}.issubset(muni.columns):
        raise ValueError("Malha municipal precisa de CD_MUN e NM_MUN")
    muni = muni[muni.CD_MUN.astype(str).str.startswith("14")][["CD_MUN","NM_MUN","geometry"]]
    muni = polygons(muni.to_crs(DIST)).dissolve(by=["CD_MUN","NM_MUN"]).reset_index()
    if len(muni) != 15:
        raise ValueError("Esperados os 15 municípios de Roraima")
    aoi = muni.dissolve()[["geometry"]]
    roads = read(inputs["roads"]).to_crs(DIST)
    classes = config.get("road_classes")
    if classes:
        roads = roads[roads["fclass"].isin(classes)]
    roads = roads.explode(index_parts=False, ignore_index=True)
    roads = roads[roads.geom_type.isin(["LineString","MultiLineString"])]
    halo = aoi.buffer(max(RANGES)*1000)
    roads = gpd.clip(roads, halo)
    if roads.empty:
        raise ValueError("Nenhuma estrada na área de processamento")
    rings = rings_from_roads(roads, aoi)
    prodes = polygons(read(inputs["prodes"]).to_crs(DIST))
    year = next((c for c in prodes if c.lower() in ("year","ano")), None)
    if year is None:
        raise ValueError("Coluna de ano ausente")
    prodes = prodes.rename(columns={year:"year"})[["year","geometry"]]
    prodes["year"] = pd.to_numeric(prodes.year, errors="raise").astype(int)
    prodes = gpd.clip(prodes, aoi)
    fragments = gpd.overlay(prodes, rings, how="intersection", keep_geom_type=True)
    fragments = gpd.overlay(fragments, muni, how="intersection", keep_geom_type=True)
    fragments["area_ha"] = hectares(fragments)
    fragments = fragments[fragments.area_ha > 0].copy()
    zones = gpd.overlay(rings, muni, how="intersection", keep_geom_type=True)
    zones["zone_ha"] = hectares(zones)
    expected = hectares(prodes).sum()
    actual = fragments.area_ha.sum()
    if abs(expected-actual) > max(0.01, expected*0.0001):
        raise ValueError(f"Falha de conservação de área: {expected} vs {actual}")
    OUT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=OUT) as tmp:
        stage = Path(tmp)
        for name, df in [("fragments", fragments), ("zones", zones), ("municipalities", muni), ("roads", roads)]:
            df.to_crs(4326).to_parquet(stage / f"{name}.parquet", index=False)
        with duckdb.connect(str(stage / "research.duckdb")) as con:
            con.register("f", pd.DataFrame(fragments.drop(columns="geometry")))
            con.register("z", pd.DataFrame(zones.drop(columns="geometry")))
            con.execute("CREATE TABLE annual AS SELECT CD_MUN,NM_MUN,ring_id,year,SUM(area_ha) AS area_ha FROM f GROUP BY ALL")
            con.execute("CREATE TABLE zones AS SELECT CD_MUN,NM_MUN,ring_id,SUM(zone_ha) AS zone_ha FROM z GROUP BY ALL")
        manifest = {"created_utc": datetime.now(timezone.utc).isoformat(), "sources": provenance,
                    "road_classes": classes, "distances_km": RANGES, "distance_crs": DIST.to_wkt(),
                    "area_crs": AREA.to_wkt(), "source_area_ha": float(expected), "fragment_area_ha": float(actual),
                    "road_coverage_note": config.get("road_coverage_note", "Cobertura externa não verificada")}
        (stage / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        for p in stage.iterdir():
            p.replace(OUT / p.name)
    print(f"Concluído: {actual:,.2f} ha; resultados em {OUT}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="research_v2.auto.json")
    args = parser.parse_args()
    run(json.loads(Path(args.config).read_text(encoding="utf-8")))
