"""
Fetch reference geometry from 'Norges maritime grenser' (Kartverket's official
Norwegian maritime-boundaries dataset, served by Geonorge).

This is *reference geometry* — static legal-boundary polygons (Jan Mayen
fisheries zone, Norwegian EEZ, territorial sea, etc.) — not spatio-temporal
observation data. The emitted GeoJSON files are intended to be consumed via
the `region` argument of DataSource.execute() on observation sources.

Usage:
    from pathlib import Path
    from collekt.reference.geonorge_maritime import fetch

    paths = fetch(layer="Fiskerisone", output_dir=Path("data/geonorge"))
    # → [Path("data/geonorge/extracted/fiskerisone.geojson")]
"""
from __future__ import annotations

import logging
import zipfile
from pathlib import Path

import geopandas as gpd
import requests
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

# 'Norges maritime grenser' — Kartverket. 22 layers including Fiskerisone
# (Jan Mayen), Fiskevernsone (Svalbard), NorgesØkonomiskeSone, etc.
DATASET_UUID = "e106adf4-c9d8-4fce-a9b5-7886a4126d23"
API = "https://nedlasting.geonorge.no/api"
CAPABILITIES_URL = f"{API}/capabilities/{DATASET_UUID}"
ORDER_URL = f"{API}/order"

DEFAULT_LAYER = "Fiskerisone"  # Jan Mayen fisheries zone — demo example


class Credentials(BaseSettings):
    """Geonorge BAAT/GeoID credentials.

    Norges maritime grenser is open data, so username/password are optional —
    the order endpoint accepts anonymous requests. The slot exists so the
    same module can later fetch restricted Geonorge datasets (FKB, Matrikkel)
    by populating GEONORGE_USERNAME / GEONORGE_PASSWORD. email is required
    by the order endpoint regardless.
    """
    username: str | None = None
    password: str | None = None
    email: str
    model_config = SettingsConfigDict(
        env_file='.env',
        env_nested_delimiter='__',
        env_prefix='GEONORGE_',
        extra='ignore',
    )


def _pick(items: list[dict], name_contains: str) -> dict:
    """Choose a codelist entry by case-insensitive substring match on 'name'."""
    for it in items:
        if name_contains.lower() in (it.get("name") or "").lower():
            return it
    raise KeyError(
        f"No codelist entry containing {name_contains!r}. "
        f"Available: {[i.get('name') for i in items]}"
    )


def _safe_name(s: str) -> str:
    """Filesystem-safe lowercase: ø→oe, å→aa, æ→ae, spaces→underscores."""
    return (s.lower()
             .replace("ø", "oe").replace("å", "aa").replace("æ", "ae")
             .replace(" ", "_"))


def list_layers(gml_path: Path) -> list[str]:
    """All layer names available in the GML file."""
    import pyogrio
    return [name for name, _ in pyogrio.list_layers(gml_path)]


def _place_order(email: str, area: dict, projection: dict, fmt: dict,
                 auth=None) -> dict:
    payload = {
        "email": email,
        "orderLines": [{
            "metadataUuid": DATASET_UUID,
            "areas":       [area],
            "projections": [projection],
            "formats":     [fmt],
        }],
    }
    r = requests.post(ORDER_URL, auth=auth, json=payload, timeout=60)
    r.raise_for_status()
    return r.json()


def _download_files(order: dict, out_dir: Path, auth=None) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for f in (order.get("files") or []):
        url  = f["downloadUrl"]
        name = f.get("name") or url.rsplit("/", 1)[-1]
        dest = out_dir / name
        with requests.get(url, auth=auth, stream=True, timeout=300) as r:
            r.raise_for_status()
            with dest.open("wb") as fh:
                for chunk in r.iter_content(64 * 1024):
                    fh.write(chunk)
        paths.append(dest)
    return paths


def _unzip_all(paths: list[Path], out_dir: Path) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    out = []
    for p in paths:
        if p.suffix.lower() == ".zip":
            with zipfile.ZipFile(p) as z:
                z.extractall(out_dir)
                out.extend(out_dir / n for n in z.namelist())
        else:
            out.append(p)
    return out


def _download_geonorge(work_dir: Path) -> list[Path]:
    creds = Credentials()
    auth = (creds.username, creds.password) if creds.username and creds.password else None

    logger.info("Fetching capabilities for Norges maritime grenser")
    caps = requests.get(CAPABILITIES_URL, timeout=30).json()
    logger.debug("Capabilities: %s", caps)

    formats     = requests.get(f"{API}/codelists/format/{DATASET_UUID}",     timeout=30).json()
    projections = requests.get(f"{API}/codelists/projection/{DATASET_UUID}", timeout=30).json()
    areas       = requests.get(f"{API}/codelists/area/{DATASET_UUID}",       timeout=30).json()

    fmt  = _pick(formats,     "GML")
    proj = _pick(projections, "EUREF89")
    area = _pick(areas,       "Hele landet")
    logger.info("Ordering: format=%s, projection=%s, area=%s",
                fmt['name'], proj['name'], area['name'])

    order = _place_order(creds.email, area, proj, fmt, auth=auth)
    raw   = _download_files(order, work_dir / "raw", auth=auth)
    return _unzip_all(raw, work_dir / "unzipped")


def fetch(
    layer: str = DEFAULT_LAYER,
    output_dir: Path = Path("data/geonorge"),
    force_refresh: bool = False,
) -> list[Path]:
    """Fetch one (or all) layers of Norges maritime grenser as GeoJSON.

    Args:
        layer: GML layer name (case-insensitive), or "all" for every layer.
            Default: 'Fiskerisone' — Jan Mayen fisheries zone.
        output_dir: Root for raw download, unzipped GML, and extracted GeoJSON.
            Creates output_dir/raw, output_dir/unzipped, output_dir/extracted.
        force_refresh: Re-order from Geonorge even if a cached GML exists.

    Returns:
        Paths to the written GeoJSON file(s) in output_dir/extracted.
    """
    cache_dir = output_dir / "unzipped"
    if not force_refresh and any(cache_dir.glob("*.gml")):
        logger.info("Using cached GML in %s", cache_dir)
        files = list(cache_dir.iterdir())
    else:
        files = _download_geonorge(output_dir)

    gml_path = next((p for p in files if p.suffix.lower() == ".gml"), None)
    if gml_path is None:
        raise RuntimeError(f"No GML file in {[p.name for p in files]}")

    available = list_layers(gml_path)
    if layer == "all":
        layers = available
    else:
        matches = [n for n in available if n.lower() == layer.lower()]
        if not matches:
            raise KeyError(
                f"Layer {layer!r} not found. "
                f"Available ({len(available)}): {available}"
            )
        layers = matches

    extracted_dir = output_dir / "extracted"
    extracted_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for lyr in layers:
        gdf = gpd.read_file(gml_path, layer=lyr).to_crs("EPSG:4326")
        out = extracted_dir / f"{_safe_name(lyr)}.geojson"
        gdf.to_file(out, driver="GeoJSON")
        logger.info("Wrote %d feature(s) -> %s", len(gdf), out)
        written.append(out)
    return written
