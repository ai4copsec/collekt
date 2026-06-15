"""
Geonorge datasource: discover Geonorge datasets covering a query spatial window,
fetch & crop to window, 
then annotate the features with metadata for possible search/filtering.

Divided into phases.
1: discover + relevance
2: WFS end-to-end
3: download/order API (order -> poll -> download -> unzip -> crop)
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import tempfile
import time
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

import geopandas as gpd
import pandas as pd
import pyogrio
import requests
from pydantic_settings import BaseSettings, SettingsConfigDict
from pyproj import Transformer
from shapely.geometry import box as shapely_box

from ..core.datasource import DataSource
from .copernicusmarine import get_coordinates_min_max

logger = logging.getLogger(__name__)
_session = requests.Session()

KARTKATALOG_API = "https://kartkatalog.geonorge.no/api"
DETAIL_PAGE = "https://kartkatalog.geonorge.no/metadata/uuid/{uuid}"

SEARCH_PAGE_SIZE = 50
WFS_PAGE_SIZE = 1000
WFS_MAX_PAGES_PER_TYPE = 20

NEDLASTING_API = "https://nedlasting.geonorge.no/api"
ORDER_PROJECTION = "25833"  # EUREF89 UTM 33, same as reference.geonorge_maritime
ORDER_POLL_SECONDS = 5
ORDER_TIMEOUT_SECONDS = 600
OGR_READABLE_FORMATS = ("GML", "FGDB", "GEOJSON", "GPKG", "SHAPE")

Bbox = tuple[float, float, float, float] # (lon_min, lat_min, lon_max, lat_max), WGS84


class GeonorgeSettings(BaseSettings):
    """GEONORGE_* settings

    email/username/password are only needed by the download/order API.
    buffer_km is the fallback when no buffer is given at construction.
    max_datasets caps fetch candidates per run
    """
    email: str | None = None
    username: str | None = None
    password: str | None = None
    buffer_km: float = 0.0
    max_datasets: int = 20

    model_config = SettingsConfigDict(
                    env_file='.env',
                    env_nested_delimiter='__',
                    env_prefix='GEONORGE_',
                    extra='ignore'
                )


@dataclass
class DatasetRecord:
    """One discovered dataset and what it is turned into."""
    uuid: str
    title: str
    organization: str
    protocol: str
    kind: str
    distribution_url: str | None
    detail_url: str
    coverage_bbox: Bbox | None = None
    is_open: bool = False
    licence_url: str | None = None
    status: str = "candidate"


def window_bbox(latitude: float, longitude: float,
                radius_km: float, buffer_km: float) -> Bbox:
    """Buffered query window as WGS84 bbox"""
    bounds = get_coordinates_min_max(latitude, longitude,
                                     radius_in_km=radius_km + buffer_km)
    return (bounds['lon_min'], bounds['lat_min'],
            bounds['lon_max'], bounds['lat_max'])


def bbox_intersects(a: Bbox, b: Bbox) -> bool:
    return a[0] <= b[2] and b[0] <= a[2] and a[1] <= b[3] and b[1] <= a[3]


def _safe_name(s: str) -> str:
    """Filesystem-safe lowercase (mirrors reference.geonorge_maritime."""
    return (s.lower()
             .replace("ø", "oe").replace("å", "aa").replace("æ", "ae")
             .replace(" ", "_").replace("/", "_").replace(":", "_"))


def classify(protocol: str) -> Literal["wfs", "download", "skip"]:
    """The only protocol dispatch point.

    OGC:WFS is bbox-clippable server-side; GEONORGE:DOWNLOAD goes through the
    order API (P3); everything else (WMS, services, APIs) is skipped.
    """
    if protocol == "OGC:WFS":
        return "wfs"
    if protocol == "GEONORGE:DOWNLOAD":
        return "download"
    return "skip"


def search_datasets(text: str | None) -> list[dict]:
    """All search hits (paged).

    WFS access is catalogued on service-type entries, not on dataset entries themselves 
    (this gets verified against the live catalogue)
    """
    hits: list[dict] = []
    while True:
        params: dict = {
            "limit": SEARCH_PAGE_SIZE,
            "offset": len(hits) + 1,
        }
        if text:
            params["text"] = text
        r = _session.get(f"{KARTKATALOG_API}/search", params=params, timeout=60)
        r.raise_for_status()
        page = r.json()
        results = page.get("Results") or []
        hits.extend(results)
        if not results or len(hits) >= page.get("NumFound", 0):
            return hits


def get_metadata(uuid: str) -> dict:
    """GET /api/getdata/{uuid}: coverage BoundingBox, Constraints, distribution."""
    r = _session.get(f"{KARTKATALOG_API}/getdata/{uuid}", timeout=60)
    r.raise_for_status()
    return r.json()


def _parse_bound(value) -> float:
    # comma decimals, for example '−56,00' (Unfortunately strange norwegian format)
    return float(str(value).replace("−", "-").replace(",", "."))


def parse_coverage_bbox(metadata: dict) -> Bbox | None:
    bounds = metadata.get("BoundingBox") or {}
    try:
        return (_parse_bound(bounds["WestBoundLongitude"]),
                _parse_bound(bounds["SouthBoundLatitude"]),
                _parse_bound(bounds["EastBoundLongitude"]),
                _parse_bound(bounds["NorthBoundLatitude"]))
    except (KeyError, TypeError, ValueError):
        return None


def discover(bbox: Bbox, search_text: str | None,
             settings: GeonorgeSettings) -> list[DatasetRecord]:
    """search -> classify -> coverage/licence metadata -> relevance.
    The search API has no spatial filter unfortunately....

    Every hit comes back as a record; non-candidates carry a status explaining
    why, so the manifest accounts for the full catalogue sweep. 
    """
    records = []
    candidates = 0
    for hit in search_datasets(search_text):
        protocol = hit.get("DistributionProtocol") or ""
        uuid = hit.get("Uuid") or ""
        record = DatasetRecord(
            uuid=uuid,
            title=hit.get("Title") or "",
            organization=hit.get("Organization") or "",
            protocol=protocol,
            kind=classify(protocol),
            distribution_url=hit.get("DistributionUrl"),
            detail_url=hit.get("ShowDetailsUrl") or DETAIL_PAGE.format(uuid=uuid),
            is_open=bool(hit.get("IsOpenData")),
        )
        records.append(record)

        if record.kind == "skip":
            record.status = f"skipped: unsupported protocol {protocol!r}"
            continue
        if candidates >= settings.max_datasets:
            record.status = f"skipped: over GEONORGE_MAX_DATASETS={settings.max_datasets}"
            continue
        try:
            metadata = get_metadata(record.uuid)
        except requests.RequestException as e:
            record.status = f"error: metadata fetch failed ({e})"
            continue
        constraints = metadata.get("Constraints") or {}
        record.licence_url = constraints.get("OtherConstraintsLink")
        record.coverage_bbox = parse_coverage_bbox(metadata)
        if record.coverage_bbox is None:
            record.status = "skipped: no coverage extent"
        elif not bbox_intersects(record.coverage_bbox, bbox):
            record.status = "skipped: coverage outside window"
        else:
            candidates += 1
    return records


def _local_name(tag: str) -> str: 
    #  strip the XML namespace
    return tag.rsplit('}', 1)[-1]


def wfs_feature_types(url: str) -> list[str]:
    """Feature type names from WFS GetCapabilities document."""
    r = _session.get(url, params={"service": "WFS", "request": "GetCapabilities"},
                     timeout=60)
    r.raise_for_status()
    names = []
    for element in ET.fromstring(r.content).iter():
        if _local_name(element.tag) != "FeatureType":
            continue
        for child in element:
            if _local_name(child.tag) == "Name" and child.text:
                names.append(child.text.strip())
                break
    return names


def _feature_count(path: Path) -> int:
    if path.suffix == ".json":
        return len(json.loads(path.read_text(encoding="utf-8")).get("features") or [])
    try:
        return len(gpd.read_file(path))
    except Exception:
        return 0


def _wfs_pages(url: str, type_name: str, axis_bbox: str, out_dir: Path) -> list[Path]:
    """GetFeature for one feature type

    Asks for GeoJSON. And if refused then fallback to server default (GML).
    Return only page that actually contains features..
    """
    paths: list[Path] = []
    output_format, suffix = "application/json", ".json"
    for page in range(WFS_MAX_PAGES_PER_TYPE):
        params = {
            "service": "WFS", "version": "2.0.0", "request": "GetFeature",
            "typeNames": type_name, "srsName": "EPSG:4326",
            "bbox": axis_bbox,
            "count": WFS_PAGE_SIZE, "startIndex": page * WFS_PAGE_SIZE,
        }
        if output_format:
            params["outputFormat"] = output_format
        r = _session.get(url, params=params, timeout=300)
        if output_format and (not r.ok or not r.text.lstrip().startswith(("{", "["))):
            output_format, suffix = None, ".gml"
            params.pop("outputFormat")
            r = _session.get(url, params=params, timeout=300)
        if not r.ok or "ExceptionReport" in r.text[:512]:
            logger.debug(f"WFS GetFeature failed for {type_name} ({url}): {r.text[:200]}")
            break
        path = out_dir / f"{_safe_name(type_name)}_{page}{suffix}"
        path.write_bytes(r.content)
        count = _feature_count(path)
        if count == 0:
            path.unlink()
            break
        paths.append(path)
        if count < WFS_PAGE_SIZE:
            break
    return paths


def fetch_wfs(record: DatasetRecord, bbox: Bbox, output_dir: Path) -> list[Path]:
    """Fetch. GetFeature per feature type with a server-side BBOX of
    the buffered window, so national layers shrink before the local crop.

    Axis order for EPSG:4326 bboxes varies between servers: the spec urn form
    (lat/lon) is tried first, the legacy lon/lat form second.
    """
    out_dir = output_dir / "raw" / record.uuid
    out_dir.mkdir(parents=True, exist_ok=True)
    # catalogue DistributionUrls embed query strings (?service=wfs&request=...)
    # that would collide with the GetFeature params
    endpoint = (record.distribution_url or "").split("?", 1)[0]
    lon_min, lat_min, lon_max, lat_max = bbox
    axis_orders = (
        f"{lat_min},{lon_min},{lat_max},{lon_max},urn:ogc:def:crs:EPSG::4326",
        f"{lon_min},{lat_min},{lon_max},{lat_max},EPSG:4326",
    )
    paths: list[Path] = []
    for type_name in wfs_feature_types(endpoint):
        for axis_bbox in axis_orders:
            pages = _wfs_pages(endpoint, type_name, axis_bbox, out_dir)
            if pages:
                paths.extend(pages)
                break
    return paths


def _nedlasting(path: str) -> dict | list:
    r = _session.get(f"{NEDLASTING_API}/{path}", timeout=60)
    r.raise_for_status()
    return r.json()


def _bbox_ring(bbox: Bbox, epsg: str = ORDER_PROJECTION) -> str:
    """Closed bbox ring as the order API's 'x1 y1 x2 y2 ...' string, in epsg."""
    transformer = Transformer.from_crs(4326, int(epsg), always_xy=True)
    lon_min, lat_min, lon_max, lat_max = bbox
    corners = [(lon_min, lat_min), (lon_max, lat_min), (lon_max, lat_max),
               (lon_min, lat_max), (lon_min, lat_min)]
    return " ".join(f"{x:.2f} {y:.2f}"
                    for x, y in (transformer.transform(*c) for c in corners))


def _pick_format(formats: list[dict]) -> dict:
    """First format crop() can actually read; SOSI-only datasets are an error."""
    by_name = {(f.get("name") or "").upper(): f for f in formats}
    for preferred in OGR_READABLE_FORMATS:
        if preferred in by_name:
            return by_name[preferred]
    raise RuntimeError(
        f"no OGR-readable download format, available: {[f.get('name') for f in formats]}")


def _pick_projection(projections: list[dict]) -> dict:
    for p in projections:
        if p.get("code") == ORDER_PROJECTION:
            return p
    return projections[0] if projections else {"code": ORDER_PROJECTION}


def build_order(uuid: str, bbox: Bbox, capabilities: dict, formats: list[dict],
                projections: list[dict], areas: list[dict], email: str) -> dict:
    """Order payload for one dataset.

    Server-side clip via coordinates + coordinatesystem (lowercase 's', unlike
    the can-download endpoint!) when supportsPolygonSelection; otherwise order
    'Hele landet' and let crop() do the reduction.
    """
    fmt = _pick_format(formats)
    proj = _pick_projection(projections)
    line: dict = {
        "metadataUuid": uuid,
        "formats": [{"name": fmt.get("name")}],
        "projections": [{"code": proj.get("code"), "name": proj.get("name"),
                         "codespace": proj.get("codespace")}],
    }
    if capabilities.get("supportsPolygonSelection"):
        line["coordinates"] = _bbox_ring(bbox)
        line["coordinatesystem"] = ORDER_PROJECTION
        line["areas"] = [{"code": "Kart", "name": "Valgt fra kart", "type": "polygon"}]
    else:
        national = next((a for a in areas if a.get("type") == "landsdekkende"), None)
        if national is None:
            raise RuntimeError("dataset supports neither polygon clip nor 'Hele landet'")
        line["areas"] = [{"code": national.get("code"), "name": national.get("name"),
                          "type": national.get("type")}]
    return {"email": email, "orderLines": [line]}


def _await_order(receipt: dict, auth: tuple[str, str] | None) -> list[dict]:
    """Re-fetch the order until every file is ReadyForDownload.

    Clipped orders are processed asynchronously; prepackaged national files are
    usually ready immediately (status missing or already ReadyForDownload).
    """
    deadline = time.monotonic() + ORDER_TIMEOUT_SECONDS
    while True:
        files = receipt.get("files") or []
        failed = [f for f in files if "error" in (f.get("status") or "").lower()]
        if failed:
            raise RuntimeError(
                f"order failed for {[f.get('metadataName') or f.get('name') for f in failed]}")
        if all(f.get("status") in (None, "ReadyForDownload") for f in files):
            return files
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"order {receipt.get('referenceNumber')} not ready after {ORDER_TIMEOUT_SECONDS}s")
        time.sleep(ORDER_POLL_SECONDS)
        r = _session.get(f"{NEDLASTING_API}/order/{receipt['referenceNumber']}",
                         auth=auth, timeout=60)
        if r.status_code in (401, 403):
            # some datasets gate the order-status endpoint behind a Geonorge login,
            # even when the order itself was accepted anonymously
            raise RuntimeError(
                "order status requires Geonorge login (set GEONORGE_USERNAME/GEONORGE_PASSWORD)")
        r.raise_for_status()
        receipt = r.json()


def _download_order_files(files: list[dict], out_dir: Path,
                          auth: tuple[str, str] | None) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for f in files:
        url = f["downloadUrl"]
        dest = out_dir / (f.get("name") or url.rsplit("/", 1)[-1])
        with _session.get(url, auth=auth, stream=True, timeout=600) as r:
            r.raise_for_status()
            if "text/html" in (r.headers.get("content-type") or ""):
                # restricted datasets answer with a GeoID login page, HTTP 200
                raise RuntimeError(
                    "download returned a login page - dataset requires Geonorge login")
            with dest.open("wb") as fh:
                for chunk in r.iter_content(64 * 1024):
                    fh.write(chunk)
        paths.append(dest)
    return paths


def _unzip_all(paths: list[Path], out_dir: Path) -> list[Path]:
    """Unzip archives; returns the extracted files (a .gdb directory counts as one).

    Clipped orders arrive without a filename extension, so zips are detected by
    content, not suffix.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    out: list[Path] = []
    for p in paths:
        if not zipfile.is_zipfile(p):
            out.append(p)
            continue
        with zipfile.ZipFile(p) as z:
            z.extractall(out_dir)
            names = [n.replace("\\", "/") for n in z.namelist() if not n.endswith("/")]
        for name in names:
            if ".gdb/" in name.lower():
                gdb = out_dir / name[:name.lower().index(".gdb/") + 4]
                if gdb not in out:
                    out.append(gdb)
            else:
                out.append(out_dir / name)
    return out


def _order_and_download(order: dict, out_dir: Path,
                        auth: tuple[str, str] | None) -> list[Path]:
    r = _session.post(f"{NEDLASTING_API}/order", json=order, auth=auth, timeout=60)
    r.raise_for_status()
    files = _await_order(r.json(), auth)
    raw = _download_order_files(files, out_dir, auth)
    return _unzip_all(raw, out_dir / "unzipped")


def fetch_download(record: DatasetRecord, bbox: Bbox, output_dir: Path) -> list[Path]:
    """Order via nedlasting.geonorge.no (the geonorge_maritime download path,
    generalized to any dataset uuid).

    Server-side polygon clip when the dataset capabilities report
    supportsPolygonSelection; otherwise (or when the clip job fails on
    Geonorge's side) the prepackaged 'Hele landet' file, reduced by local crop.
    Requires GEONORGE_EMAIL; anonymous is fine for open data.
    """
    settings = GeonorgeSettings()
    if not settings.email:
        raise ValueError("GEONORGE_EMAIL is required for download/order datasets")
    auth = ((settings.username, settings.password)
            if settings.username and settings.password else None)

    capabilities = _nedlasting(f"capabilities/{record.uuid}")
    formats = _nedlasting(f"codelists/format/{record.uuid}")
    projections = _nedlasting(f"codelists/projection/{record.uuid}")
    use_polygon = bool(capabilities.get("supportsPolygonSelection"))
    out_dir = output_dir / "raw" / record.uuid

    if use_polygon:
        order = build_order(record.uuid, bbox, capabilities, formats,
                            projections, [], settings.email)
        try:
            return _order_and_download(order, out_dir, auth)
        except RuntimeError as e:
            logger.warning(f"{record.title}: clipped order failed ({e}); "
                           f"retrying with 'Hele landet'")

    areas = _nedlasting(f"codelists/area/{record.uuid}")
    order = build_order(record.uuid, bbox, {**capabilities, "supportsPolygonSelection": False},
                        formats, projections, areas, settings.email)
    return _order_and_download(order, out_dir, auth)


def _layers(path: Path) -> list[str | None]:
    # downloaded GML/FGDB hold many layers, WFS pages just one
    try:
        return [name for name, _ in pyogrio.list_layers(path)]
    except Exception:
        return [None]


def crop(paths: list[Path], bbox: Bbox) -> gpd.GeoDataFrame:
    """Clip fetched vector files (every layer) to buffered window bbox in WGS84.

    This is the step that reduces national data to only what is inside the box.
    """
    window = shapely_box(*bbox)
    frames = []
    for path in paths:
        for layer in _layers(path):
            try:
                gdf = gpd.read_file(path, layer=layer)
            except Exception as e:
                logger.warning(f"unreadable layer {layer!r} in {path.name} -- {e}")
                continue
            # geometry-less layers read as plain DataFrames; concat/clip need
            # unique row indexes and column names (some GML layers repeat gml_id)
            if not isinstance(gdf, gpd.GeoDataFrame) or gdf.empty:
                continue
            gdf = gdf.reset_index(drop=True)
            gdf = gdf.loc[:, ~gdf.columns.duplicated()]
            gdf = gdf.set_crs(4326) if gdf.crs is None else gdf.to_crs(4326)
            clipped = gdf.clip(window)
            if not clipped.empty:
                frames.append(clipped)
    if not frames:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    return gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs="EPSG:4326")


def annotate(gdf: gpd.GeoDataFrame, record: DatasetRecord) -> gpd.GeoDataFrame:
    """Per-feature provenance as flat properties surviving the GeoJSON round-trip."""
    gdf = gdf.copy()
    gdf["collekt_dataset_uuid"] = record.uuid
    gdf["collekt_dataset_title"] = record.title
    gdf["collekt_organization"] = record.organization
    gdf["collekt_protocol"] = record.protocol
    gdf["collekt_distribution_url"] = record.distribution_url
    gdf["collekt_licence_url"] = record.licence_url
    gdf["collekt_access"] = "open" if record.is_open else "restricted"
    gdf["collekt_detail_url"] = record.detail_url
    gdf["collekt_retrieved_at"] = dt.datetime.now(tz=dt.timezone.utc).isoformat()
    return gdf


class Geonorge(DataSource):
    """Datasource driven by Kartkatalog. Discover datasets covering the query
    window, fetch per DistributionProtocol, crop to buffered bbox,
    annotate per-feature metadata, store as GeoJSON files plus metadata in a manifest."""

    buffer_km: float | None
    search_text: str | None

    def __init__(self, buffer_km: float | None = None, search_text: str | None = None):
        # search_text is the interim home of the optional filter
        super().__init__(name="geonorge")

        self.buffer_km = buffer_km
        self.search_text = search_text

    def execute(self,
            from_time: dt.datetime | None = None,
            to_time: dt.datetime | None = None,
            longitude: float | None = None,
            latitude: float | None = None,
            radius: float | None = None,
            region: Path | None = None,
            log_level: str | None = None,
            verbose: bool | None = None,
            output_dir: Path | None = Path(tempfile.gettempdir())
           ) -> list[Path]:

        if latitude is None or longitude is None or radius is None:
            raise ValueError("geonorge requires latitude, longitude and radius")

        settings = GeonorgeSettings()
        buffer_km = self.buffer_km if self.buffer_km is not None else settings.buffer_km
        bbox = window_bbox(latitude, longitude, radius_km=radius, buffer_km=buffer_km)

        out_dir = Path(output_dir) / "geonorge"
        out_dir.mkdir(parents=True, exist_ok=True)

        records = discover(bbox, self.search_text, settings)
        logger.info(f"Discovered {len(records)} datasets, "
                    f"{sum(r.status == 'candidate' for r in records)} candidates")

        written: list[Path] = []
        for record in records:
            if record.status != "candidate":
                continue
            try:
                if record.kind == "wfs":
                    raw = fetch_wfs(record, bbox, out_dir)
                else:
                    raw = fetch_download(record, bbox, out_dir)
                features = crop(raw, bbox)
                if features.empty:
                    record.status = "empty: no features inside window"
                    continue
                features = annotate(features, record)
                path = out_dir / f"{record.uuid}_{_safe_name(record.title)}.geojson"
                features.to_file(path, driver="GeoJSON")
                record.status = f"collected: {len(features)} features"
                written.append(path)
            except Exception as e:
                record.status = f"error: {e}"
                logger.warning(f"{record.title} ({record.uuid}) failed -- {e}")

        manifest = out_dir / "manifest.json"
        manifest.write_text(json.dumps({
            "datasource": self.name,
            "executed_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
            "window": {"latitude": latitude, "longitude": longitude,
                       "radius_km": radius, "buffer_km": buffer_km, "bbox": bbox},
            "time_window": {
                "from_time": from_time.isoformat() if from_time else None,
                "to_time": to_time.isoformat() if to_time else None,
                "note": "unused: catalogue datasets are static reference data",
            },
            "datasets": [asdict(r) for r in records],
        }, indent=2, ensure_ascii=False), encoding="utf-8")

        return [manifest, *written]
