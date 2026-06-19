"""
Geonorge datasource: fetch a curated allowlist of maritime datasets, crop each
to the query window, then annotate features with per-feature provenance.

Pipeline per dataset: fetch (WFS server-side bbox, or the download/order API)
-> crop to window -> annotate. The set of datasets is fixed (ALLOWLIST, pinned
by UUID) rather than discovered, so a run is deterministic and fast: no
catalogue sweep, no rasters, no time wasted on data we do not want.

WFS datasets are cropped server-side via BBOX. Download datasets are cropped
server-side too when they support a polygon clip; the rest fall back to ordering
the whole-country ('Hele landet') file and cropping it locally.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import tempfile
import time
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

# GDAL's GML driver otherwise resolves each page's xsi:schemaLocation by
# fetching the remote XSD (a DescribeFeatureType call back to the slow Geonorge
# server) on every cold open -- 30-60 s even for a 2-feature page. We don't need
# it: geometry and attributes are inferred from the data. This single line is
# the biggest geonorge fetch speedup (profiled: ~122 s -> ~0 across a run).
pyogrio.set_gdal_config_options({"GML_DOWNLOAD_SCHEMA": "NO"})

DETAIL_PAGE = "https://kartkatalog.geonorge.no/metadata/uuid/{uuid}"

WFS_PAGE_SIZE = 1000
WFS_MAX_PAGES_PER_TYPE = 20
WFS_REQUEST_TIMEOUT = 60  # s; GetFeature is bbox-filtered (small) so fail fast

NEDLASTING_API = "https://nedlasting.geonorge.no/api"
ORDER_PROJECTION = "25833"  # EUREF89 UTM 33
ORDER_POLL_SECONDS = 5
ORDER_TIMEOUT_SECONDS = 600
OGR_READABLE_FORMATS = ("GML", "FGDB", "GEOJSON", "GPKG", "SHAPE")

Bbox = tuple[float, float, float, float]  # (lon_min, lat_min, lon_max, lat_max), WGS84

# --- Allowlist: the ONLY Geonorge datasets we fetch. Curated for maritime
# surveillance and pinned by UUID (stable across title edits). Prefer the
# OGC:WFS variant (server-side bbox crop, no national download); fall back to
# GEONORGE:DOWNLOAD only where no WFS exists. All are open, OGR-readable vector
# layers -- no rasters.
#
# WFS entries carry the exact feature-type name(s) to request, so we skip the
# slow per-dataset GetCapabilities call and GetFeature only the layers we want
# (the general Fiskeridir/Norkyst services expose 56/11 layers; we name a few).
# Re-probe an endpoint's GetCapabilities only when adding/changing a dataset.
#                (uuid, title, protocol, endpoint-or-url, wfs_type_names)
ALLOWLIST: list[tuple[str, str, str, str, tuple[str, ...]]] = [
    # navigation & infrastructure
    ("871960a1-0f01-4c47-8f79-5d338b65197e", "Dybdedata - kurver generaliserte",
     "GEONORGE:DOWNLOAD", "https://nedlasting.geonorge.no/api/capabilities/", ()),
    ("42e58e93-13da-4f47-8c1c-3525af7c3e77", "Hovedled og biled WFS",
     "OGC:WFS", "https://wfs.geonorge.no/skwms1/wfs.farled",
     ("app:HovedledOgBiled",)),  # fairway centrelines only; drop the area polygon + its outline
    ("73e46cf4-d9f5-4d75-b148-bb5edf888c4a", "Navigasjonsinstallasjoner WFS",
     "OGC:WFS", "https://maps.kystverket.cloudgis.no/enterprise/services/PROD/nfs_sistop_ekstern_prod/MapServer/WFSServer",
     ("nfs_sistop_ekstern_prod:Lys", "nfs_sistop_ekstern_prod:IB",
      "nfs_sistop_ekstern_prod:Racon", "nfs_sistop_ekstern_prod:Fast_sjømerke",
      "nfs_sistop_ekstern_prod:Flytende_merke",)),
    ("3eef614a-b82f-4779-8a30-02876b792d1a", "Nødhavner WFS",
     "OGC:WFS", "https://wfs.geonorge.no/skwms1/wfs.nodhavner", ("app:Nødhavn",)),
    ("1765495c-cfce-49d1-899e-321e2c421f39", "Akvakultur - lokaliteter WFS",
     "OGC:WFS", "https://wfs.geonorge.no/skwms1/wfs.akvakulturlokaliteter",
     ("app:AkvakulturFlate", "app:AkvakulturPunkt", "app:Akvakulturgrense")),
    ("4e3c59b4-528f-4806-ada3-e9455c9b7d4e", "Korallrev WFS",
     "OGC:WFS", "https://wfs.geonorge.no/skwms1/wfs.korallrev", ("app:Korallrev",)),
    ("e8c675c4-ada7-427e-8103-abf891b824b9", "Korallrev - forbudsområder WFS",
     "OGC:WFS", "https://wfs.geonorge.no/skwms1/wfs.korallrevforbudsomrader",
     ("app:Korallrev", "app:KorallrevGrense")),
    ("e46767e4-c6d9-49a6-93e8-716da0922fd7", "Havnedata",
     "GEONORGE:DOWNLOAD", "https://nedlasting.geonorge.no/api/capabilities/", ()),
    # regulatory / restricted zones
    ("f9552903-2445-46c9-8a0d-37439f87a448", "Forsvarets skyte- og øvingsfelt i sjø WFS",
     "OGC:WFS", "https://wfs.geonorge.no/skwms1/wfs.ovingsfeltsjo",
     ("app:SkytefeltSjø", "app:Skytefeltgrense")),
    ("8e7b3c93-4601-487a-b241-7017e2f624e7", "Militære forbudsområder innen sjøforsvaret WFS",
     "OGC:WFS", "https://wfs.geonorge.no/skwms1/wfs.militereforbudsomradersjo",
     ("app:ForbudsområdeGrense", "app:MilitærtForbudsområdeSjø")),
    ("b2c6f249-0c19-45c0-a91a-8f873d2c3c4f", "Tråling forbudsområder",
     "OGC:WFS", "https://gis.fiskeridir.no/server/services/FiskeridirWFS_fiskeri/MapServer/WFSServer",
     ("FiskeridirWFS_fiskeri:traalforbud_jennegga_malangsgrunnen",
      "FiskeridirWFS_fiskeri:traalforbud_storegga",
      "FiskeridirWFS_fiskeri:reguleringer_stormasket_traal_inntil12nm")),
    # oceanographic (Norkyst exposes depths 0-250 m; we keep the 0 m surface layer)
    ("a0e9f7ca-9ab0-4606-a62e-e71b8f78377d", "Norkyst - Gjennomsnittlig strømstyrke og retning WFS",
     "OGC:WFS", "https://kart.hi.no/data/oseanografi/wfs",
     ("oseanografi:Norkyst_gjennomsnittlig_stromstyrke_og_retning_000m",)),
]


class RestrictedDatasetError(Exception):
    """A download hit a Geonorge auth wall (401/403 or a GeoID login page) instead
    of data. Use GEONORGE_USERNAME/GEONORGE_PASSWORD.
    """


class GeonorgeSettings(BaseSettings):
    """GEONORGE_* settings. email/username/password are only needed by the
    download/order API; buffer_km is the fallback window buffer.
    """
    email: str | None = None
    username: str | None = None
    password: str | None = None
    buffer_km: float = 0.0

    model_config = SettingsConfigDict(
                    env_file='.env',
                    env_nested_delimiter='__',
                    env_prefix='GEONORGE_',
                    extra='ignore'
                )


@dataclass
class DatasetRecord:
    """One allowlisted dataset and what it is turned into."""
    uuid: str
    title: str
    protocol: str
    kind: str
    distribution_url: str | None
    detail_url: str
    type_names: tuple[str, ...] = ()
    organization: str = ""
    is_open: bool = True
    licence_url: str | None = None
    status: str = "pending"


def window_bbox(latitude: float, longitude: float,
                radius_km: float, buffer_km: float) -> Bbox:
    """Buffered query window as WGS84 bbox"""
    bounds = get_coordinates_min_max(latitude, longitude,
                                     radius_in_km=radius_km + buffer_km)
    return (bounds['lon_min'], bounds['lat_min'],
            bounds['lon_max'], bounds['lat_max'])


def _safe_name(s: str) -> str:
    """Filesystem-safe lowercase."""
    return (s.lower()
             .replace("ø", "oe").replace("å", "aa").replace("æ", "ae")
             .replace(" ", "_").replace("/", "_").replace(":", "_"))


def classify(protocol: str) -> Literal["wfs", "download", "skip"]:
    """Protocol dispatch: OGC:WFS clips server-side; GEONORGE:DOWNLOAD orders."""
    if protocol == "OGC:WFS":
        return "wfs"
    if protocol == "GEONORGE:DOWNLOAD":
        return "download"
    return "skip"


def discover() -> list[DatasetRecord]:
    """Build records straight from ALLOWLIST -- no catalogue search, no network."""
    return [
        DatasetRecord(uuid=uuid, title=title, protocol=protocol,
                      kind=classify(protocol), distribution_url=url,
                      detail_url=DETAIL_PAGE.format(uuid=uuid), type_names=types)
        for uuid, title, protocol, url, types in ALLOWLIST
    ]


def _feature_count(path: Path) -> int:
    if path.suffix == ".json":
        return len(json.loads(path.read_text(encoding="utf-8")).get("features") or [])
    try:
        return len(gpd.read_file(path))
    except Exception:
        return 0


def _wfs_output_format(endpoint: str) -> str | None:
    """Format to request per server (probed), to avoid a wasted first request.

    deegree (wfs.geonorge.no) and the ArcGIS MapServer services both serve GML
    by default and reject GeoJSON, so we request GML directly (None) -- cheap to
    read now that remote-schema resolution is off, and full fidelity (ArcGIS
    GeoJSON drops some features). geoserver (hi.no) returns GeoJSON cleanly.
    """
    if "MapServer/WFSServer" in endpoint or "wfs.geonorge.no" in endpoint:
        return None
    return "application/json"


def _wfs_pages(url: str, type_name: str, axis_bbox: str, out_dir: Path,
               output_format: str | None) -> list[Path]:
    """GetFeature pages for one feature type, requesting output_format
    (None = the server's GML default). Keeps a one-shot GML fallback in case a
    server unexpectedly refuses the expected format. Returns only pages with
    features.
    """
    paths: list[Path] = []
    safe_type = _safe_name(type_name)
    of = output_format
    suffix = ".json" if of else ".gml"
    for page in range(WFS_MAX_PAGES_PER_TYPE):
        params = {
            "service": "WFS", "version": "2.0.0", "request": "GetFeature",
            "typeNames": type_name, "srsName": "EPSG:4326",
            "bbox": axis_bbox,
            "count": WFS_PAGE_SIZE, "startIndex": page * WFS_PAGE_SIZE,
        }
        if of:
            params["outputFormat"] = of
        r = _session.get(url, params=params, timeout=WFS_REQUEST_TIMEOUT)
        if of and (not r.ok or not r.text.lstrip().startswith(("{", "["))):
            of, suffix = None, ".gml"
            params.pop("outputFormat")
            r = _session.get(url, params=params, timeout=WFS_REQUEST_TIMEOUT)
        if not r.ok or "ExceptionReport" in r.text[:512]:
            logger.debug(f"WFS GetFeature failed for {type_name} ({url}): {r.text[:200]}")
            break
        path = out_dir / f"{safe_type}_{page}{suffix}"
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
    """GetFeature each allowlisted type with a server-side BBOX of the window.

    Requests the output format the endpoint is known to support (skipping the
    GeoJSON attempt on GML-only servers). WFS 2.0.0 with the urn CRS uses lat/lon
    axis order per spec, which all our endpoints honor -- so we send that
    directly rather than retrying the legacy lon/lat form.
    """
    out_dir = output_dir / "raw" / record.uuid
    out_dir.mkdir(parents=True, exist_ok=True)
    # catalogue DistributionUrls embed query strings (?service=wfs&request=...)
    # that would collide with the GetFeature params
    endpoint = (record.distribution_url or "").split("?", 1)[0]
    output_format = _wfs_output_format(endpoint)
    lon_min, lat_min, lon_max, lat_max = bbox
    axis_bbox = f"{lat_min},{lon_min},{lat_max},{lon_max},urn:ogc:def:crs:EPSG::4326"
    paths: list[Path] = []
    for type_name in record.type_names:
        paths.extend(_wfs_pages(endpoint, type_name, axis_bbox, out_dir, output_format))
    return paths


def _nedlasting(path: str) -> dict | list:
    r = _session.get(f"{NEDLASTING_API}/{path}", timeout=60)
    r.raise_for_status()
    return r.json()


def _pick_format(formats: list[dict]) -> dict:
    """First format crop() can actually read; raster-only datasets are an error."""
    by_name = {(f.get("name") or "").upper(): f for f in formats}
    for preferred in OGR_READABLE_FORMATS:
        if preferred in by_name:
            return by_name[preferred]
    raise RuntimeError(
        f"no OGR-readable download format, available: {[f.get('name') for f in formats]}")


def _bbox_ring(bbox: Bbox) -> str:
    """Closed bbox ring as the order API's 'x1 y1 x2 y2 ...' string, in ORDER_PROJECTION."""
    transformer = Transformer.from_crs(4326, int(ORDER_PROJECTION), always_xy=True)
    lon_min, lat_min, lon_max, lat_max = bbox
    corners = [(lon_min, lat_min), (lon_max, lat_min), (lon_max, lat_max),
               (lon_min, lat_max), (lon_min, lat_min)]
    return " ".join(f"{x:.2f} {y:.2f}"
                    for x, y in (transformer.transform(*c) for c in corners))


def build_order(uuid: str, bbox: Bbox, capabilities: dict, formats: list[dict],
                projections: list[dict], areas: list[dict], email: str) -> dict:
    """Order payload for one dataset, in an OGR-readable format.

    Prefer the server-side polygon clip (returns only the window); otherwise
    order the whole-country ('Hele landet') file, which crop() reduces locally.
    """
    fmt = _pick_format(formats)
    proj = next((p for p in projections if p.get("code") == ORDER_PROJECTION),
                projections[0] if projections else {"code": ORDER_PROJECTION})
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
            raise RestrictedDatasetError(
                "restricted dataset; use GEONORGE_USERNAME/GEONORGE_PASSWORD")
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
            if r.status_code in (401, 403):
                raise RestrictedDatasetError(
                    "restricted dataset; use GEONORGE_USERNAME/GEONORGE_PASSWORD")
            r.raise_for_status()
            if "text/html" in (r.headers.get("content-type") or ""):
                # restricted datasets answer with a GeoID login page, HTTP 200
                raise RestrictedDatasetError(
                    "restricted dataset; use GEONORGE_USERNAME/GEONORGE_PASSWORD")
            with dest.open("wb") as fh:
                for chunk in r.iter_content(64 * 1024):
                    fh.write(chunk)
        paths.append(dest)
    return paths


def _unzip_all(paths: list[Path], out_dir: Path) -> list[Path]:
    """Unzip archives; returns the extracted files (a .gdb directory counts as one). """
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


def fetch_download(record: DatasetRecord, bbox: Bbox, output_dir: Path) -> list[Path]:
    """Order the dataset via nedlasting.geonorge.no, then crop locally.

    Always prefer a server-side crop: if the dataset supports a polygon clip the
    order is bbox-clipped server-side; only datasets without one fall back to the
    whole-country ('Hele landet') download. Requires GEONORGE_EMAIL; anonymous is
    fine for open data.

    NOTE for future additions: before adding a GEONORGE:DOWNLOAD dataset, prefer a
    server-side crop where one exists -- an OGC:WFS / OGC:API-Features variant of
    the same data, or this order API's polygon clip -- and only rely on the full
    'Hele landet' download when no cropping option is available (it is the slow
    path, and some datasets do not even produce a national file).
    """
    settings = GeonorgeSettings()
    if not settings.email:
        raise ValueError("GEONORGE_EMAIL is required for download/order datasets")
    auth = ((settings.username, settings.password)
            if settings.username and settings.password else None)

    capabilities = _nedlasting(f"capabilities/{record.uuid}")
    formats = _nedlasting(f"codelists/format/{record.uuid}")
    projections = _nedlasting(f"codelists/projection/{record.uuid}")
    # only the 'Hele landet' fallback needs the area codelist
    areas = ([] if capabilities.get("supportsPolygonSelection")
             else _nedlasting(f"codelists/area/{record.uuid}"))
    order = build_order(record.uuid, bbox, capabilities, formats, projections,
                        areas, settings.email)
    out_dir = output_dir / "raw" / record.uuid
    r = _session.post(f"{NEDLASTING_API}/order", json=order, auth=auth, timeout=60)
    r.raise_for_status()
    files = _await_order(r.json(), auth)
    raw = _download_order_files(files, out_dir, auth)
    return _unzip_all(raw, out_dir / "unzipped")


def _layers(path: Path) -> list[str | None]:
    # downloaded GML/FGDB hold many layers, WFS pages just one
    try:
        return [name for name, _ in pyogrio.list_layers(path)]
    except Exception:
        return [None]


# shapefile sidecars + schema/doc files that are not standalone vector sources:
# reading a .dbf/.shx re-opens the parent .shp and duplicates its features.
_NON_VECTOR_SUFFIXES = {
    ".dbf", ".shx", ".prj", ".cpg", ".sbn", ".sbx", ".qix", ".aih", ".ain",
    ".gfs", ".xsd", ".xml", ".txt", ".pdf", ".doc", ".docx", ".html", ".htm",
    ".png", ".jpg", ".jpeg",
}


def _read_bbox(window, layer_crs) -> tuple | None:
    """The window's bounds in the layer's CRS, for a pyogrio bbox= pushdown.

    The transformed bounds are an axis-aligned superset of the window (crop()
    then clips exactly), so no in-window feature is dropped. None when the CRS is
    unknown -- the whole layer is read and clipped, as before.
    """
    if not layer_crs:
        return None
    try:
        return tuple(gpd.GeoSeries([window], crs="EPSG:4326")
                     .to_crs(layer_crs).total_bounds)
    except Exception:
        return None


def crop(paths: list[Path], bbox: Bbox) -> gpd.GeoDataFrame:
    """Clip fetched vector files to the buffered window bbox.

    Reads only the window via a bbox= pushdown in each layer's CRS (national
    download files are otherwise read in full), then clips exactly. Skips
    shapefile sidecars/doc files, which would re-open the same .shp.
    """
    window = shapely_box(*bbox)
    frames = []
    for path in paths:
        if path.suffix.lower() in _NON_VECTOR_SUFFIXES:
            continue
        for layer in _layers(path):
            try:
                read_bbox = _read_bbox(window, pyogrio.read_info(path, layer=layer).get("crs"))
            except Exception:
                read_bbox = None
            try:
                gdf = gpd.read_file(path, layer=layer, bbox=read_bbox)
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
    """Fetch the curated ALLOWLIST of maritime Geonorge datasets, crop each to the
    query window, annotate per-feature provenance, write GeoJSON + a manifest."""

    buffer_km: float | None

    def __init__(self, buffer_km: float | None = None):
        super().__init__(name="geonorge")
        self.buffer_km = buffer_km

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

        records = discover()
        logger.info(f"Fetching {len(records)} allowlisted Geonorge datasets")

        written: list[Path] = []
        for record in records:
            try:
                raw = (fetch_wfs(record, bbox, out_dir) if record.kind == "wfs"
                       else fetch_download(record, bbox, out_dir))
                features = crop(raw, bbox)
                if features.empty:
                    record.status = "empty: no features inside window"
                    continue
                features = annotate(features, record)
                path = out_dir / f"{record.uuid}_{_safe_name(record.title)}.geojson"
                path.unlink(missing_ok=True)  # pyogrio/Windows errors on overwriting an existing GeoJSON
                features.to_file(path, driver="GeoJSON")
                record.status = f"collected: {len(features)} features"
                written.append(path)
            except RestrictedDatasetError as e:
                # auth wall, not data: never write it, record it as skipped
                record.status = "skipped: restricted dataset; use GEONORGE_USERNAME/GEONORGE_PASSWORD"
                logger.warning(f"{record.title} ({record.uuid}) skipped -- {e}")
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
