"""Geonorge maritime vector-data adapter.

Fetches one curated Geonorge dataset (pinned by its catalogue UUID) for the
request's bounding box and writes the features as GeoParquet. Geonorge datasets
are static reference data, so this is a per-request source: the time window is
ignored and a single file is written for the whole request.

Each catalog entry names its access protocol:

- ``wfs``: GetFeature on the entry's ``type_names`` with a server-side BBOX.
- ``download``: the nedlasting.geonorge.no order API. The order is clipped
  server-side to the bbox when the dataset supports a polygon clip; otherwise the
  whole-country ('Hele landet') file is ordered and cropped locally. Orders need
  ``GEONORGE_EMAIL``; restricted datasets also need ``GEONORGE_USERNAME`` /
  ``GEONORGE_PASSWORD``.

Whichever protocol is used, the fetched files are clipped exactly to the bbox.
"""

from __future__ import annotations

import logging
import os
import tempfile
import time
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from collekt.core.availability import Availability, AvailabilityMethod, AvailabilityStatus
from collekt.core.config import Config, SourceConfig
from collekt.core.diagnostics import DoctorCheck, DoctorStatus
from collekt.core.naming import format_pattern, pattern_values
from collekt.core.request import Request
from collekt.sources.base import (
    ProgressCallback,
    SourceAdapter,
    SourceResult,
    SourceStatus,
    null_progress,
    register_adapter,
    should_reuse_cache,
)

if TYPE_CHECKING:
    import geopandas as gpd

logger = logging.getLogger(__name__)

EMAIL_ENV = "GEONORGE_EMAIL"
USERNAME_ENV = "GEONORGE_USERNAME"
PASSWORD_ENV = "GEONORGE_PASSWORD"

WFS_PAGE_SIZE = 1000
WFS_MAX_PAGES_PER_TYPE = 20
WFS_REQUEST_TIMEOUT = 60  # s; GetFeature is bbox-filtered (small), so fail fast

NEDLASTING_API = "https://nedlasting.geonorge.no/api"
ORDER_PROJECTION = "25833"  # EUREF89 UTM 33
ORDER_POLL_SECONDS = 5
ORDER_TIMEOUT_SECONDS = 600
OGR_READABLE_FORMATS = ("GML", "FGDB", "GEOJSON", "GPKG", "SHAPE")

# Shapefile sidecars and schema/doc files shipped in order archives. They are not
# standalone vector sources: reading a .dbf/.shx re-opens the parent .shp and
# duplicates its features.
NON_VECTOR_SUFFIXES = frozenset(
    {
        ".dbf",
        ".shx",
        ".prj",
        ".cpg",
        ".sbn",
        ".sbx",
        ".qix",
        ".aih",
        ".ain",
        ".gfs",
        ".xsd",
        ".xml",
        ".txt",
        ".pdf",
        ".doc",
        ".docx",
        ".html",
        ".htm",
        ".png",
        ".jpg",
        ".jpeg",
    }
)

Bbox = tuple[float, float, float, float]  # (west, south, east, north), WGS84

# wfs.geonorge.no intermittently answers 504 after 30 s (observed ~1 request in 5)
# while the same request otherwise returns in well under a second, so gateway
# errors on GET are retried; the final response is still returned for reporting.
_session = requests.Session()
_session.mount(
    "https://",
    HTTPAdapter(
        max_retries=Retry(
            total=2, status_forcelist=(502, 503, 504), allowed_methods={"GET"}, backoff_factor=1, raise_on_status=False
        )
    ),
)


class RestrictedDatasetError(Exception):
    """A download hit a Geonorge auth wall (401/403 or a GeoID login page) instead of data."""


class MissingEmailError(Exception):
    """A download dataset was requested without ``GEONORGE_EMAIL``, which the order API requires."""


RESTRICTED_MESSAGE = f"restricted dataset; set {USERNAME_ENV}/{PASSWORD_ENV}"


def _bbox(request: Request) -> Bbox:
    region = request.region
    return (region.west, region.south, region.east, region.north)


def _output_path(request: Request, source: SourceConfig, request_dir: Path) -> Path:
    values = pattern_values(
        source=source.name,
        dataset_id=source.dataset_id,
        region=request.region,
        start=request.start_datetime,
        end=request.end_datetime,
    )
    return request_dir / source.path / format_pattern(source.filename_pattern, values)


def _protocol(source: SourceConfig) -> str:
    protocol = str(source.raw.get("protocol", ""))
    if protocol not in {"wfs", "download"}:
        raise ValueError(f"source {source.name!r} has protocol {protocol!r}; expected 'wfs' or 'download'")
    return protocol


# --- WFS --------------------------------------------------------------------


def _wfs_axis_bbox(bbox: Bbox) -> str:
    """BBOX parameter in WFS 2.0.0 urn-CRS axis order (lat/lon), which all our endpoints honor."""
    west, south, east, north = bbox
    return f"{south},{west},{north},{east},urn:ogc:def:crs:EPSG::4326"


def _wfs_params(bbox: Bbox) -> dict[str, Any]:
    """The GetFeature parameters shared by every page and feature type (also shown in plans)."""
    return {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "srsName": "EPSG:4326",
        "bbox": _wfs_axis_bbox(bbox),
        "count": WFS_PAGE_SIZE,
    }


def _feature_count(path: Path) -> int:
    import pyogrio

    try:
        return int(pyogrio.read_info(path, force_feature_count=True)["features"])
    except Exception:  # noqa: BLE001 - an unreadable page holds no usable features
        return 0


def _wfs_pages(endpoint: str, type_name: str, base_params: dict[str, Any], out_dir: Path) -> list[Path]:
    """Fetch every GetFeature page of one feature type; returns only pages with features."""
    paths: list[Path] = []
    params = dict(base_params, typeNames=type_name)
    safe_type = type_name.replace(":", "_")
    for page in range(WFS_MAX_PAGES_PER_TYPE):
        if page:
            # sent from the second page on only: some deegree services (e.g. Nødhavner)
            # time out on an explicit startIndex=0, although it is the default
            params["startIndex"] = page * WFS_PAGE_SIZE
        response = _session.get(endpoint, params=params, timeout=WFS_REQUEST_TIMEOUT)
        if not response.ok or "ExceptionReport" in response.text[:512]:
            raise RuntimeError(f"WFS GetFeature failed for {type_name}: {response.text[:200]}")
        path = out_dir / f"{safe_type}_{page}.gml"
        path.write_bytes(response.content)
        count = _feature_count(path)
        if count == 0:
            path.unlink()
            break
        paths.append(path)
        if count < WFS_PAGE_SIZE:
            break
    return paths


def fetch_wfs(source: SourceConfig, bbox: Bbox, out_dir: Path) -> list[Path]:
    """GetFeature each of the source's ``type_names`` with a server-side BBOX."""
    endpoint = str(source.raw["endpoint"])
    base_params = _wfs_params(bbox)
    paths: list[Path] = []
    for type_name in source.raw.get("type_names") or ():
        paths.extend(_wfs_pages(endpoint, str(type_name), base_params, out_dir))
    return paths


# --- Download/order API -----------------------------------------------------


def _nedlasting(path: str) -> Any:
    response = _session.get(f"{NEDLASTING_API}/{path}", timeout=60)
    response.raise_for_status()
    return response.json()


def _pick_format(formats: list[dict[str, Any]]) -> dict[str, Any]:
    """First format `crop` can read; raster- or SOSI-only datasets are an error."""
    by_name = {(f.get("name") or "").upper(): f for f in formats}
    for preferred in OGR_READABLE_FORMATS:
        if preferred in by_name:
            return by_name[preferred]
    raise RuntimeError(f"no OGR-readable download format, available: {[f.get('name') for f in formats]}")


def _bbox_ring(bbox: Bbox) -> str:
    """Closed bbox ring as the order API's ``"x1 y1 x2 y2 ..."`` string, in ORDER_PROJECTION."""
    from pyproj import Transformer

    transformer = Transformer.from_crs(4326, int(ORDER_PROJECTION), always_xy=True)
    west, south, east, north = bbox
    corners = [(west, south), (east, south), (east, north), (west, north), (west, south)]
    return " ".join(f"{x:.2f} {y:.2f}" for x, y in (transformer.transform(*corner) for corner in corners))


def build_order(
    uuid: str,
    bbox: Bbox,
    capabilities: dict[str, Any],
    formats: list[dict[str, Any]],
    projections: list[dict[str, Any]],
    areas: list[dict[str, Any]],
    email: str,
) -> dict[str, Any]:
    """Build the order payload for one dataset, in an OGR-readable format.

    Prefers the server-side polygon clip, which returns only the bbox; otherwise
    orders the whole-country ('Hele landet') file for `crop` to reduce locally.
    """
    fmt = _pick_format(formats)
    proj = next(
        (p for p in projections if p.get("code") == ORDER_PROJECTION),
        projections[0] if projections else {"code": ORDER_PROJECTION},
    )
    line: dict[str, Any] = {
        "metadataUuid": uuid,
        "formats": [{"name": fmt.get("name")}],
        "projections": [{"code": proj.get("code"), "name": proj.get("name"), "codespace": proj.get("codespace")}],
    }
    if capabilities.get("supportsPolygonSelection"):
        line["coordinates"] = _bbox_ring(bbox)
        # lowercase 's' here, unlike the can-download endpoint's coordinateSystem
        line["coordinatesystem"] = ORDER_PROJECTION
        line["areas"] = [{"code": "Kart", "name": "Valgt fra kart", "type": "polygon"}]
    else:
        national = next((a for a in areas if a.get("type") == "landsdekkende"), None)
        if national is None:
            raise RuntimeError("dataset supports neither polygon clip nor 'Hele landet'")
        line["areas"] = [{"code": national.get("code"), "name": national.get("name"), "type": national.get("type")}]
    return {"email": email, "orderLines": [line]}


def _await_order(receipt: dict[str, Any], auth: tuple[str, str] | None) -> list[dict[str, Any]]:
    """Re-fetch the order until every file is ReadyForDownload."""
    deadline = time.monotonic() + ORDER_TIMEOUT_SECONDS
    while True:
        files = receipt.get("files") or []
        failed = [f for f in files if "error" in (f.get("status") or "").lower()]
        if failed:
            raise RuntimeError(f"order failed for {[f.get('metadataName') or f.get('name') for f in failed]}")
        if all(f.get("status") in (None, "ReadyForDownload") for f in files):
            return files
        if time.monotonic() > deadline:
            raise TimeoutError(f"order {receipt.get('referenceNumber')} not ready after {ORDER_TIMEOUT_SECONDS}s")
        time.sleep(ORDER_POLL_SECONDS)
        response = _session.get(f"{NEDLASTING_API}/order/{receipt['referenceNumber']}", auth=auth, timeout=60)
        if response.status_code in (401, 403):
            # some datasets gate the order-status endpoint behind a Geonorge login,
            # even when the order itself was accepted anonymously
            raise RestrictedDatasetError(RESTRICTED_MESSAGE)
        response.raise_for_status()
        receipt = response.json()


def _download_order_files(files: list[dict[str, Any]], out_dir: Path, auth: tuple[str, str] | None) -> list[Path]:
    paths = []
    for file in files:
        url = file["downloadUrl"]
        dest = out_dir / (file.get("name") or url.rsplit("/", 1)[-1])
        with _session.get(url, auth=auth, stream=True, timeout=600) as response:
            if response.status_code in (401, 403):
                raise RestrictedDatasetError(RESTRICTED_MESSAGE)
            response.raise_for_status()
            if "text/html" in (response.headers.get("content-type") or ""):
                # restricted datasets answer with a GeoID login page, HTTP 200
                raise RestrictedDatasetError(RESTRICTED_MESSAGE)
            with dest.open("wb") as handle:
                for chunk in response.iter_content(64 * 1024):
                    handle.write(chunk)
        paths.append(dest)
    return paths


def _unzip_all(paths: list[Path], out_dir: Path) -> list[Path]:
    """Unzip archives and return the extracted files (a .gdb directory counts as one).

    Clipped order files arrive without an extension, so archives are detected by
    content rather than by name.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    out: list[Path] = []
    for path in paths:
        if not zipfile.is_zipfile(path):
            out.append(path)
            continue
        with zipfile.ZipFile(path) as archive:
            archive.extractall(out_dir)
            names = [n.replace("\\", "/") for n in archive.namelist() if not n.endswith("/")]
        for name in names:
            if ".gdb/" in name.lower():
                gdb = out_dir / name[: name.lower().index(".gdb/") + 4]
                if gdb not in out:
                    out.append(gdb)
            else:
                out.append(out_dir / name)
    return out


def fetch_download(source: SourceConfig, bbox: Bbox, out_dir: Path) -> list[Path]:
    """Order the dataset from nedlasting.geonorge.no, download it, and unzip it.

    Requires ``GEONORGE_EMAIL``; anonymous orders are fine for open data.
    """
    email = os.environ.get(EMAIL_ENV)
    if not email:
        raise MissingEmailError(f"{EMAIL_ENV} is required for Geonorge download datasets")
    username, password = os.environ.get(USERNAME_ENV), os.environ.get(PASSWORD_ENV)
    auth = (username, password) if username and password else None

    uuid = str(source.dataset_id)
    capabilities = _nedlasting(f"capabilities/{uuid}")
    formats = _nedlasting(f"codelists/format/{uuid}")
    projections = _nedlasting(f"codelists/projection/{uuid}")
    # only the 'Hele landet' fallback needs the area codelist
    areas = [] if capabilities.get("supportsPolygonSelection") else _nedlasting(f"codelists/area/{uuid}")
    order = build_order(uuid, bbox, capabilities, formats, projections, areas, email)
    response = _session.post(f"{NEDLASTING_API}/order", json=order, auth=auth, timeout=60)
    if not response.ok and "restricted" in response.text.lower():
        # a role-gated dataset is refused at order time with HTTP 500 and a plain-text reason
        raise RestrictedDatasetError(f"{RESTRICTED_MESSAGE} (Geonorge: {response.text.strip()[:200]})")
    response.raise_for_status()
    files = _await_order(response.json(), auth)
    return _unzip_all(_download_order_files(files, out_dir, auth), out_dir / "unzipped")


# --- Crop & write -----------------------------------------------------------


def _layers(path: Path) -> list[str | None]:
    """Layer names in a file; downloaded GML/FGDB hold many, WFS pages just one."""
    import pyogrio

    try:
        return [name for name, _ in pyogrio.list_layers(path)]
    except Exception:  # noqa: BLE001 - fall back to the driver's default layer
        return [None]


def _read_bbox(window: Any, layer_crs: str | None) -> tuple[float, ...] | None:
    """The window's bounds in the layer's CRS, for a pyogrio ``bbox=`` pushdown.

    The transformed bounds are an axis-aligned superset of the window (`crop`
    then clips exactly), so no in-window feature is dropped. `None` when the CRS
    is unknown, in which case the whole layer is read and clipped.
    """
    import geopandas as gpd

    if not layer_crs:
        return None
    try:
        return tuple(gpd.GeoSeries([window], crs="EPSG:4326").to_crs(layer_crs).total_bounds)
    except Exception:  # noqa: BLE001 - an unusable CRS just disables the pushdown
        return None


def crop(paths: list[Path], bbox: Bbox) -> gpd.GeoDataFrame:
    """Clip fetched vector files to the bbox and merge them into one WGS84 frame.

    Reads only the window from each layer (national download files are otherwise
    read in full), then clips exactly. Skips shapefile sidecars and doc files.
    """
    import geopandas as gpd
    import pandas as pd
    import pyogrio
    from shapely.geometry import box

    window = box(*bbox)
    frames = []
    for path in paths:
        if path.suffix.lower() in NON_VECTOR_SUFFIXES:
            continue
        for layer in _layers(path):
            try:
                read_bbox = _read_bbox(window, pyogrio.read_info(path, layer=layer).get("crs"))
            except Exception:  # noqa: BLE001 - read the whole layer instead
                read_bbox = None
            try:
                gdf = gpd.read_file(path, layer=layer, bbox=read_bbox)
            except Exception as exc:  # noqa: BLE001 - one unreadable layer must not sink the dataset
                logger.warning("unreadable layer %r in %s: %s", layer, path.name, exc)
                continue
            # geometry-less layers read as plain DataFrames; concat/clip need unique
            # row indexes and column names (some GML layers repeat gml_id)
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


def _write_geoparquet(features: gpd.GeoDataFrame, output_path: Path) -> None:
    """Write via a temporary file so an interrupted write never leaves a reusable partial file."""
    # Columns merged from several layers/pages can mix value types, which Arrow rejects.
    for column in features.columns.drop(features.geometry.name):
        if features[column].dtype == object:
            features[column] = features[column].map(lambda value: None if value is None else str(value))
    partial = output_path.with_name(f"{output_path.name}.part")
    features.to_parquet(partial)
    partial.replace(output_path)


# --- Adapter ----------------------------------------------------------------


def _result(
    source: SourceConfig,
    status: SourceStatus,
    *,
    path: Path | None = None,
    message: str | None = None,
    details: dict[str, Any] | None = None,
) -> SourceResult:
    return SourceResult(
        source=source.name,
        status=status,
        path=path,
        dataset_id=source.dataset_id,
        variables=source.variables,
        message=message,
        format="parquet",
        details=details,
    )


def fetch_geonorge(
    request: Request,
    source: SourceConfig,
    config: Config,
    request_dir: Path,
    *,
    progress: ProgressCallback = null_progress,
) -> list[SourceResult]:
    """Fetch one Geonorge dataset for the request's bbox and write it as GeoParquet."""
    import pyogrio

    output_path = _output_path(request, source, request_dir)
    if should_reuse_cache(output_path.exists(), config):
        return [_result(source, SourceStatus.REUSED, path=output_path)]

    # GDAL's GML driver otherwise fetches each page's remote XSD from the slow
    # Geonorge servers on every open (30-60 s even for a 2-feature page);
    # geometry and attributes are inferred from the data instead.
    pyogrio.set_gdal_config_options({"GML_DOWNLOAD_SCHEMA": "NO"})

    bbox = _bbox(request)
    try:
        protocol = _protocol(source)
        progress(source.name, f"fetching Geonorge dataset {source.dataset_id} ({protocol})")
        with tempfile.TemporaryDirectory(prefix="collekt-geonorge-") as scratch:
            fetch = fetch_wfs if protocol == "wfs" else fetch_download
            features = crop(fetch(source, bbox, Path(scratch)), bbox)
    except (RestrictedDatasetError, MissingEmailError) as exc:
        return [_result(source, SourceStatus.SKIPPED, message=str(exc))]
    except Exception as exc:  # noqa: BLE001 - provider/network failures are reported, not raised
        return [_result(source, SourceStatus.FAILED, message=str(exc))]

    if features.empty:
        return [_result(source, SourceStatus.SKIPPED, message="no features inside the request region")]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    _write_geoparquet(features, output_path)
    return [
        _result(
            source,
            SourceStatus.DOWNLOADED,
            path=output_path,
            details={"provider": "geonorge", "method": protocol, "features": len(features)},
        )
    ]


def plan_geonorge(request: Request, source: SourceConfig, config: Config, request_dir: Path) -> list[SourceResult]:
    """Plan the Geonorge fetch without contacting Geonorge."""
    bbox = _bbox(request)
    protocol = _protocol(source)
    if protocol == "wfs":
        planned = {
            "endpoint": source.raw.get("endpoint"),
            "type_names": list(source.raw.get("type_names") or ()),
            **_wfs_params(bbox),
        }
    else:
        planned = {"api": NEDLASTING_API, "metadataUuid": source.dataset_id, "bbox": list(bbox)}
    details = {
        "provider": "geonorge",
        "method": protocol,
        "request": planned,
        "availability": Availability(AvailabilityStatus.NOT_CHECKED, AvailabilityMethod.NOT_CHECKED).as_dict(),
    }
    return [_result(source, SourceStatus.PLANNED, path=_output_path(request, source, request_dir), details=details)]


def diagnose(config: Config, online: bool) -> list[DoctorCheck]:
    """Return Geonorge credential diagnostics (only download datasets need them)."""
    if os.environ.get(EMAIL_ENV):
        return [DoctorCheck("geonorge credentials", DoctorStatus.OK, f"found {EMAIL_ENV}")]
    return [
        DoctorCheck(
            "geonorge credentials",
            DoctorStatus.WARN,
            f"no {EMAIL_ENV} found; Geonorge download datasets will be skipped (WFS datasets are unaffected)",
        )
    ]


register_adapter(
    SourceAdapter(
        kind="geonorge",
        fetch=fetch_geonorge,
        plan=plan_geonorge,
        diagnose=diagnose,
        known_raw_keys=frozenset({"protocol", "endpoint", "type_names"}),
    )
)
