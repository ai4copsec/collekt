"""Geonorge maritime vector-data adapter.

Fetches one curated Geonorge dataset (pinned by its catalogue UUID) for the
request's bounding box and writes the features as GeoParquet. Geonorge datasets
are static reference data, so this is a per-request source: the time window is
ignored and a single file is written for the whole request.

Each catalog entry names a WFS ``endpoint`` and the ``type_names`` to request.
Features are fetched with a server-side BBOX and then clipped
exactly to the bbox. A request whose region misses the entry's declared ``coverage``
is skipped without contacting Geonorge.
"""

from __future__ import annotations

import logging
import math
import tempfile
import time
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Any

import requests

from collekt.core.availability import (
    Availability,
    AvailabilityMethod,
    AvailabilityStatus,
    region_overlaps,
    region_reason,
    static_coverage,
)
from collekt.core.config import Config, SourceConfig
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

WFS_PAGE_SIZE = 1000
WFS_MAX_PAGES_PER_TYPE = 20
WFS_REQUEST_TIMEOUT = 30  # s; one try: GetFeature is bbox-filtered (small), so fail fast

Bbox = tuple[float, float, float, float]  # (west, south, east, north), WGS84

_session = requests.Session()


class GeonorgeTruncatedResultWarning(UserWarning):
    """Warns that a WFS feature type hit the page limit, so its result may be incomplete."""


class HostUnavailableError(Exception):
    """A WFS server failed repeatedly in this run, so further requests to it are skipped."""


# Circuit breaker: once a WFS server has failed HOST_FAILURE_LIMIT times (timeouts,
# connection errors, 502/503/504) within HOST_FAILURE_WINDOW seconds, later requests
# to it are skipped instead of each waiting out the same outage. A success clears
# the record; the window keeps an old outage from outliving the run in a long-lived
# process such as a notebook.
HOST_FAILURE_LIMIT = 2
HOST_FAILURE_WINDOW = 210  # s (3.5 min)
GATEWAY_ERRORS = frozenset({502, 503, 504})
_host_failures: dict[str, list[float]] = {}


def _recent_failures(host: str) -> list[float]:
    cutoff = time.monotonic() - HOST_FAILURE_WINDOW
    return [t for t in _host_failures.get(host, []) if t >= cutoff]


def _record_failure(host: str) -> None:
    _host_failures[host] = [*_recent_failures(host), time.monotonic()]


logger = logging.getLogger(__name__)


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


def _coverage_skip_reason(request: Request, source: SourceConfig) -> str | None:
    """Why the request region misses the entry's declared `coverage`, or `None` if it overlaps."""
    coverage = static_coverage(source)
    if coverage is None or region_overlaps(request.region, coverage):
        return None
    return region_reason(request.region, coverage)


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
    """Features in one GetFeature page, 0 for a valid empty page.

    A page GDAL cannot read at all (e.g. an HTML error or login page served with
    HTTP 200) raises, so the dataset is reported as failed rather than as empty.
    """
    import pyogrio

    if len(pyogrio.list_layers(path)) == 0:  # an empty FeatureCollection has no layer
        return 0
    return int(pyogrio.read_info(path, force_feature_count=True)["features"])


def _wfs_pages(endpoint: str, type_name: str, base_params: dict[str, Any], out_dir: Path) -> list[Path]:
    """Fetch every GetFeature page of one feature type; returns only pages with features.

    Logs each request's HTTP status, elapsed time, size and feature count, so a
    slow or failing server is visible while it happens.
    """
    host = endpoint.split("/")[2]
    paths: list[Path] = []
    params = dict(base_params, typeNames=type_name)
    safe_type = type_name.replace(":", "_")
    for page in range(WFS_MAX_PAGES_PER_TYPE):
        if page:
            # sent from the second page on only: some deegree services (e.g. Nødhavner)
            # time out on an explicit startIndex=0, although it is the default
            params["startIndex"] = page * WFS_PAGE_SIZE
        label = f"{type_name} page {page + 1}"
        if len(failures := _recent_failures(host)) >= HOST_FAILURE_LIMIT:
            raise HostUnavailableError(
                f"{host} failed {len(failures)} times in the last {HOST_FAILURE_WINDOW / 60:g} min; "
                f"not querying it again for now"
            )
        logger.info(f"GetFeature {label} from {host}")
        started = time.monotonic()
        try:
            response = _session.get(endpoint, params=params, timeout=WFS_REQUEST_TIMEOUT)
        except requests.RequestException as exc:
            _record_failure(host)
            logger.warning(f"{label}: {type(exc).__name__} after {time.monotonic() - started:.1f} s")
            raise
        elapsed = time.monotonic() - started
        if not response.ok or "ExceptionReport" in response.text[:512]:
            if response.status_code in GATEWAY_ERRORS:
                _record_failure(host)
            logger.warning(f"{label}: HTTP {response.status_code} after {elapsed:.1f} s")
            raise RuntimeError(
                f"WFS GetFeature failed for {type_name} (HTTP {response.status_code} after {elapsed:.0f} s): "
                f"{response.text[:200]}"
            )
        _host_failures.pop(host, None)  # the server answers again
        path = out_dir / f"{safe_type}_{page}.gml"
        path.write_bytes(response.content)
        count = _feature_count(path)
        logger.info(
            f"{label}: HTTP {response.status_code} in {elapsed:.1f} s, {len(response.content) / 1024:.1f} KB, "
            f"{count} features"
        )
        if count == 0:
            path.unlink()
            break
        paths.append(path)
        if count < WFS_PAGE_SIZE:
            break
    else:  # every page came back full: there may be more features than were fetched
        warnings.warn(
            f"{type_name} reached the limit of {WFS_MAX_PAGES_PER_TYPE} pages x {WFS_PAGE_SIZE} features; "
            f"the result may be incomplete. Narrow the request region to collect the rest.",
            GeonorgeTruncatedResultWarning,
            stacklevel=2,
        )
    return paths


def fetch_wfs(source: SourceConfig, bbox: Bbox, out_dir: Path) -> list[Path]:
    """GetFeature each of the source's ``type_names`` with a server-side BBOX."""
    endpoint = str(source.raw["endpoint"])
    base_params = _wfs_params(bbox)
    paths: list[Path] = []
    for type_name in source.raw.get("type_names") or ():
        paths.extend(_wfs_pages(endpoint, str(type_name), base_params, out_dir))
    return paths


# --- Crop & write -----------------------------------------------------------


def crop(paths: list[Path], bbox: Bbox) -> gpd.GeoDataFrame:
    """Clip fetched WFS pages exactly to the bbox and merge them into one WGS84 frame.

    The server-side BBOX returns every feature that touches the window, so features
    reaching outside it are cut at its edge here.
    """
    import geopandas as gpd
    import pandas as pd
    from shapely.geometry import box

    window = box(*bbox)
    frames = []
    for path in paths:
        gdf = gpd.read_file(path)
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


def _text_or_none(value: Any) -> str | None:
    """A cell as text, keeping missing values (None, float NaN) missing instead of the string 'nan'."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    return str(value)


def _write_geoparquet(features: gpd.GeoDataFrame, output_path: Path) -> None:
    """Write via a temporary file so an interrupted write never leaves a reusable partial file."""
    # Columns merged from several layers/pages can mix value types, which Arrow rejects.
    for column in features.columns.drop(features.geometry.name):
        if features[column].dtype == object:
            features[column] = features[column].map(_text_or_none)
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

    if reason := _coverage_skip_reason(request, source):
        return [_result(source, SourceStatus.SKIPPED, message=reason)]

    output_path = _output_path(request, source, request_dir)
    if should_reuse_cache(output_path.exists(), config):
        return [_result(source, SourceStatus.REUSED, path=output_path)]

    # GDAL's GML driver otherwise fetches each page's remote XSD from the slow
    # Geonorge servers on every open (30-60 s even for a 2-feature page);
    # geometry and attributes are inferred from the data instead.
    pyogrio.set_gdal_config_options({"GML_DOWNLOAD_SCHEMA": "NO"})

    bbox = _bbox(request)
    try:
        progress(source.name, f"fetching Geonorge dataset {source.dataset_id}")
        with tempfile.TemporaryDirectory(prefix="collekt-geonorge-") as scratch:
            started = time.monotonic()
            logger.info(f"Requesting {source.name} ({', '.join(source.raw.get('type_names') or ())})")
            pages = fetch_wfs(source, bbox, Path(scratch))
            size_kb = sum(page.stat().st_size for page in pages) / 1024
            features = crop(pages, bbox)
            logger.info(
                f"Fetched {source.name}: {len(features)} features inside the region, "
                f"{size_kb:.1f} KB in {len(pages)} page(s), {time.monotonic() - started:.1f} s"
            )
    except HostUnavailableError as exc:
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
            details={"provider": "geonorge", "method": "wfs", "features": len(features)},
        )
    ]


def plan_geonorge(request: Request, source: SourceConfig, config: Config, request_dir: Path) -> list[SourceResult]:
    """Plan the Geonorge fetch without contacting Geonorge."""
    coverage = static_coverage(source)
    method = AvailabilityMethod.COVERAGE if coverage else AvailabilityMethod.NOT_CHECKED
    coverage_dict = coverage.as_dict() if coverage else None
    if reason := _coverage_skip_reason(request, source):
        availability = Availability(AvailabilityStatus.UNAVAILABLE, method, reason=reason, coverage=coverage_dict)
        return [_result(source, SourceStatus.SKIPPED, message=reason, details={"availability": availability.as_dict()})]

    planned = {
        "endpoint": source.raw.get("endpoint"),
        "type_names": list(source.raw.get("type_names") or ()),
        **_wfs_params(_bbox(request)),
    }
    details = {
        "provider": "geonorge",
        "method": "wfs",
        "request": planned,
        "availability": Availability(
            AvailabilityStatus.AVAILABLE if coverage else AvailabilityStatus.NOT_CHECKED, method, coverage=coverage_dict
        ).as_dict(),
    }
    return [_result(source, SourceStatus.PLANNED, path=_output_path(request, source, request_dir), details=details)]


register_adapter(
    SourceAdapter(
        kind="geonorge",
        fetch=fetch_geonorge,
        plan=plan_geonorge,
        known_raw_keys=frozenset({"endpoint", "type_names"}),
    )
)
