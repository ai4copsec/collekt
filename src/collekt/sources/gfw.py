"""Global Fishing Watch (GFW) Events API adapter.

Queries GFW's Events API (fishing, encounters, port visits, loitering, and AIS
gaps) for a region and time window, and writes the matching events as an
annotated Parquet file. A per-request feature source, like skytruth.py: it
emits one file for the whole request rather than one file per day.

Region filtering uses a GeoJSON geometry (the request's bbox, or its precise
`Region.geometry` polygon when given) rather than GFW's alternative named-
region filter (`region={"dataset": "public-eez-areas", "id": ...}`) - a
collekt request carries an arbitrary area of interest, not a pre-known GFW
region id. Confirmed against the installed `gfwapiclient` request models
(`EventGeometry`/`EventBaseBody`) - the client's own usage-guide docs only show
the named-region form, but `geometry` is a first-class sibling parameter.

Two response quirks worth knowing when reading the output:

- `start`/`end` filtering is by *overlap*, not by event start: a years-long
  port visit that merely spans the requested window is returned with its true
  (much wider) start/end, not clipped to the request.
- GFW's `end_date` is exclusive (per `gfwapiclient`'s own request-model
  docstring), so this adapter queries through `request.end_datetime`'s day
  plus one, to include events on the last requested day.

Each of `encounter_json`/`fishing_json`/`gap_json`/`loitering_json`/
`port_visit_json` holds the type-specific detail block (only one populated per
row, matching the event's `type`) as JSON text rather than a typed struct
column - GFW's response models allow undocumented extra fields (`extra=
"allow"`), so a fixed struct type would be one field addition away from
silently dropping data.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from typing import Any

from collekt.core.availability import Availability, AvailabilityMethod, AvailabilityStatus
from collekt.core.config import Config, SourceConfig
from collekt.core.diagnostics import DoctorCheck, DoctorStatus
from collekt.core.naming import format_pattern, pattern_values
from collekt.core.request import Region, Request
from collekt.sources.base import (
    ProgressCallback,
    SourceAdapter,
    SourceResult,
    SourceStatus,
    null_progress,
    register_adapter,
    should_reuse_cache,
)

SPEC_YAML = Path(__file__).parent / "gfw.spec.yaml"
DEFAULT_DATASET_ID = "gfw-events"
DEFAULT_DATASETS = (
    "public-global-fishing-events:latest",
    "public-global-port-visits-events:latest",
    "public-global-encounters-events:latest",
    "public-global-loitering-events:latest",
    "public-global-gaps-events:latest",
)
GFW_TOKEN_ENV = "GFW_API_ACCESS_TOKEN"


def _output_path(request: Request, source: SourceConfig, request_dir: Path) -> Path:
    values = pattern_values(
        source=source.name,
        dataset_id=source.dataset_id or DEFAULT_DATASET_ID,
        region=request.region,
        start=request.start_datetime,
        end=request.end_datetime,
    )
    # pattern_values' bbox_hash covers only the bounding box, but _geometry() below queries the
    # precise Region.geometry polygon when one is given - two different polygons sharing a bbox
    # would otherwise collide on the same bbox_hash-keyed output path, and should_reuse_cache
    # would silently serve one polygon's events for the other's request.
    values["geometry_hash"] = _geometry_hash(request.region)
    return request_dir / source.path / format_pattern(source.filename_pattern, values)


def _geometry_hash(region: Region) -> str:
    """Short, `_`-prefixed hash of `region.geometry`, or `""` when the region is a plain bbox."""
    if not region.geometry:
        return ""
    return "_" + hashlib.sha1(region.geometry.encode("utf-8")).hexdigest()[:8]


def _query_window(request: Request) -> tuple[str, str]:
    """Return (start_date, end_date) as ISO dates, with end_date exclusive-adjusted (see module docstring)."""
    start_date = request.start_datetime.date().isoformat()
    end_date = (request.end_datetime.date() + timedelta(days=1)).isoformat()
    return start_date, end_date


def _geometry(region: Region) -> dict[str, Any]:
    """Return a GeoJSON Polygon for `region`: its precise geometry if given, else its bbox."""
    if region.geometry:
        import shapely.wkt
        from shapely.geometry import mapping

        return mapping(shapely.wkt.loads(region.geometry))
    return {
        "type": "Polygon",
        "coordinates": [
            [
                [region.west, region.south],
                [region.east, region.south],
                [region.east, region.north],
                [region.west, region.north],
                [region.west, region.south],
            ]
        ],
    }


def _json_or_none(value: Any) -> str | None:
    return None if value is None else json.dumps(value, default=str)


def _flatten_event(event: Any) -> dict[str, Any]:
    """Flatten one gfwapiclient `EventItem` into a row matching `gfw.spec.yaml`."""
    d = event.model_dump()
    position = d.get("position") or {}
    distances = d.get("distances") or {}
    vessel = d.get("vessel") or {}
    return {
        "id": d.get("id"),
        "type": d.get("type"),
        "start": d.get("start"),
        "end": d.get("end"),
        "lat": position.get("lat"),
        "lon": position.get("lon"),
        "bounding_box": d.get("bounding_box"),
        "start_distance_from_shore_km": distances.get("start_distance_from_shore_km"),
        "end_distance_from_shore_km": distances.get("end_distance_from_shore_km"),
        "start_distance_from_port_km": distances.get("start_distance_from_port_km"),
        "end_distance_from_port_km": distances.get("end_distance_from_port_km"),
        "vessel_id": vessel.get("id"),
        "vessel_name": vessel.get("name"),
        "vessel_ssvid": vessel.get("ssvid"),
        "vessel_flag": vessel.get("flag"),
        "vessel_type": vessel.get("type"),
        "regions": d.get("regions"),
        "encounter_json": _json_or_none(d.get("encounter")),
        "fishing_json": _json_or_none(d.get("fishing")),
        "gap_json": _json_or_none(d.get("gap")),
        "loitering_json": _json_or_none(d.get("loitering")),
        "port_visit_json": _json_or_none(d.get("port_visit")),
    }


async def _get_all_events(source: SourceConfig, region: Region, start_date: str, end_date: str) -> list[dict[str, Any]]:
    import gfwapiclient as gfw

    client = gfw.Client(access_token=os.environ.get(GFW_TOKEN_ENV, ""))
    try:
        datasets = list(source.raw.get("datasets", DEFAULT_DATASETS))
        result = await client.events.get_all_events(
            datasets=datasets,
            start_date=start_date,
            end_date=end_date,
            geometry=_geometry(region),
            # No in-code fallback: the shipped default (gfw.yaml) is the single place that sets
            # it - omitting `limit` here entirely falls through to gfwapiclient's own default
            # (99999).
            limit=source.raw.get("limit"),
        )
        return [_flatten_event(event) for event in result.data()]
    finally:
        # gfw.Client has no public close()/aclose() or context-manager support of its own - it
        # only wraps an httpx.AsyncClient subclass (HTTPClient) as a private attribute. Without
        # this, each query would leak its connection pool in a long-lived process.
        await client._http_client.aclose()


def _fetch_events(source: SourceConfig, region: Region, start_date: str, end_date: str) -> list[dict[str, Any]]:
    """Query the GFW Events API. Sync wrapper around the async client, and the network boundary
    tests stub out (`fetch_gfw` is called synchronously - see test_sources_features.py).

    `asyncio.run` refuses to run inside an already-running event loop, which is exactly the
    situation in a Jupyter kernel (ipykernel >= 7 executes every cell inside the loop). collekt's
    own notebooks call `Fetcher.download()` synchronously, so the query is handed to a worker
    thread with its own loop whenever one is already running in this thread.
    """

    def _run() -> list[dict[str, Any]]:
        return asyncio.run(_get_all_events(source, region, start_date, end_date))

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return _run()
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(_run).result()


def fetch_gfw(
    request: Request,
    source: SourceConfig,
    config: Config,
    request_dir: Path,
    *,
    progress: ProgressCallback = null_progress,
) -> list[SourceResult]:
    """Query the GFW Events API and write matching events as Parquet."""
    out_dir = request_dir / source.path
    out_dir.mkdir(parents=True, exist_ok=True)
    dataset_id = source.dataset_id or DEFAULT_DATASET_ID
    output_path = _output_path(request, source, request_dir)
    if should_reuse_cache(output_path.exists(), config):
        return [
            SourceResult(
                source=source.name,
                status=SourceStatus.REUSED,
                path=output_path,
                dataset_id=dataset_id,
                variables=source.variables,
                format="parquet",
            )
        ]

    start_date, end_date = _query_window(request)
    progress(source.name, "querying the GFW Events API")
    try:
        events = _fetch_events(source, request.region, start_date, end_date)
    except Exception as exc:  # noqa: BLE001 - network/auth/response failures are warnings, not fatal errors
        return [
            SourceResult(
                source=source.name,
                status=SourceStatus.SKIPPED,
                dataset_id=dataset_id,
                variables=source.variables,
                message=str(exc),
                format="parquet",
            )
        ]

    if not events:
        return [
            SourceResult(
                source=source.name,
                status=SourceStatus.SKIPPED,
                dataset_id=dataset_id,
                variables=source.variables,
                message="the API returned 0 events for this query",
                format="parquet",
            )
        ]

    try:
        _write_parquet(events, output_path, source)
    except Exception as exc:  # noqa: BLE001 - e.g. missing optional deps (damast) are warnings
        return [
            SourceResult(
                source=source.name,
                status=SourceStatus.SKIPPED,
                dataset_id=dataset_id,
                variables=source.variables,
                message=str(exc),
                format="parquet",
            )
        ]
    return [
        SourceResult(
            source=source.name,
            status=SourceStatus.DOWNLOADED,
            path=output_path,
            dataset_id=dataset_id,
            variables=source.variables,
            format="parquet",
            details={"provider": "gfw", "events": len(events)},
        )
    ]


def _write_parquet(events: list[dict[str, Any]], output_path: Path, source: SourceConfig) -> None:
    import damast
    import polars as pl

    df = pl.DataFrame(events).with_columns(
        pl.col("start").cast(pl.Datetime(time_unit="us", time_zone="UTC")),
        pl.col("end").cast(pl.Datetime(time_unit="us", time_zone="UTC")),
        pl.col("lat").cast(pl.Float64),
        pl.col("lon").cast(pl.Float64),
        pl.col("bounding_box").cast(pl.List(pl.Float64)),
    )
    datasets = list(source.raw.get("datasets", DEFAULT_DATASETS))
    metadata = damast.core.MetaData.load_yaml(SPEC_YAML)
    metadata.add_annotation(
        damast.core.Annotation(name=damast.core.Annotation.Key.Comment, value=f"Created from GFW datasets: {datasets}")
    )
    adf = damast.core.AnnotatedDataFrame(
        dataframe=df, metadata=metadata, validation_mode=damast.core.ValidationMode.UPDATE_DATA
    )
    adf.export(output_path)


def plan_gfw(request: Request, source: SourceConfig, config: Config, request_dir: Path) -> list[SourceResult]:
    """Plan the GFW query without contacting the API."""
    dataset_id = source.dataset_id or DEFAULT_DATASET_ID
    start_date, end_date = _query_window(request)
    details = {
        "provider": "gfw",
        "method": "events.get_all_events",
        "request": {
            "datasets": list(source.raw.get("datasets", DEFAULT_DATASETS)),
            "start_date": start_date,
            "end_date": end_date,
            "geometry": _geometry(request.region),
            "limit": source.raw.get("limit"),
        },
        "availability": Availability(AvailabilityStatus.NOT_CHECKED, AvailabilityMethod.NOT_CHECKED).as_dict(),
    }
    return [
        SourceResult(
            source=source.name,
            status=SourceStatus.PLANNED,
            path=_output_path(request, source, request_dir),
            dataset_id=dataset_id,
            variables=source.variables,
            format="parquet",
            details=details,
        )
    ]


def diagnose(config: Config, online: bool) -> list[DoctorCheck]:
    """Return GFW credential diagnostics."""
    if os.environ.get(GFW_TOKEN_ENV):
        return [DoctorCheck("gfw credentials", DoctorStatus.OK, f"found {GFW_TOKEN_ENV}")]
    return [
        DoctorCheck(
            "gfw credentials",
            DoctorStatus.WARN,
            f"no {GFW_TOKEN_ENV} found; GFW queries will fail",
        )
    ]


register_adapter(
    SourceAdapter(
        kind="gfw",
        fetch=fetch_gfw,
        plan=plan_gfw,
        diagnose=diagnose,
        known_raw_keys=frozenset({"datasets", "limit"}),
    )
)
