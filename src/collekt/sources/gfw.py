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

The Events endpoint returns one fixed-size page per POST and reports no total,
so this adapter pages on `offset` until a short page arrives - otherwise a
result that happened to fill the page would be silently truncated. Two config
keys shape that, both optional:

- `limit` is the *page size* (GFW's own `limit` parameter), the same meaning the
  key has in skytruth.py. Left unset it falls through to `gfwapiclient`'s
  default of 99999, which is the right default here: a query costs 10-20s
  server-side almost regardless of how many rows it returns, so the cost is per
  request, not per row. A large page keeps the common case to a single POST and
  leaves paging as the safety net it should be. Set it smaller only to bound
  per-response size, at roughly 10-20s per extra page.
- `max_events` caps the total across pages. Unset (the default) means "collect
  everything". When a cap does cut a result short the adapter raises
  `GFWTruncatedResultWarning` - truncation is never silent.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import warnings
from collections.abc import Callable
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
# `gfwapiclient`'s own fallback: EventResource._prepare_get_all_events_request_params does
# `"limit": limit or 99999`, so an unset (or 0) `limit` reaches the API as 99999. The paging
# loop needs to know the page size actually in force to recognize a short - i.e. final - page.
CLIENT_DEFAULT_LIMIT = 99999
# Offset paging is only coherent against a stable order, and the endpoint's default order is
# unspecified. Verified against the live API: 197 events read 20-at-a-time over 10 pages
# reassemble to exactly the single-request result, same ids in the same order.
EVENT_SORT = "+start"
# Text columns, cast explicitly so an event type absent from this result does not leave a Null
# column for damast to repair. Mirrors skytruth.py's SKYTRUTH_STRING_COLUMNS.
GFW_STRING_COLUMNS = (
    "id",
    "type",
    "vessel_id",
    "vessel_name",
    "vessel_ssvid",
    "vessel_flag",
    "vessel_type",
    "encounter_json",
    "fishing_json",
    "gap_json",
    "loitering_json",
    "port_visit_json",
)


class GFWTruncatedResultWarning(UserWarning):
    """Warns that a configured `max_events` cap cut a GFW result short."""


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
    # Same reasoning for `max_events`: it changes how many events land in the file, so a capped
    # and an uncapped run of the same request must not share a cache path - otherwise lifting the
    # cap would keep serving the truncated file. `limit` is deliberately *not* part of the key:
    # it is only the page size, and paging to exhaustion makes the contents identical either way.
    values["cap_suffix"] = _cap_suffix(source)
    return request_dir / source.path / format_pattern(source.filename_pattern, values)


def _geometry_hash(region: Region) -> str:
    """Short, `_`-prefixed hash of `region.geometry`, or `""` when the region is a plain bbox."""
    if not region.geometry:
        return ""
    return "_" + hashlib.sha1(region.geometry.encode("utf-8")).hexdigest()[:8]


def _cap_suffix(source: SourceConfig) -> str:
    """Short, `_`-prefixed tag for a `max_events` cap, or `""` when the query is uncapped."""
    cap = _max_events(source)
    return "" if cap is None else f"_max{cap}"


def _query_window(request: Request) -> tuple[str, str]:
    """Return (start_date, end_date) as ISO dates, with end_date exclusive-adjusted (see module docstring)."""
    start_date = request.start_datetime.date().isoformat()
    end_date = (request.end_datetime.date() + timedelta(days=1)).isoformat()
    return start_date, end_date


def _geometry(region: Region) -> dict[str, Any]:
    """Return a GeoJSON Polygon for `region`: its precise geometry if given, else its bbox.

    Raises:
        ValueError: `region.geometry` is WKT for something other than a Polygon (e.g. a
            MultiPolygon from a multi-feature AOI, unioned by `Region.from_geojson`).
            `gfwapiclient`'s `EventGeometry.type` is an untyped `str`, so a MultiPolygon would
            otherwise reach the API unchecked and fail there instead of here.
    """
    if region.geometry:
        import shapely.wkt
        from shapely.geometry import mapping

        geometry = shapely.wkt.loads(region.geometry)
        if geometry.geom_type != "Polygon":
            raise ValueError(
                f"gfw requires a Polygon region, got {geometry.geom_type}. Split the area of "
                "interest into separate Polygon requests."
            )
        return mapping(geometry)
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


def _datasets(source: SourceConfig) -> list[str]:
    """Return the GFW dataset ids to query."""
    return list(source.raw.get("datasets", DEFAULT_DATASETS))


def _page_size(source: SourceConfig) -> int | None:
    """Return the configured page size, or `None` to accept the client's own default.

    A non-positive `limit` is treated as unset rather than as "no events", because that is
    what the client does with it (`limit or 99999`).
    """
    limit = source.raw.get("limit")
    if limit is None:
        return None
    limit = int(limit)
    return limit if limit > 0 else None


def _max_events(source: SourceConfig) -> int | None:
    """Return the configured total cap across pages, or `None` for "collect everything"."""
    cap = source.raw.get("max_events")
    if cap is None:
        return None
    cap = int(cap)
    return cap if cap > 0 else None


async def _collect_pages(
    events: Any,
    *,
    datasets: list[str],
    start_date: str,
    end_date: str,
    geometry: dict[str, Any],
    page_size: int | None,
    max_events: int | None,
    on_page: Callable[[int, int], None] = lambda _page, _total: None,
) -> list[dict[str, Any]]:
    """Page through the Events API on `offset` and return every matching event.

    `events` is the client's events resource (`gfw.Client().events`), taken as a parameter so
    the loop can be exercised without a client or a network. Paging stops at the first short
    page - the endpoint reports no total, so a full page is the only signal that more may
    follow - or at `max_events`, which warns rather than truncating silently.

    Args:
        events: Events resource exposing `get_all_events`.
        datasets: GFW dataset ids to query.
        start_date: Inclusive ISO start date.
        end_date: Exclusive ISO end date (see the module docstring).
        geometry: GeoJSON geometry to filter on.
        page_size: Rows per request, or `None` for the client's default.
        max_events: Total cap across pages, or `None` for no cap.
        on_page: Called with (page number, running total) after each page.

    Returns:
        Flattened event rows, de-duplicated by event id, in the order returned.

    Raises:
        GFWTruncatedResultWarning: Warned (not raised) when `max_events` cuts the result short.
    """
    in_force = page_size or CLIENT_DEFAULT_LIMIT
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    offset = 0
    page_number = 0
    capped = False

    while True:
        result = await events.get_all_events(
            datasets=datasets,
            start_date=start_date,
            end_date=end_date,
            geometry=geometry,
            limit=page_size,
            offset=offset,
            sort=EVENT_SORT,
        )
        page = [_flatten_event(event) for event in result.data()]
        page_number += 1
        for row in page:
            # A row without an id cannot be de-duplicated; keep it rather than drop it.
            identifier = row.get("id")
            if identifier is not None:
                if identifier in seen:
                    continue
                seen.add(identifier)
            rows.append(row)
        on_page(page_number, len(rows))

        if max_events is not None and len(rows) >= max_events:
            capped = len(rows) > max_events or len(page) == in_force
            rows = rows[:max_events]
            break
        if len(page) < in_force:
            break
        offset += len(page)

    if capped:
        warnings.warn(
            f"GFW returned at least {max_events} events; stopping at the configured "
            f"max_events={max_events}. Raise or unset `max_events` on the gfw source, or "
            f"narrow the region/time window, to collect the rest.",
            GFWTruncatedResultWarning,
            stacklevel=2,
        )
    return rows


async def _get_all_events(
    source: SourceConfig,
    region: Region,
    start_date: str,
    end_date: str,
    on_page: Callable[[int, int], None] = lambda _page, _total: None,
) -> list[dict[str, Any]]:
    import gfwapiclient as gfw

    client = gfw.Client(access_token=os.environ.get(GFW_TOKEN_ENV, ""))
    try:
        return await _collect_pages(
            client.events,
            datasets=_datasets(source),
            start_date=start_date,
            end_date=end_date,
            geometry=_geometry(region),
            page_size=_page_size(source),
            max_events=_max_events(source),
            on_page=on_page,
        )
    finally:
        # gfw.Client has no public close()/aclose() or context-manager support of its own - it
        # only wraps an httpx.AsyncClient subclass (HTTPClient) as a private attribute. Without
        # this, each query would leak its connection pool in a long-lived process.
        await client._http_client.aclose()


def _fetch_events(
    source: SourceConfig,
    region: Region,
    start_date: str,
    end_date: str,
    on_page: Callable[[int, int], None] = lambda _page, _total: None,
) -> list[dict[str, Any]]:
    """Query the GFW Events API. Sync wrapper around the async client, and the network boundary
    tests stub out (`fetch_gfw` is called synchronously - see test_sources_features.py).

    `asyncio.run` refuses to run inside an already-running event loop, which is exactly the
    situation in a Jupyter kernel (ipykernel >= 7 executes every cell inside the loop). collekt's
    own notebooks call `Fetcher.download()` synchronously, so the query is handed to a worker
    thread with its own loop whenever one is already running in this thread.

    A `GFWTruncatedResultWarning` raised in that worker thread would be invisible to a caller's
    `warnings.catch_warnings`, so the threaded path collects warnings and re-raises them here.
    Both paths therefore warn in the caller's thread. Swapping the global filter is safe: this
    thread is blocked on the result, so nothing else in it can warn meanwhile.
    """

    def _run() -> list[dict[str, Any]]:
        return asyncio.run(_get_all_events(source, region, start_date, end_date, on_page))

    def _run_capturing() -> tuple[list[dict[str, Any]], list[warnings.WarningMessage]]:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            return _run(), caught

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return _run()

    with ThreadPoolExecutor(max_workers=1) as pool:
        rows, caught = pool.submit(_run_capturing).result()
    for entry in caught:
        warnings.warn(entry.message, entry.category, stacklevel=2)
    return rows


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

    def _on_page(page_number: int, total: int) -> None:
        # A page costs 10-20s server-side, so a multi-page query is a long silence otherwise.
        if page_number > 1:
            progress(source.name, f"querying the GFW Events API (page {page_number}, {total} events so far)")

    try:
        events = _fetch_events(source, request.region, start_date, end_date, _on_page)
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

    # infer_schema_length=None scans every row instead of the default first 100. Only one of the
    # five *_json columns is populated per event, so a rarer event type (loitering, say) can be
    # absent from the first 100 rows: polars would infer Null for that column and then fail on the
    # first string it met further down. Paging made this reachable - a truncated result could
    # easily hold one event type only.
    df = pl.DataFrame(events, infer_schema_length=None).with_columns(
        pl.col("start").cast(pl.Datetime(time_unit="us", time_zone="UTC")),
        pl.col("end").cast(pl.Datetime(time_unit="us", time_zone="UTC")),
        pl.col("lat").cast(pl.Float64),
        pl.col("lon").cast(pl.Float64),
        pl.col("bounding_box").cast(pl.List(pl.Float64)),
        # Cast explicitly rather than leave an all-null column as Null: damast repairs those with
        # a warning, and the same trick keeps skytruth.py's output quiet (SKYTRUTH_STRING_COLUMNS).
        *[pl.col(column).cast(pl.String) for column in GFW_STRING_COLUMNS],
    )
    datasets = _datasets(source)
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
            "datasets": _datasets(source),
            "start_date": start_date,
            "end_date": end_date,
            "geometry": _geometry(request.region),
            # `limit` is the page size actually sent; the query pages on `offset` until a short
            # page arrives, or until `max_events` (null = collect everything) is reached.
            "limit": _page_size(source) or CLIENT_DEFAULT_LIMIT,
            "max_events": _max_events(source),
            "sort": EVENT_SORT,
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
        known_raw_keys=frozenset({"datasets", "limit", "max_events"}),
    )
)
