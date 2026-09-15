"""Local archive adapter.

Reads a pre-existing tabular collection on disk as a collekt source, row-filtered to each
request day and region, and written into the request directory. This is the tabular analogue
of eOdyn's `mode: archive`, generalized so any local collection can be queried without
per-source code. Two ways to locate a day's rows are supported - see `_spec`:

- `layout`: the archive is already day-partitioned, e.g. via ``damast convert --save-as`` or
  `AnnotatedDataFrame.export_partitioned` - a day's file(s) are resolved from the archive's own
  layout (`damast.core.SaveAs.expected_paths`) without reading anything.
- `file_pattern` + `time_column`: the archive is not partitioned by time at all - every file
  matching `file_pattern` is globbed once and a day's rows are selected by filtering
  `time_column`, the same way `region_columns` selects rows by region.

Producing or updating the archive itself (e.g. via `damast watch`) is out of scope here - this
adapter only reads an already-existing one.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any

from collekt.core.availability import Coverage
from collekt.core.config import Config, SourceConfig
from collekt.core.naming import format_pattern, pattern_values
from collekt.core.request import Region, Request
from collekt.core.temporal import SamplingPlan, sampling_plan
from collekt.sources.base import (
    ProgressCallback,
    SourceAdapter,
    SourceResult,
    SourceStatus,
    null_progress,
    register_adapter,
    should_reuse_cache,
)
from collekt.sources.planning import plan_source, static_source_coverage

DEFAULT_DATASET_ID = "local-archive"


@dataclass(frozen=True)
class _LocalSpec:
    """Parsed ``local``-source configuration: day-partitioned via `layout`."""

    archive_root: Path
    layout: str
    lat_column: str
    lon_column: str


@dataclass(frozen=True)
class _LocalGlobSpec:
    """Parsed ``local``-source configuration: unpartitioned, day-filtered via `time_column`."""

    archive_root: Path
    file_pattern: str
    time_column: str
    lat_column: str
    lon_column: str


def _spec(source: SourceConfig) -> _LocalSpec | _LocalGlobSpec:
    archive_root = source.raw.get("archive_root")
    layout = source.raw.get("layout")
    file_pattern = source.raw.get("file_pattern")
    region_columns = source.raw.get("region_columns")
    if not archive_root:
        raise ValueError(f"local source {source.name!r}: 'archive_root' is required")
    if not region_columns or len(list(region_columns)) != 2:
        raise ValueError(f"local source {source.name!r}: 'region_columns' must list exactly [lat_column, lon_column]")
    lat_column, lon_column = region_columns
    archive_root_path = Path(str(archive_root)).expanduser()

    if layout and file_pattern:
        raise ValueError(f"local source {source.name!r}: 'layout' and 'file_pattern' are mutually exclusive")

    if file_pattern:
        time_column = source.raw.get("time_column")
        if not time_column:
            raise ValueError(f"local source {source.name!r}: 'file_pattern' requires 'time_column'")
        return _LocalGlobSpec(
            archive_root=archive_root_path,
            file_pattern=str(file_pattern),
            time_column=str(time_column),
            lat_column=str(lat_column),
            lon_column=str(lon_column),
        )

    if not layout:
        raise ValueError(f"local source {source.name!r}: one of 'layout' or 'file_pattern' is required")
    if not str(layout).startswith(("time:", "time+column:")):
        raise ValueError(
            f"local source {source.name!r}: 'layout' must be a 'time:' or 'time+column:' partitioning spec"
            " (see damast.core.SaveAs.parse) - fetch_local serves one result per request day, which needs a"
            f" time dimension to split rows by; got {layout!r}. Use 'file_pattern' + 'time_column' instead if"
            " the archive isn't partitioned by time at all."
        )
    return _LocalSpec(
        archive_root=archive_root_path,
        layout=str(layout),
        lat_column=str(lat_column),
        lon_column=str(lon_column),
    )


def _day_bounds(day: date) -> tuple[datetime, datetime]:
    return datetime.combine(day, time.min, tzinfo=UTC), datetime.combine(day, time.max, tzinfo=UTC)


def _matched_files(spec: _LocalSpec, day: date) -> list[Path]:
    """Return the archive files that may hold `day`'s data, resolving any glob wildcard."""
    import damast

    start, end = _day_bounds(day)
    matches: list[Path] = []
    for candidate in damast.core.SaveAs.expected_paths(spec.layout, start=start, end=end):
        if "*" in candidate.as_posix():
            matches.extend(sorted(spec.archive_root.glob(str(candidate))))
        else:
            full = spec.archive_root / candidate
            if full.exists():
                matches.append(full)
    return matches


def _read_region_subset(files: list[Path], spec: _LocalSpec, region: Region, variables: tuple[str, ...]):
    """Read `files` and return (row-filtered-to-`region` DataFrame, its damast metadata)."""
    import damast
    import polars

    adf = damast.core.AnnotatedDataFrame.from_files([str(f) for f in files], metadata_required=False)
    lazy = adf.lazyframe.filter(
        polars.col(spec.lat_column).is_between(region.south, region.north)
        & polars.col(spec.lon_column).is_between(region.west, region.east)
    )
    if variables:
        lazy = lazy.select(list(variables))
    return lazy.collect(), adf.metadata


def _glob_files(spec: _LocalGlobSpec) -> list[Path]:
    """Return every archive file matching `file_pattern`, resolved once per fetch (day-independent)."""
    return sorted(spec.archive_root.glob(spec.file_pattern))


def _read_glob_day_subset(
    files: list[Path], spec: _LocalGlobSpec, day: date, region: Region, variables: tuple[str, ...]
):
    """Read `files` and return (rows for `day` within `region`, damast metadata).

    Unlike `_read_region_subset`, `files` are not already day-scoped - `time_column` selects the
    day here, the same way `region_columns` selects the region.
    """
    import damast
    import polars

    start, end = _day_bounds(day)
    adf = damast.core.AnnotatedDataFrame.from_files([str(f) for f in files], metadata_required=False)
    lazy = adf.lazyframe
    time_dtype = lazy.collect_schema().get(spec.time_column)
    if getattr(time_dtype, "time_zone", None) is None:
        # A naive `time_column` is treated as already UTC (collekt's own datetime parsing does
        # the same, see `_parse_datetime`), not as a value to localize.
        start, end = start.replace(tzinfo=None), end.replace(tzinfo=None)
    lazy = lazy.filter(
        polars.col(spec.time_column).is_between(start, end)
        & polars.col(spec.lat_column).is_between(region.south, region.north)
        & polars.col(spec.lon_column).is_between(region.west, region.east)
    )
    if variables:
        lazy = lazy.select(list(variables))
    return lazy.collect(), adf.metadata


def fetch_local(
    request: Request,
    source: SourceConfig,
    config: Config,
    request_dir: Path,
    *,
    progress: ProgressCallback = null_progress,
) -> list[SourceResult]:
    """Serve a local, damast-managed tabular archive as a collekt source.

    Each requested day's rows are read, filtered to the request's region (`region_columns`), and
    written as a single Parquet file per day into `request_dir`. A day with no matching file, or
    no in-region rows, is skipped rather than treated as an error. See the module docstring for
    the two ways (`layout` vs `file_pattern` + `time_column`) a day's rows can be located.
    """
    import damast

    spec = _spec(source)
    dataset_id = source.dataset_id or DEFAULT_DATASET_ID
    out_dir = request_dir / source.path
    out_dir.mkdir(parents=True, exist_ok=True)
    plan = sampling_plan(request, source)

    # Glob mode resolves the candidate files once - unlike `layout`, they carry no per-day
    # information, so the same file list is filtered by `time_column` for every request day.
    glob_matched = _glob_files(spec) if isinstance(spec, _LocalGlobSpec) else None

    results: list[SourceResult] = []
    for day in request.iter_days():
        temporal = plan.day_dict(day)
        values = pattern_values(
            source=source.name,
            dataset_id=dataset_id,
            region=request.region,
            start=request.start_datetime,
            end=request.end_datetime,
            day=day,
        )
        output_path = out_dir / format_pattern(source.filename_pattern, values)

        if should_reuse_cache(output_path.exists(), config):
            results.append(
                SourceResult(
                    source=source.name,
                    status=SourceStatus.REUSED,
                    path=output_path,
                    dataset_id=dataset_id,
                    variables=source.variables,
                    day=day.isoformat(),
                    format="parquet",
                    details={"temporal": temporal, "archive_root": str(spec.archive_root)},
                )
            )
            continue

        if isinstance(spec, _LocalGlobSpec):
            matched = glob_matched
            no_match_message = f"no file matches {spec.file_pattern!r} under {spec.archive_root}"
            no_match_details = {
                "temporal": temporal,
                "archive_root": str(spec.archive_root),
                "file_pattern": spec.file_pattern,
            }
        else:
            matched = _matched_files(spec, day)
            no_match_message = f"no archive file matches {day.isoformat()} under {spec.archive_root}"
            no_match_details = {"temporal": temporal, "archive_root": str(spec.archive_root), "layout": spec.layout}

        if not matched:
            results.append(
                SourceResult(
                    source=source.name,
                    status=SourceStatus.SKIPPED,
                    dataset_id=dataset_id,
                    variables=source.variables,
                    day=day.isoformat(),
                    message=no_match_message,
                    details=no_match_details,
                )
            )
            continue

        progress(source.name, f"reading {day.isoformat()} from {len(matched)} local file(s)")
        try:
            if isinstance(spec, _LocalGlobSpec):
                collected, metadata = _read_glob_day_subset(matched, spec, day, request.region, source.variables)
            else:
                collected, metadata = _read_region_subset(matched, spec, request.region, source.variables)
        except Exception as exc:  # noqa: BLE001 - a malformed/missing-column archive file is a warning, not a crash
            results.append(
                SourceResult(
                    source=source.name,
                    status=SourceStatus.SKIPPED,
                    dataset_id=dataset_id,
                    variables=source.variables,
                    day=day.isoformat(),
                    message=str(exc),
                    details={"temporal": temporal, "matched_files": [str(f) for f in matched]},
                )
            )
            continue

        if collected.height == 0:
            results.append(
                SourceResult(
                    source=source.name,
                    status=SourceStatus.SKIPPED,
                    dataset_id=dataset_id,
                    variables=source.variables,
                    day=day.isoformat(),
                    message=f"no rows within the request region for {day.isoformat()}",
                    details={"temporal": temporal, "matched_files": [str(f) for f in matched]},
                )
            )
            continue

        subset = damast.core.AnnotatedDataFrame(collected, metadata, validation_mode=damast.core.ValidationMode.IGNORE)
        subset.export(output_path)
        results.append(
            SourceResult(
                source=source.name,
                status=SourceStatus.DOWNLOADED,
                path=output_path,
                dataset_id=dataset_id,
                variables=source.variables,
                day=day.isoformat(),
                format="parquet",
                details={
                    "provider": "local-archive",
                    "method": "archive",
                    "temporal": temporal,
                    "archive_root": str(spec.archive_root),
                    "matched_files": [str(f) for f in matched],
                    "rows": collected.height,
                },
            )
        )
    return results


def _local_dataset_for_day(source: SourceConfig, day: date) -> str:
    return source.dataset_id or DEFAULT_DATASET_ID


def _local_day_details(
    request: Request, source: SourceConfig, day: date, plan: SamplingPlan, coverage: Coverage | None
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    dataset_id = source.dataset_id or DEFAULT_DATASET_ID
    spec = _spec(source)
    request_details: dict[str, Any] = {"archive_root": str(spec.archive_root)}
    if isinstance(spec, _LocalGlobSpec):
        request_details["file_pattern"] = spec.file_pattern
        request_details["time_column"] = spec.time_column
    else:
        request_details["layout"] = spec.layout
    details = {
        "provider": "local-archive",
        "method": "archive",
        "temporal": plan.day_dict(day),
        "request": request_details,
    }
    return dataset_id, details, {}


def plan_local(request: Request, source: SourceConfig, config: Config, request_dir: Path) -> list[SourceResult]:
    """Plan local-archive reads using the source's declarative coverage, if any."""
    coverage, method, checked = static_source_coverage(source)
    return plan_source(
        request,
        source,
        request_dir,
        coverage=coverage,
        method=method,
        checked=checked,
        dataset_for_day=_local_dataset_for_day,
        day_details=_local_day_details,
        planned_format="parquet",
    )


register_adapter(
    SourceAdapter(
        kind="local",
        fetch=fetch_local,
        plan=plan_local,
        known_raw_keys=frozenset({"archive_root", "layout", "region_columns", "file_pattern", "time_column"}),
    )
)
