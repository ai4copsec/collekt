"""Shared helpers for reading damast-annotated tabular files by day and region.

Used by adapters that serve row-oriented data (AIS positions, say) out of files on disk:
`local` reads a pre-existing archive, `file_archive` a mirror it downloads itself. Both
select one UTC day's rows within the request region and keep the damast column metadata.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

from collekt.core.request import Region


class TabularConfigError(ValueError):
    """The source's configured columns do not match the files it points at.

    Kept apart from the errors a single unreadable file raises: a column that does not exist is
    a configuration mistake affecting every day equally, so it is raised rather than reported as
    a per-day skip that reads like "this archive has no data".
    """


def day_bounds(day: date) -> tuple[datetime, datetime]:
    """Inclusive ``[start, end]`` for `day`, the range `damast.core.SaveAs.expected_paths` takes."""
    return datetime.combine(day, time.min, tzinfo=UTC), datetime.combine(day, time.max, tzinfo=UTC)


def day_range(day: date) -> tuple[datetime, datetime]:
    """Half-open ``[start, next_day)`` for `day` - exact whatever the column's time resolution."""
    start = datetime.combine(day, time.min, tzinfo=UTC)
    return start, start + timedelta(days=1)


def layout_files(archive_root: Path, layout: str, day: date) -> list[Path]:
    """Return the files of a damast time-partitioned archive that may hold `day`'s rows.

    The candidates come from the archive's own layout (`damast.core.SaveAs.expected_paths`), so
    nothing is read to find them; glob wildcards in the layout are resolved against the disk.

    Example:

    ```python
    layout_files(Path("mirror"), "time:timestamp+daily:%Y/%m/ais_%Y-%m-%d", date(2024, 3, 1))
    ```

    Args:
        archive_root: Directory the layout's paths are relative to.
        layout: A ``time:``/``time+column:`` `SaveAs` partitioning spec.
        day: UTC day to look up.

    Returns:
        The existing candidate files, in layout order.
    """
    import damast

    start, end = day_bounds(day)
    matches: list[Path] = []
    for candidate in damast.core.SaveAs.expected_paths(layout, start=start, end=end):
        if "*" in candidate.as_posix():
            matches.extend(sorted(archive_root.glob(str(candidate))))
            continue
        full = archive_root / candidate
        if full.exists():
            matches.append(full)
            continue
        # `expected_paths` appends damast's own extension, so an archive written with a
        # compression suffix (`<name>.zst.parquet`) never equals `<name>.parquet`. Allow exactly
        # one extra dot-separated segment, which picks up `.zst`/`.gz`/`.snappy` without also
        # matching an unrelated `<name>_v2.parquet`.
        compressed = f"{candidate.with_suffix('').as_posix()}.*{candidate.suffix}"
        matches.extend(sorted(archive_root.glob(compressed)))
    return matches


def require_columns(schema, columns) -> None:
    """Raise `TabularConfigError` unless every one of `columns` is in `schema`.

    Args:
        schema: The polars schema of the data.
        columns: Configured column names.

    Raises:
        TabularConfigError: A configured column is missing.
    """
    missing = sorted({c for c in columns if c not in schema})
    if missing:
        raise TabularConfigError(
            f"column(s) {', '.join(missing)} not found in the archive. Available columns: {', '.join(schema.names())}"
        )


def narrowed_metadata(metadata, columns):
    """Return `metadata` restricted to `columns`.

    `AnnotatedDataFrame` validates its metadata against the frame on construction, and a column
    described by the metadata but absent from the frame is rejected whatever the `ValidationMode`
    - so a `variables` selection needs the metadata narrowed to match, not validation relaxed.
    """
    import damast

    kept = set(columns)
    annotations = metadata.annotations
    return damast.core.MetaData(
        columns=[c for c in metadata.columns if c.name in kept],
        annotations=list(annotations.values()) if isinstance(annotations, dict) else list(annotations),
    )


def read_day_subset(
    files: list[Path],
    *,
    time_column: str,
    lat_column: str,
    lon_column: str,
    day: date,
    region: Region,
    variables: tuple[str, ...],
    end_day: date | None = None,
    time_format: str | None = None,
    validation_mode=None,
):
    """Read `files` and return (rows for `day` within `region`, damast metadata).

    The rows are filtered on time as well as region, so callers can pass files that span more
    than the day (a whole unpartitioned archive, or a monthly file) without leaking other days.

    Args:
        files: Data files to read together; damast picks up an embedded or sidecar spec.
        time_column: Column holding each row's timestamp.
        lat_column: Column holding each row's latitude.
        lon_column: Column holding each row's longitude.
        day: First UTC day to keep.
        region: Request region the rows must fall in.
        variables: Columns to keep; empty keeps all.
        end_day: Last UTC day to keep (inclusive); defaults to `day`.
        time_format: `strptime` format to parse `time_column` with when it is stored as text,
            e.g. a CSV read as-is. Parsed values are taken as UTC; rows that do not parse are
            dropped.
        validation_mode: damast `ValidationMode` applied while loading; damast's default
            (`READONLY`) when `None`.

    Returns:
        The collected rows and the damast metadata they were loaded with.

    Raises:
        TabularConfigError: A configured column is missing from the files.
    """
    import damast
    import polars

    kwargs = {} if validation_mode is None else {"validation_mode": validation_mode}
    adf = damast.core.AnnotatedDataFrame.from_files([str(f) for f in files], metadata_required=False, **kwargs)
    lazy = adf.lazyframe
    schema = lazy.collect_schema()
    require_columns(schema, (time_column, lat_column, lon_column))

    if time_format is not None and schema[time_column] == polars.String:
        parsed = polars.col(time_column).str.strptime(polars.Datetime("us", "UTC"), time_format, strict=False)
        lazy = lazy.with_columns(parsed)
        schema = lazy.collect_schema()

    start, end = day_range(day)
    if end_day is not None:
        end = day_range(end_day)[1]
    if getattr(schema.get(time_column), "time_zone", None) is None:
        # A naive `time_column` is treated as already UTC (collekt's own datetime parsing does
        # the same, see `_parse_datetime`), not as a value to localize.
        start, end = start.replace(tzinfo=None), end.replace(tzinfo=None)
    lazy = lazy.filter(
        polars.col(time_column).is_between(start, end, closed="left")
        & polars.col(lat_column).is_between(region.south, region.north)
        & polars.col(lon_column).is_between(region.west, region.east)
    )
    if variables:
        lazy = lazy.select(list(variables))
    return lazy.collect(), adf.metadata
