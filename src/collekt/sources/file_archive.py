"""Remote file-archive adapter.

Serves providers that publish tabular data as zipped CSV files over plain HTTP, one file per day
or per month - for example the AIS archives of NOAA Marine Cadastre and the Danish Maritime
Authority (AISDK). A provider is described entirely in its catalog entry, so a further archive
of this shape needs a catalog file (and ideally a damast spec), not a new module.

Each remote file is downloaded once into a persistent mirror under `archive_root`, shared by
every request, and its CSV members are either

- converted to annotated Parquet and written with `AnnotatedDataFrame.export_partitioned` into a
  damast time-partitioned archive following `layout` (`convert: parquet`, the default). The
  mirror is then a regular damast archive that the `local` adapter or damast's own tools can read
  as well; a day's files are found from the layout alone (`SaveAs.expected_paths`). Should two
  remote files contribute rows to the same partition, the rows are merged and de-duplicated
  rather than overwritten.
- kept byte-identical in one directory per remote file, with only a damast spec sidecar
  `<member>.spec.yaml` written next to each CSV (`convert: spec_only`).

A marker under `archive_root/.mirrored/` records each completely mirrored remote file, so an
interrupted download or conversion is retried rather than mistaken for a complete one.

The damast metadata comes from the provider's bundled spec (`spec`, a file next to this module
such as `aisdk.spec.yaml`), or is inferred from the data when no spec is configured. Each request
day is then read from the mirror, filtered to that day and the request region, and written as a
single Parquet file into the request directory - the same output contract as the `local` adapter.

Configuration keys (besides the common source keys):

- `archive_root`: mirror directory. A relative path is resolved under the directory named by the
  ``COLLEKT_ARCHIVE_ROOT`` environment variable (which a project ``.env`` can set), falling back to
  ``~/.cache/collekt/archive``; an absolute path is used as is.
- `periods`: list of `{url, every: day|month, start?, until?, member?}`. `url` and `member` are
  `str.format` templates over `date`. A day uses the first period whose `start`/`until`
  (inclusive) contain it; `every: month` formats `url` with the first day of the month, so all
  days of a month share one download. `member` names the day's CSV inside a multi-day archive,
  so in `spec_only` mode a day reads only its own file.
- `time_column`, `region_columns: [lat_column, lon_column]`: columns rows are filtered on.
- `time_format`: `strptime` format of `time_column` in the CSV; parsed values are taken as UTC.
- `convert`: `parquet` or `spec_only`.
- `layout`: damast ``time:`` partitioning spec of the Parquet mirror (see `damast.core.SaveAs`);
  defaults to one file per UTC day, ``time:<time_column>+daily:%Y/%m/%Y-%m-%d``.
- `spec`: filename of the provider's damast spec next to this module.
- `coverage` (common key): the provider's region and date range. Requests outside it are skipped
  before anything is downloaded, and rows are only returned within it. Several entries can
  therefore share one `archive_root` and differ only in their region - one download serves all.

Conversion loads one CSV member at a time fully into memory (`export_partitioned` collects the
frame), which is fine for a day of AIS data (a few GB) but sets the scale this adapter targets.
"""

from __future__ import annotations

import json
import os
import shutil
import zipfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from collekt.core.availability import (
    Coverage,
    clip_region,
    day_in_range,
    day_reason,
    region_overlaps,
    region_reason,
    static_coverage,
)
from collekt.core.config import Config, SourceConfig
from collekt.core.diagnostics import DoctorCheck, package_check
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
from collekt.sources.batching.common import daily_output_errors, staged_output
from collekt.sources.batching.files import run_file_batch
from collekt.sources.planning import plan_source, static_source_coverage
from collekt.sources.tabular import TabularConfigError, layout_files, narrowed_metadata, read_day_subset

DEFAULT_DATASET_ID = "file-archive"
CONVERT_MODES = ("parquet", "spec_only")
SPEC_DIR = Path(__file__).parent
DOWNLOAD_CHUNK_BYTES = 8 * 1024 * 1024
DOWNLOAD_TIMEOUT_S = 60
ARCHIVE_ROOT_ENV = "COLLEKT_ARCHIVE_ROOT"
DEFAULT_ARCHIVE_BASE = Path("~/.cache/collekt/archive")


class RemoteFileMissingError(FileNotFoundError):
    """The provider does not serve the file a day maps to (HTTP 404)."""


@dataclass(frozen=True)
class ArchivePeriod:
    """A date range over which the provider publishes files with one naming scheme."""

    url: str
    every: str = "day"
    start: date | None = None
    until: date | None = None
    member: str | None = None

    def covers(self, day: date) -> bool:
        """Return whether `day` falls inside this period (bounds inclusive)."""
        return (self.start is None or self.start <= day) and (self.until is None or day <= self.until)

    def anchor(self, day: date) -> date:
        """Return the date the file holding `day` is named after."""
        return day.replace(day=1) if self.every == "month" else day


@dataclass(frozen=True)
class ArchiveSpec:
    """Parsed ``file_archive``-source configuration."""

    archive_root: Path
    periods: tuple[ArchivePeriod, ...]
    convert: str
    layout: str
    time_column: str
    time_format: str | None
    lat_column: str
    lon_column: str
    spec_path: Path | None


@dataclass(frozen=True)
class RemoteFile:
    """One remote archive file, and the CSV member inside it that holds a given day."""

    url: str
    stem: str
    member: str | None


def _optional_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    return value if isinstance(value, date) else date.fromisoformat(str(value))


def resolve_archive_root(value: Any) -> Path:
    """Return the mirror directory for a configured `archive_root`.

    A relative `archive_root` (as the bundled catalogs use) is placed under ``$COLLEKT_ARCHIVE_ROOT``,
    so one variable - set in the environment or a project ``.env`` - moves every mirror. Without
    it, mirrors go under ``~/.cache/collekt/archive``. An absolute `archive_root` is kept.

    Example:
        ```python
        resolve_archive_root("aisdk")  # $COLLEKT_ARCHIVE_ROOT/aisdk
        ```

    Args:
        value: The configured `archive_root`.

    Returns:
        The absolute mirror directory.
    """
    root = Path(str(value)).expanduser()
    if root.is_absolute():
        return root
    base = os.environ.get(ARCHIVE_ROOT_ENV) or DEFAULT_ARCHIVE_BASE
    return Path(base).expanduser() / root


def parse_archive_spec(source: SourceConfig) -> ArchiveSpec:
    """Parse and validate the provider keys of a ``file_archive`` source.

    Args:
        source: Resolved source configuration.

    Returns:
        The parsed configuration.

    Raises:
        ValueError: A required key is missing or a value is not supported.
    """
    raw = source.raw
    name = source.name
    if not raw.get("archive_root"):
        raise ValueError(f"file_archive source {name!r}: 'archive_root' is required")
    if not raw.get("time_column"):
        raise ValueError(f"file_archive source {name!r}: 'time_column' is required")
    region_columns = list(raw.get("region_columns") or [])
    if len(region_columns) != 2:
        raise ValueError(f"file_archive source {name!r}: 'region_columns' must list exactly [lat_column, lon_column]")
    convert = str(raw.get("convert", "parquet"))
    if convert not in CONVERT_MODES:
        raise ValueError(f"file_archive source {name!r}: 'convert' must be one of {CONVERT_MODES}, got {convert!r}")

    periods = []
    for entry in raw.get("periods") or []:
        every = str(entry.get("every", "day"))
        if every not in ("day", "month"):
            raise ValueError(f"file_archive source {name!r}: period 'every' must be 'day' or 'month', got {every!r}")
        if not entry.get("url"):
            raise ValueError(f"file_archive source {name!r}: every period needs a 'url'")
        periods.append(
            ArchivePeriod(
                url=str(entry["url"]),
                every=every,
                start=_optional_date(entry.get("start")),
                until=_optional_date(entry.get("until")),
                member=None if entry.get("member") is None else str(entry["member"]),
            )
        )
    if not periods:
        raise ValueError(f"file_archive source {name!r}: 'periods' must list at least one period")

    time_column = str(raw["time_column"])
    layout = str(raw.get("layout") or f"time:{time_column}+daily:%Y/%m/%Y-%m-%d")
    if not layout.startswith(("time:", "time+column:")):
        raise ValueError(
            f"file_archive source {name!r}: 'layout' must be a 'time:' or 'time+column:' partitioning spec"
            f" (see damast.core.SaveAs.parse), got {layout!r}"
        )

    spec_name = raw.get("spec")
    spec_path = None if not spec_name else SPEC_DIR / str(spec_name)
    if spec_path is not None and not spec_path.is_file():
        raise ValueError(f"file_archive source {name!r}: spec {spec_name!r} not found in {SPEC_DIR}")

    lat_column, lon_column = region_columns
    return ArchiveSpec(
        archive_root=resolve_archive_root(raw["archive_root"]),
        periods=tuple(periods),
        convert=convert,
        layout=layout,
        time_column=time_column,
        time_format=None if raw.get("time_format") is None else str(raw["time_format"]),
        lat_column=str(lat_column),
        lon_column=str(lon_column),
        spec_path=spec_path,
    )


def remote_file_for_day(spec: ArchiveSpec, day: date) -> RemoteFile | None:
    """Return the remote file holding `day`, or `None` if no period covers it.

    Example:
        ```python
        remote_file_for_day(spec, date(2024, 3, 1)).url
        ```

    Args:
        spec: Parsed source configuration.
        day: Request day.

    Returns:
        The file's URL, its mirror name, and the day's member inside it (if configured).
    """
    for period in spec.periods:
        if period.covers(day):
            url = format_pattern(period.url, {"date": period.anchor(day)})
            member = None if period.member is None else format_pattern(period.member, {"date": day})
            stem = url.rstrip("/").rsplit("/", maxsplit=1)[-1].removesuffix(".zip")
            return RemoteFile(url=url, stem=stem, member=member)
    return None


def mirror_marker(spec: ArchiveSpec, remote: RemoteFile) -> Path:
    """Return the marker file recording that `remote` has been mirrored completely."""
    return spec.archive_root / ".mirrored" / f"{remote.stem}.json"


def _download(url: str, target: Path) -> None:
    """Stream `url` to `target`, raising `RemoteFileMissingError` on HTTP 404."""
    import requests

    with requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT_S) as response:
        if response.status_code == 404:
            raise RemoteFileMissingError(f"provider has no file at {url}")
        response.raise_for_status()
        with target.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                handle.write(chunk)


def _extract_csv_members(archive: Path, out_dir: Path) -> list[Path]:
    """Extract every CSV member of `archive` flat into `out_dir`.

    Members are written under their base name only, so a member path cannot escape `out_dir`.
    """
    extracted = []
    with zipfile.ZipFile(archive) as bundle:
        for info in bundle.infolist():
            name = Path(info.filename).name
            if info.is_dir() or not name.lower().endswith(".csv"):
                continue
            target = out_dir / name
            with bundle.open(info) as src, target.open("wb") as dst:
                shutil.copyfileobj(src, dst, DOWNLOAD_CHUNK_BYTES)
            extracted.append(target)
    if not extracted:
        raise ValueError(f"{archive.name} contains no CSV file")
    return extracted


def _metadata_for(spec: ArchiveSpec, lazy, url: str):
    """Return the damast metadata for one CSV: the provider spec, or inferred from the data.

    A provenance comment naming `url` is added either way. Spec columns the file lacks are
    dropped, since damast rejects metadata describing columns the frame does not have.
    """
    import damast

    if spec.spec_path is None:
        metadata = damast.core.AnnotatedDataFrame.infer_annotation(lazy)
    else:
        metadata = narrowed_metadata(damast.core.MetaData.load_yaml(spec.spec_path), lazy.collect_schema().names())
    metadata.add_annotation(
        damast.core.Annotation(name=damast.core.Annotation.Key.Comment, value=f"Downloaded from {url}")
    )
    return metadata


def _with_column_type(metadata, column: str, schema):
    """Record `column`'s actual dtype in `metadata` - the spec describes the CSV text, not the parsed time."""
    if column in metadata and column in schema:
        metadata[column].representation_type = schema[column]
    return metadata


def _convert_to_partitions(csv: Path, spec: ArchiveSpec, url: str, out_dir: Path) -> list[Path]:
    """Write `csv` as annotated Parquet partitions per `spec.layout` under `out_dir`, then remove it.

    The time column is parsed to UTC first, since the partitions are cut along it.
    """
    import damast
    import polars as pl

    lazy, _ = damast.core.AnnotatedDataFrame.load_csv([str(csv)])
    if spec.time_format is not None:
        lazy = lazy.with_columns(pl.col(spec.time_column).str.strptime(pl.Datetime("us", "UTC"), spec.time_format))
    metadata = _with_column_type(_metadata_for(spec, lazy, url), spec.time_column, lazy.collect_schema())
    adf = damast.core.AnnotatedDataFrame(lazy, metadata, validation_mode=damast.core.ValidationMode.UPDATE_DATA)
    written = adf.export_partitioned(out_dir, damast.core.SaveAs.parse(spec.layout).strategy)
    csv.unlink()
    return written


def _publish_partition(staged: Path, target: Path) -> None:
    """Move a staged partition to `target`, merging with rows already there.

    Provider files normally hold whole days, so a partition has a single source; if another remote
    file already wrote it, both are combined and exact duplicate rows dropped, which also keeps a
    repeated mirror of the same file idempotent.
    """
    import damast

    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        both = damast.core.AnnotatedDataFrame.from_files([str(target), str(staged)], metadata_required=False)
        merged = damast.core.AnnotatedDataFrame(
            both.lazyframe.unique(maintain_order=True),
            both.metadata,
            validation_mode=damast.core.ValidationMode.IGNORE,
        )
        merged_path = staged.with_name(f"merged-{staged.name}")
        merged.export(merged_path)
        merged_path.replace(target)
        staged.unlink()
        return
    staged.replace(target)


def _write_spec_sidecar(csv: Path, spec: ArchiveSpec, url: str) -> None:
    """Write the damast spec for `csv` next to it, leaving the CSV untouched."""
    import damast

    lazy, _ = damast.core.AnnotatedDataFrame.load_csv([str(csv)])
    _metadata_for(spec, lazy, url).save_yaml(damast.core.MetaData.specfile([csv]))


def mirror(spec: ArchiveSpec, remote: RemoteFile, progress: ProgressCallback, source_name: str) -> None:
    """Make sure `remote` is downloaded and unpacked into the mirror.

    Everything is assembled in a temporary directory under `archive_root` and published at the
    end, followed by the marker, so an interrupted run never looks complete.

    Args:
        spec: Parsed source configuration.
        remote: The remote file to mirror.
        progress: Progress callback.
        source_name: Source name reported to `progress`.

    Raises:
        RemoteFileMissingError: The provider does not serve the file.
    """
    marker = mirror_marker(spec, remote)
    if marker.is_file():
        return
    spec.archive_root.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=f".{remote.stem}-", dir=spec.archive_root) as staging:
        staging_dir = Path(staging)
        archive = staging_dir / f"{remote.stem}.zip"
        progress(source_name, f"downloading {remote.url}")
        _download(remote.url, archive)
        unpacked = staging_dir / remote.stem
        unpacked.mkdir()
        members = _extract_csv_members(archive, unpacked)
        archive.unlink()

        if spec.convert == "parquet":
            partitions = staging_dir / "partitions"
            written: list[Path] = []
            for csv in members:
                progress(source_name, f"converting {csv.name}")
                written.extend(_convert_to_partitions(csv, spec, remote.url, partitions))
            published = [path.relative_to(partitions) for path in written]
            for relative in published:
                _publish_partition(partitions / relative, spec.archive_root / relative)
        else:
            for csv in members:
                progress(source_name, f"annotating {csv.name}")
                _write_spec_sidecar(csv, spec, remote.url)
            target = spec.archive_root / remote.stem
            if target.exists():
                shutil.rmtree(target)
            unpacked.replace(target)
            published = [target.relative_to(spec.archive_root) / csv.name for csv in members]

    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(json.dumps({"url": remote.url, "convert": spec.convert, "files": [str(p) for p in published]}))


def _day_files(spec: ArchiveSpec, remote: RemoteFile, day: date) -> list[Path]:
    """Return the mirrored data files to read for `day`.

    Parquet mirrors are looked up through `layout`; spec-only mirrors keep the provider's files,
    so the day's `member` (or every CSV of the remote file) is read.
    """
    if spec.convert == "parquet":
        return layout_files(spec.archive_root, spec.layout, day)
    directory = spec.archive_root / remote.stem
    if remote.member is not None:
        candidate = directory / remote.member
        return [candidate] if candidate.is_file() else []
    return sorted(directory.glob("*.csv"))


def _result(source: SourceConfig, day: date, status: SourceStatus, details: dict[str, Any], **fields) -> SourceResult:
    return SourceResult(
        source=source.name,
        status=status,
        dataset_id=source.dataset_id or DEFAULT_DATASET_ID,
        variables=source.variables,
        day=day.isoformat(),
        details=details,
        **fields,
    )


def fetch_file_archive(
    request: Request,
    source: SourceConfig,
    config: Config,
    request_dir: Path,
    *,
    progress: ProgressCallback = null_progress,
    _days: tuple[date, ...] | None = None,
) -> list[SourceResult]:
    """Fetch one region-filtered Parquet file per request day from a remote file archive.

    A day whose remote file the provider does not serve, or that has no rows in the region, is
    skipped; a configured column the data does not have is raised as a configuration error.

    Args:
        request: Region and time window to fetch.
        source: Resolved source configuration.
        config: Global configuration (cache policy).
        request_dir: Directory for this collection request.
        progress: Optional progress callback.
        _days: Internal batch-planner day selection; keeps the full request's naming context.

    Returns:
        One `SourceResult` per requested day.

    Raises:
        ValueError: The source configuration does not match the archive.
    """
    import damast

    spec = parse_archive_spec(source)
    dataset_id = source.dataset_id or DEFAULT_DATASET_ID
    out_dir = request_dir / source.path
    out_dir.mkdir(parents=True, exist_ok=True)
    plan = sampling_plan(request, source)
    mirror_errors: dict[str, Exception | None] = {}
    # The declared coverage gates downloads: a remote file can be gigabytes, so a request outside
    # the provider's region or date range is skipped before anything is fetched.
    coverage = static_coverage(source)
    region_miss = None if coverage is None or region_overlaps(request.region, coverage) else coverage
    # Rows are read within the request region clipped to the coverage box, so an entry only ever
    # returns rows of the region it declares - two entries that share one mirror and differ only
    # in region (e.g. either side of the antimeridian) never return the same rows.
    read_region = request.region
    if coverage is not None and region_miss is None:
        read_region = Region.from_bbox(clip_region(request.region, coverage))

    results: list[SourceResult] = []
    for day in request.iter_days() if _days is None else _days:
        details: dict[str, Any] = {"temporal": plan.day_dict(day), "archive_root": str(spec.archive_root)}
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
            results.append(_result(source, day, SourceStatus.REUSED, details, path=output_path, format="parquet"))
            continue

        if region_miss is not None:
            message = region_reason(request.region, region_miss)
            results.append(_result(source, day, SourceStatus.SKIPPED, details, message=message))
            continue
        if coverage is not None and not day_in_range(day, coverage):
            results.append(_result(source, day, SourceStatus.SKIPPED, details, message=day_reason(day, coverage)))
            continue

        remote = remote_file_for_day(spec, day)
        if remote is None:
            results.append(_result(source, day, SourceStatus.SKIPPED, details, message="no period covers this day"))
            continue
        details["url"] = remote.url
        if remote.url not in mirror_errors:
            try:
                mirror(spec, remote, progress, source.name)
                mirror_errors[remote.url] = None
            except Exception as exc:  # noqa: BLE001 - one unavailable file must not block other days
                mirror_errors[remote.url] = exc
        if mirror_errors[remote.url] is not None:
            results.append(_result(source, day, SourceStatus.SKIPPED, details, message=str(mirror_errors[remote.url])))
            continue

        files = _day_files(spec, remote, day)
        if not files:
            message = f"no mirrored file for {day.isoformat()} under {spec.archive_root}"
            results.append(_result(source, day, SourceStatus.SKIPPED, details, message=message))
            continue
        details["matched_files"] = [str(f) for f in files]

        progress(source.name, f"reading {day.isoformat()} from {len(files)} mirrored file(s)")
        try:
            rows, metadata = read_day_subset(
                files,
                time_column=spec.time_column,
                lat_column=spec.lat_column,
                lon_column=spec.lon_column,
                day=day,
                region=read_region,
                variables=source.variables,
                time_format=spec.time_format,
                validation_mode=damast.core.ValidationMode.UPDATE_DATA,
            )
        except TabularConfigError as exc:
            raise ValueError(f"file_archive source {source.name!r}: {exc}") from exc
        if rows.height == 0:
            message = f"no rows within the request region for {day.isoformat()}"
            results.append(_result(source, day, SourceStatus.SKIPPED, details, message=message))
            continue

        subset = damast.core.AnnotatedDataFrame(
            rows,
            _with_column_type(narrowed_metadata(metadata, rows.columns), spec.time_column, rows.schema),
            validation_mode=damast.core.ValidationMode.IGNORE,
        )
        try:
            with staged_output(output_path) as staged:
                subset.export(staged)
        except Exception as exc:  # noqa: BLE001 - one daily export must not block others
            message = f"could not export {day.isoformat()}: {exc}"
            results.append(_result(source, day, SourceStatus.SKIPPED, details, message=message))
            continue
        details |= {"provider": "file-archive", "method": "archive", "rows": rows.height}
        results.append(_result(source, day, SourceStatus.DOWNLOADED, details, path=output_path, format="parquet"))
    return results


def _dataset_for_day(source: SourceConfig, day: date) -> str:
    return source.dataset_id or DEFAULT_DATASET_ID


def _day_details(
    request: Request, source: SourceConfig, day: date, plan: SamplingPlan, coverage: Coverage | None
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    spec = parse_archive_spec(source)
    remote = remote_file_for_day(spec, day)
    request_details: dict[str, Any] = {"archive_root": str(spec.archive_root), "convert": spec.convert}
    if remote is not None:
        request_details |= {"url": remote.url, "mirrored": mirror_marker(spec, remote).is_file()}
    details = {
        "provider": "file-archive",
        "method": "archive",
        "temporal": plan.day_dict(day),
        "request": request_details,
    }
    return source.dataset_id or DEFAULT_DATASET_ID, details, {}


def plan_file_archive(request: Request, source: SourceConfig, config: Config, request_dir: Path) -> list[SourceResult]:
    """Plan file-archive reads using the source's declarative coverage, without downloading."""
    coverage, method, checked = static_source_coverage(source)
    return plan_source(
        request,
        source,
        request_dir,
        coverage=coverage,
        method=method,
        checked=checked,
        dataset_for_day=_dataset_for_day,
        day_details=_day_details,
        planned_format="parquet",
    )


def diagnose(config: Config, online: bool) -> list[DoctorCheck]:
    """Return file-archive conversion-dependency diagnostics."""
    return [package_check("damast package", "damast"), package_check("polars package", "polars")]


register_adapter(
    SourceAdapter(
        kind="file_archive",
        batch=run_file_batch,
        batch_check=daily_output_errors,
        fetch=fetch_file_archive,
        plan=plan_file_archive,
        diagnose=diagnose,
        known_raw_keys=frozenset(
            {"archive_root", "periods", "convert", "layout", "time_column", "time_format", "region_columns", "spec"}
        ),
    )
)
