"""Shared provenance, cache, and atomic-output helpers for batch adapters."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from collekt.core.config import Config, SourceConfig
from collekt.core.naming import format_pattern
from collekt.core.request import Request
from collekt.sources.base import SourceResult, SourceStatus, get_adapter, should_reuse_cache

# Stand-ins for every placeholder except `date`, which is the only one that
# changes between the daily files of a batch.
_PROBE_VALUES = {
    "source": "probe",
    "dataset_id": "probe",
    "start": datetime(2000, 1, 1, tzinfo=UTC),
    "end": datetime(2002, 1, 1, tzinfo=UTC),
    "bbox_hash": "00000000",
    "west": 0.0,
    "east": 1.0,
    "south": 0.0,
    "north": 1.0,
}
# Two full years, one of them a leap year: long enough for a pattern that
# repeats with the month (`%d`) or the year (`%j`, `%m-%d`) to collide.
_PROBE_DAYS = tuple(date(2000, 1, 1) + timedelta(days=offset) for offset in range(731))


def daily_output_errors(source: SourceConfig) -> list[str]:
    """Reject filename patterns that give two different days the same output file.

    Splitting a multi-day retrieval writes one file per day, so a pattern that
    repeats - never varying with `date`, or using only part of it such as the
    day of the month - would have one day overwrite another. The probe days
    only exercise the pattern; no day of the request is formatted here.
    """
    try:
        names = {format_pattern(source.filename_pattern, _PROBE_VALUES | {"date": day}) for day in _PROBE_DAYS}
    except Exception:  # noqa: BLE001 - an unformattable pattern is reported by the adapter itself
        return []
    if len(names) < len(_PROBE_DAYS):
        return [
            f"filename_pattern {source.filename_pattern!r} does not give every day its own file, "
            "which batch splitting requires"
        ]
    return []


def batch_details(
    source: SourceConfig, start: Any, end: Any, payload: Any, *, transport: str = "range"
) -> dict[str, Any]:
    """Describe a logical provider operation without equating it with an HTTP call."""
    identity = json.dumps(
        [source.name, source.kind, source.variables, source.raw, str(start), str(end), payload],
        sort_keys=True,
        default=str,
    )
    return {
        "id": hashlib.sha256(identity.encode()).hexdigest()[:16],
        "start": str(start),
        "end": str(end),
        "transport": transport,
        "request": payload,
    }


def cached(result: SourceResult, config: Config) -> SourceResult:
    """Mark an existing output reusable without attributing a new retrieval to it."""
    if result.path is not None and should_reuse_cache(result.path.exists(), config):
        return replace(result, status=SourceStatus.REUSED)
    return result


def failed(result: SourceResult, exc: Exception | str) -> SourceResult:
    """Return a warning without exposing a partial output as a completed file."""
    return replace(result, status=SourceStatus.SKIPPED, path=None, message=str(exc))


@contextmanager
def staged_output(path: Path) -> Iterator[Path]:
    """Publish the data file last, after its writer and metadata sidecars succeed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=".collekt-", dir=path.parent) as directory:
        staged = Path(directory) / path.name
        yield staged
        if not staged.is_file():
            raise RuntimeError("download completed but file is missing")
        for sidecar in staged.parent.iterdir():
            if sidecar != staged:
                sidecar.replace(path.parent / sidecar.name)
        staged.replace(path)


def checkpoint_query(directory: Path, batch: dict[str, Any], config: Config, query: Callable[[], Any]) -> Any:
    """Resume completed catalogue windows after a later window fails."""
    path = directory / f"{batch['id']}.json"
    if should_reuse_cache(path.exists(), config):
        try:
            return json.loads(path.read_text())
        except (ValueError, OSError):
            pass
    value = query()
    with staged_output(path) as staged:
        staged.write_text(json.dumps(value, default=str))
    return value


def deduplicate(rows: list[dict[str, Any]], key: Callable[[dict[str, Any]], Any]) -> list[dict[str, Any]]:
    """Keep provider order and retain records without an identifier."""
    seen = set()
    unique = []
    for row in rows:
        identity = key(row)
        if identity is None or identity not in seen:
            unique.append(row)
            if identity is not None:
                seen.add(identity)
    return unique


def daily_groups(
    request: Request,
    source: SourceConfig,
    config: Config,
    request_dir: Path,
    batch_days: int,
    *,
    compatible: Callable[[SourceResult], Any] = lambda item: None,
) -> tuple[list[SourceResult], list[list[int]]]:
    """Group consecutive missing days within fixed windows and compatible payloads."""
    results = [cached(item, config) for item in get_adapter(source.kind).plan(request, source, config, request_dir)]
    paths = [item.path for item in results if item.path is not None]
    if len(paths) != len(set(paths)):
        raise ValueError("batch downloads require a distinct output filename for each day")
    groups = []
    previous = None
    for index, item in enumerate(results):
        if item.status != SourceStatus.PLANNED or item.day is None:
            previous = None
            continue
        day = date.fromisoformat(item.day)
        bucket = (day - request.start_datetime.date()).days // batch_days
        key = (bucket, item.dataset_id, compatible(item))
        if previous is None or previous[0] != key or day != previous[1] + timedelta(days=1):
            groups.append([])
        groups[-1].append(index)
        previous = key, day
    return results, groups
