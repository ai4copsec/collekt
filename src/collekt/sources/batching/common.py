"""Shared provenance, cache, and atomic-output helpers for batch adapters."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from collekt.core.config import Config, SourceConfig
from collekt.core.naming import format_pattern
from collekt.sources.base import SourceResult, SourceStatus, should_reuse_cache

_PROBE_VALUES = {
    "source": "probe",
    "dataset_id": "probe",
    "start": datetime(2001, 1, 1, tzinfo=UTC),
    "end": datetime(2001, 1, 2, tzinfo=UTC),
    "bbox_hash": "00000000",
    "west": 0.0,
    "east": 1.0,
    "south": 0.0,
    "north": 1.0,
}


def daily_output_errors(source: SourceConfig) -> list[str]:
    """Reject filename patterns that give two days of a batch the same output file.

    Splitting a multi-day retrieval writes one file per day, so a pattern that
    does not vary with `date` would have each day overwrite the previous one.
    The two probe dates only exercise the pattern; no day of the request is
    formatted here.
    """
    try:
        names = {
            format_pattern(source.filename_pattern, _PROBE_VALUES | {"date": date(2001, 1, day)}) for day in (1, 2)
        }
    except Exception:  # noqa: BLE001 - an unformattable pattern is reported by the adapter itself
        return []
    if len(names) == 1:
        return [f"filename_pattern {source.filename_pattern!r} does not vary per day, which batch splitting requires"]
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
