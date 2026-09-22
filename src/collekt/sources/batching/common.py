"""Shared provenance, cache, and atomic-output helpers for batch adapters."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from collekt.core.config import Config, SourceConfig
from collekt.sources.base import SourceResult, SourceStatus, should_reuse_cache


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
