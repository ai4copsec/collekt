"""Windowed event queries assembled into the original collection-level file.

The windowing, checkpointing, resume, and deduplication here are shared by all
event sources. Everything provider-specific - how one window is queried, how
the merged events are written, and any adjustment to the window payloads - is
supplied by the adapter, which binds its own steps with `functools.partial`:

```python
batch = partial(run_event_batch, query=_query_batch, write=_write_batch)
```
"""

from __future__ import annotations

import shutil
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import Any

from collekt.core.batching import request_windows
from collekt.core.config import Config, SourceConfig
from collekt.core.request import Region, Request
from collekt.sources.base import BatchOptions, ProgressCallback, SourceResult, SourceStatus, get_adapter
from collekt.sources.batching.common import batch_details, cached, checkpoint_query, deduplicate, failed, staged_output

EventQuery = Callable[[SourceConfig, Region, dict[str, Any], ProgressCallback], dict[str, Any]]
"""Query one window's payload; return a JSON-serialisable dict with a `rows` list."""

EventWrite = Callable[[list[dict[str, Any]], list[dict[str, Any]], SourceConfig, Path], int]
"""Write the deduplicated rows (given every window's response) and return the record count."""

PayloadAdjust = Callable[[list[dict[str, Any]], list[Request]], None]
"""Adjust the per-window payloads in place before they are queried."""


def run_event_batch(
    request: Request,
    source: SourceConfig,
    config: Config,
    request_dir: Path,
    options: BatchOptions,
    *,
    query: EventQuery,
    write: EventWrite,
    adjust: PayloadAdjust | None = None,
) -> list[SourceResult]:
    """Query independent windows, resume failures, and deduplicate overlapping events."""
    adapter = get_adapter(source.kind)
    item = cached(adapter.plan(request, source, config, request_dir)[0], config)
    if item.status == SourceStatus.REUSED:
        return [item]
    windows = request_windows(request, options.days)
    payloads = [adapter.plan(window, source, config, request_dir)[0].details["request"] for window in windows]
    if adjust is not None:
        adjust(payloads, windows)
    batches = [
        batch_details(source, window.start_datetime, window.end_datetime, payload)
        for window, payload in zip(windows, payloads, strict=True)
    ]
    item = replace(item, details=item.details | {"batches": batches})
    if options.dry_run:
        return [item]
    checkpoint_dir = item.path.parent / ".batch-queries" / item.path.name
    responses = []
    errors = []
    for batch in batches:
        options.progress(source.name, f"querying batch {batch['start']} to {batch['end']}")
        try:
            responses.append(
                checkpoint_query(
                    checkpoint_dir,
                    batch,
                    config,
                    lambda batch=batch: query(source, request.region, batch["request"], options.progress),
                )
            )
        except Exception as exc:  # noqa: BLE001 - save other completed windows for retry
            errors.append(f"batch {batch['id']}: {exc}")
    if errors:
        return [failed(item, "; ".join(errors))]
    try:
        rows = deduplicate(
            [row for response in responses for row in response["rows"]],
            lambda row: row.get("id", (row.get("properties") or {}).get("id")),
        )
        if not rows:
            return [failed(item, "the API returned 0 events for this query")]
        with staged_output(item.path) as staged:
            records = write(rows, responses, source, staged)
    except Exception as exc:  # noqa: BLE001 - retain checkpoints if assembly fails
        return [failed(item, exc)]
    # Only discard the completed queries once the collection file is published,
    # and never let a cleanup error turn a published file into a failure.
    shutil.rmtree(checkpoint_dir, ignore_errors=True)
    return [replace(item, status=SourceStatus.DOWNLOADED, details=item.details | {"records": records})]
