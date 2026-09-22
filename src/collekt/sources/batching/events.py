"""Windowed event queries assembled into the original collection-level file."""

from __future__ import annotations

import shutil
import warnings
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any

from collekt.core.batching import request_windows
from collekt.core.config import Config, SourceConfig
from collekt.core.request import Region, Request
from collekt.sources.base import ProgressCallback, SourceResult, SourceStatus, get_adapter
from collekt.sources.batching.common import batch_details, cached, checkpoint_query, deduplicate, failed, staged_output


def run_event_batch(
    request: Request,
    source: SourceConfig,
    config: Config,
    request_dir: Path,
    *,
    batch_days: int,
    dry_run: bool,
    progress: ProgressCallback,
) -> list[SourceResult]:
    """Query independent windows, resume failures, and deduplicate overlapping events."""
    adapter = get_adapter(source.kind)
    item = cached(adapter.plan(request, source, config, request_dir)[0], config)
    if item.status == SourceStatus.REUSED:
        return [item]
    windows = request_windows(request, batch_days)
    batches = [
        batch_details(
            source,
            window.start_datetime,
            window.end_datetime,
            adapter.plan(window, source, config, request_dir)[0].details["request"],
        )
        for window in windows
    ]
    if source.kind == "skytruth":
        # The API payload has second precision. Overlap at midnight so a
        # fractional timestamp in the preceding second is never lost.
        for batch, following in zip(batches, windows[1:], strict=False):
            start = batch["request"]["datetime"].split("/", 1)[0]
            batch["request"]["datetime"] = f"{start}/{following.start_datetime:%Y-%m-%dT%H:%M:%SZ}"
        batches = [batch_details(source, batch["start"], batch["end"], batch["request"]) for batch in batches]
    item = replace(item, details=item.details | {"batches": batches})
    if dry_run:
        return [item]
    checkpoint_dir = item.path.parent / ".batch-queries" / item.path.name
    responses = []
    errors = []
    for batch in batches:
        progress(source.name, f"querying batch {batch['start']} to {batch['end']}")
        try:
            responses.append(
                checkpoint_query(
                    checkpoint_dir,
                    batch,
                    config,
                    lambda batch=batch: _query(source, request.region, batch["request"], progress),
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
        if source.kind == "gfw":
            from collekt.sources import gfw

            for row in rows:
                for key in ("start", "end"):
                    if isinstance(row.get(key), str):
                        row[key] = datetime.fromisoformat(row[key].replace("Z", "+00:00"))
            rows.sort(key=lambda row: row["start"].timestamp() if row.get("start") else float("-inf"))
            cap = gfw._max_events(source)
            if cap is not None and (len(rows) > cap or any(response["capped"] for response in responses)):
                warnings.warn(
                    f"GFW results truncated at max_events={cap} across all batches",
                    gfw.GFWTruncatedResultWarning,
                    stacklevel=2,
                )
                rows = rows[:cap]
            with staged_output(item.path) as staged:
                gfw._write_parquet(rows, staged, source)
        else:
            from collekt.sources import skytruth

            rows.sort(key=lambda row: str((row.get("properties") or {}).get("slick_timestamp", "")), reverse=True)
            with staged_output(item.path) as staged:
                skytruth._write_parquet(rows, staged, "; ".join(response["url"] for response in responses))
        shutil.rmtree(checkpoint_dir)
        return [replace(item, status=SourceStatus.DOWNLOADED, details=item.details | {"records": len(rows)})]
    except Exception as exc:  # noqa: BLE001 - retain checkpoints if assembly fails
        return [failed(item, exc)]


def _query(source: SourceConfig, region: Region, payload: dict[str, Any], progress: ProgressCallback) -> dict[str, Any]:
    if source.kind == "skytruth":
        from collekt.sources import skytruth

        url = (source.raw.get("api_url") or skytruth.CERULEAN_API_SLICK) + "?sortby=%2Dslick_timestamp"
        rows, url = skytruth._fetch_pages(url, payload)
        return {"rows": rows, "url": url}
    from collekt.sources import gfw

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", gfw.GFWTruncatedResultWarning)
        rows = gfw._fetch_events(
            source,
            region,
            payload["start_date"],
            payload["end_date"],
            lambda page, total: progress(source.name, f"GFW page {page}: {total} events"),
        )
    capped = False
    for warning in caught:
        if issubclass(warning.category, gfw.GFWTruncatedResultWarning):
            capped = True
        else:
            warnings.warn(warning.message, warning.category, stacklevel=2)
    return {"rows": rows, "capped": capped}
