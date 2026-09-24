"""Logical batches for providers that serve one native file per URL."""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from pathlib import Path

from collekt.core.config import Config, SourceConfig
from collekt.core.request import Request
from collekt.sources.base import BatchOptions, SourceResult, SourceStatus, get_adapter
from collekt.sources.batching.common import batch_details, daily_groups


def run_file_batch(
    request: Request,
    source: SourceConfig,
    config: Config,
    request_dir: Path,
    options: BatchOptions,
) -> list[SourceResult]:
    """Expose logical batches for archives/providers with one URL per native file.

    The grouping is provenance only: each native file still has its own URL and
    transfer. An adapter routed here must accept the private `_days` keyword on
    its `fetch`, which restricts the daily loop to the days this plan still
    needs while keeping the full request as the naming and cadence anchor.
    """
    planned, groups = daily_groups(request, source, config, request_dir, options.days)
    provenance = {}
    for indices in groups:
        members = [planned[index] for index in indices]
        batch = batch_details(
            source,
            members[0].day,
            members[-1].day,
            [item.details.get("request", {}) for item in members],
            transport="individual_files",
        )
        for index in indices:
            planned[index] = replace(planned[index], details=planned[index].details | {"batch": batch})
            provenance[planned[index].day] = batch
    if options.dry_run or not groups:
        return planned
    # Preserve the full request: restarting daily loops on clipped requests
    # would change cadence anchors and filename patterns using start/end.
    results = get_adapter(source.kind).fetch(
        request,
        source,
        config,
        request_dir,
        progress=options.progress,
        _days=tuple(date.fromisoformat(day) for day in provenance),
    )
    if any(item.day is None for item in results):
        return [item for item in planned if item.status != SourceStatus.PLANNED] + results
    by_day = {
        item.day: replace(item, details=(item.details or {}) | {"batch": provenance[item.day]})
        if item.status != SourceStatus.REUSED
        else item
        for item in results
    }
    return [by_day.get(item.day, item) for item in planned]
