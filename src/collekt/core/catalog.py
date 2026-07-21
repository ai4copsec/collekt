"""A read-only view of the bundled dataset catalog."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from collekt.core.availability import (
    GLOBAL_EAST,
    GLOBAL_NORTH,
    GLOBAL_SOUTH,
    GLOBAL_WEST,
    static_coverage,
)
from collekt.core.config import get_config

if TYPE_CHECKING:
    from pathlib import Path


def _coverage_dict(coverage: Any) -> dict[str, Any]:
    """Coverage as a plain dict, defaulting to whole-globe / all-time when unspecified."""
    if coverage is None:
        return {
            "west": GLOBAL_WEST,
            "east": GLOBAL_EAST,
            "south": GLOBAL_SOUTH,
            "north": GLOBAL_NORTH,
            "start": None,
            "end": None,
            "kind": None,
        }
    return {
        "west": coverage.west,
        "east": coverage.east,
        "south": coverage.south,
        "north": coverage.north,
        "start": coverage.start,
        "end": coverage.end,
        "kind": coverage.kind,
    }


def available_datasets(*, conf_dir: str | Path | None = None) -> dict[str, dict[str, Any]]:
    """Return every catalog dataset as a flat mapping keyed by dataset key.

    Each value is a uniform metadata dict with the same keys for every provider:
    ``provider``, ``path`` (e.g. ``"cmems/med/currents"``), ``dataset_id``,
    ``variables``, ``has_depth``, ``temporal_sampling``, ``coverage`` (whole-globe /
    all-time when the source does not restrict it), ``url``, and ``doi``. Group or
    filter by the ``provider`` / ``path`` fields; see `describe_datasets` for a
    human-readable listing.

    Example:

    ```python
    import collekt

    catalog = collekt.available_datasets()
    catalog["cmems_med_currents_nrt"]["variables"]
    # ['uo', 'vo']
    [key for key, meta in catalog.items() if meta["provider"] == "cmems"]
    ```

    Args:
        conf_dir: Optional downstream config directory overlaid on the bundled catalogs.

    Returns:
        A flat ``{dataset_key: metadata}`` mapping with a uniform metadata schema.
    """
    config = get_config(conf_dir=conf_dir)
    catalog: dict[str, dict[str, Any]] = {}
    for name, source in config.sources.items():
        catalog[name] = {
            "provider": source.kind,
            "path": str(source.path) if source.path else source.kind,
            "dataset_id": source.dataset_id or source.name,
            "variables": list(source.available_variables),
            "has_depth": bool(source.has_depth),
            "temporal_sampling": source.temporal_sampling,
            "coverage": _coverage_dict(static_coverage(source)),
            "url": source.url,
            "doi": source.doi,
        }
    return catalog


def describe_datasets(*, conf_dir: str | Path | None = None) -> str:
    """Return a human-readable, provider-grouped listing of the catalog.

    Example:

    ```python
    import collekt

    print(collekt.describe_datasets())
    ```

    Args:
        conf_dir: Optional downstream config directory overlaid on the bundled catalogs.

    Returns:
        A multi-line string; print it to browse the datasets, their extent, date range,
        and variables grouped by provider.
    """
    by_provider: dict[str, list[tuple[str, dict[str, Any]]]] = {}
    for key, meta in available_datasets(conf_dir=conf_dir).items():
        by_provider.setdefault(meta["provider"], []).append((key, meta))

    lines: list[str] = []
    for provider in sorted(by_provider):
        entries = sorted(by_provider[provider])
        lines.append(f"{provider}  ({len(entries)})")
        for key, meta in entries:
            coverage = meta["coverage"]
            bbox = f"[{coverage['west']:g}, {coverage['east']:g}] x [{coverage['south']:g}, {coverage['north']:g}]"
            span = f"{coverage['start'] or '…'}..{coverage['end'] or 'now'}"
            variables = ", ".join(meta["variables"]) or "—"
            lines.append(f"  {key:34} {bbox:28} {span}")
            lines.append(f"      {meta['path']}  |  {variables}")
    return "\n".join(lines)
