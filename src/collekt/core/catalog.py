"""A hierarchical, read-only view of the bundled dataset catalog."""

from __future__ import annotations

from pathlib import PurePath
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


def available_datasets(*, conf_dir: str | Path | None = None) -> dict[str, Any]:
    """Return all catalog datasets as a nested dict keyed by their ``path``.

    Each dataset becomes a leaf under its `path` components (e.g.
    ``cmems -> med -> currents -> cmems_med_currents_nrt``), carrying the provider,
    dataset id, advertised variables, coverage, and reference links. Use it to
    discover which keys and variables a request can select. Every leaf has a
    ``coverage`` (whole-globe / all-time when the source does not restrict it) and a
    ``dataset_id`` (the source key when the provider has no distinct id).

    Example:

    ```python
    import collekt

    catalog = collekt.available_datasets()
    list(catalog["cmems"]["med"]["currents"])
    # ['cmems_med_currents_nrt', 'cmems_med_currents_nrt_15min', ...]
    ```

    Args:
        conf_dir: Optional downstream config directory overlaid on the bundled catalogs.

    Returns:
        A nested dictionary; leaves are per-dataset metadata dictionaries.
    """
    config = get_config(conf_dir=conf_dir)
    tree: dict[str, Any] = {}
    for name, source in config.sources.items():
        parts = list(PurePath(str(source.path)).parts) if source.path else [source.kind]
        node = tree
        for part in parts:
            node = node.setdefault(part, {})
        node[name] = {
            "provider": source.kind,
            "dataset_id": source.dataset_id or source.name,
            "variables": list(source.available_variables),
            "has_depth": bool(source.has_depth),
            "temporal_sampling": source.temporal_sampling,
            "coverage": _coverage_dict(static_coverage(source)),
            "url": source.url,
            "doi": source.doi,
        }
    return tree
