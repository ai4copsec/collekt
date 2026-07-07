"""A hierarchical, read-only view of the bundled dataset catalog."""

from __future__ import annotations

from pathlib import PurePath
from typing import TYPE_CHECKING, Any

from collekt.core.availability import static_coverage
from collekt.core.config import get_config

if TYPE_CHECKING:
    from pathlib import Path


def available_datasets(*, conf_dir: str | Path | None = None) -> dict[str, Any]:
    """Return all catalog datasets as a nested dict keyed by their ``path``.

    Each dataset becomes a leaf under its `path` components (e.g.
    ``cmems -> med -> currents -> cmems_med_currents_nrt``), carrying the provider,
    dataset id, advertised variables, coverage, and reference links. Use it to
    discover which keys and variables a request can select.

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
        coverage = static_coverage(source)
        node[name] = {
            "provider": source.kind,
            "dataset_id": source.dataset_id,
            "variables": list(source.available_variables),
            "has_depth": bool(source.has_depth),
            "temporal_sampling": source.temporal_sampling,
            "coverage": None
            if coverage is None
            else {
                "west": coverage.west,
                "east": coverage.east,
                "south": coverage.south,
                "north": coverage.north,
                "start": coverage.start,
                "end": coverage.end,
                "kind": coverage.kind,
            },
            "url": source.url,
            "doi": source.doi,
        }
    return tree
