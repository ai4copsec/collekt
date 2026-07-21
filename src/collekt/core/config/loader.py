"""Layered YAML configuration loading."""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

# Configuration ships as package data inside ``collekt`` (``src/collekt/conf`` in
# the source tree, ``<site-packages>/collekt/conf`` once installed), so it is
# always co-located with the code regardless of how the package is installed.
# This module lives at ``collekt/core/config/loader.py``; ``parents[2]`` is the
# ``collekt`` package directory.
_PACKAGE_CONF_DIR = Path(__file__).resolve().parents[2] / "conf"


def default_conf_dir() -> Path:
    """Return the directory holding collekt configuration files.

    Returns:
        Absolute `Path` to the bundled ``conf`` directory.

    Raises:
        FileNotFoundError: If the bundled ``conf`` directory is missing.
    """
    if _PACKAGE_CONF_DIR.is_dir():
        return _PACKAGE_CONF_DIR
    raise FileNotFoundError(f"Could not locate the bundled conf/ directory at {_PACKAGE_CONF_DIR}")


def _read_yaml(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _expand_env(obj: Any) -> Any:
    if isinstance(obj, Mapping):
        return {key: _expand_env(value) for key, value in obj.items()}
    if isinstance(obj, list):
        return [_expand_env(item) for item in obj]
    if isinstance(obj, str):
        return os.path.expandvars(obj)
    return obj


def _deep_merge(base: Mapping[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        existing = result.get(key)
        if isinstance(existing, Mapping) and isinstance(value, Mapping):
            result[key] = _deep_merge(existing, value)
        else:
            result[key] = value
    return result


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return [str(item) for item in value]


def _overlay(base: Mapping[str, Any], overlay: Mapping[str, Any]) -> dict[str, Any]:
    """Deep-merge ``overlay`` onto ``base``, concatenating ``source_catalogs``.

    A downstream configuration directory extends the bundled one rather than
    replacing it: its ``default.yaml`` declares only additions and overrides, and
    its ``source_catalogs`` are appended to the bundled list (deduplicated,
    bundled first) so the shipped datasets stay available.
    """
    merged = _deep_merge(base, overlay)
    catalogs = [*_as_list(base.get("source_catalogs")), *_as_list(overlay.get("source_catalogs"))]
    if catalogs:
        merged["source_catalogs"] = list(dict.fromkeys(catalogs))
    return merged


def _load_source_catalogs(config: Mapping[str, Any], conf_dir: Path) -> dict[str, Any]:
    """Expand ``source_catalogs`` into the final ``sources`` mapping."""
    merged = dict(config)
    explicit_sources = dict(merged.get("sources", {}) or {})
    catalog_sources: dict[str, Any] = {}
    for name in _as_list(merged.get("source_catalogs")):
        path = conf_dir / "source" / f"{name}.yaml"
        if not path.exists():
            # Fall back to the bundled catalogs, so a downstream conf_dir can
            # select collekt's shipped sources by name (and add its own).
            bundled = _PACKAGE_CONF_DIR / "source" / f"{name}.yaml"
            if bundled.exists():
                path = bundled
        if not path.exists():
            raise FileNotFoundError(f"Unknown source catalog {name!r}: {path} not found")
        raw = _read_yaml(path)
        sources = raw.get("sources", raw)
        if not isinstance(sources, Mapping):
            raise ValueError(f"Source catalog {name!r} must contain a mapping")
        catalog_sources = _deep_merge(catalog_sources, sources)
    merged["sources"] = _deep_merge(catalog_sources, explicit_sources)
    return merged


def load_config(
    *,
    overrides: Mapping[str, Any] | None = None,
    conf_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Load and merge the YAML configuration.

    Example:
        ```python
        from collekt.core.config import load_config

        cfg = load_config(overrides={"cache": {"overwrite": True}})
        ```

    Args:
        overrides: Optional mapping merged last.
        conf_dir: Optional configuration directory that **extends** the bundled
            one. Its ``default.yaml`` (if present) overlays the bundled defaults
            and its ``source_catalogs`` are appended, so a consuming brick adds
            or overrides datasets without re-declaring the shipped ones.

    Returns:
        The merged, environment-expanded configuration mapping.

    Raises:
        FileNotFoundError: If the bundled base file or a requested source catalog
            is missing.
    """
    merged = _read_yaml(default_conf_dir() / "default.yaml")
    source_dir = default_conf_dir()
    if conf_dir is not None:
        source_dir = Path(conf_dir)
        overlay_path = source_dir / "default.yaml"
        if overlay_path.exists():
            merged = _overlay(merged, _read_yaml(overlay_path))
    if overrides:
        merged = _deep_merge(merged, overrides)
    merged = _load_source_catalogs(merged, source_dir)
    return _expand_env(merged)
