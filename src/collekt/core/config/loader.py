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
    preset: str | None = None,
    overrides: Mapping[str, Any] | None = None,
    conf_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Load and merge the YAML configuration.

    Example:
        ```python
        from collekt.core.config import load_config

        cfg = load_config(preset="drift", overrides={"cache": {"overwrite": True}})
        ```

    Args:
        preset: Optional preset name from ``<conf_dir>/preset/<preset>.yaml``.
        overrides: Optional mapping merged last.
        conf_dir: Optional configuration directory. Defaults to the bundled
            ``conf`` directory; a consuming brick can point this at its own
            catalogs and presets.

    Returns:
        The merged, environment-expanded configuration mapping.

    Raises:
        FileNotFoundError: If the base file or requested preset is missing.
    """
    base = Path(conf_dir) if conf_dir is not None else default_conf_dir()
    merged = _read_yaml(base / "default.yaml")
    if preset is not None:
        preset_path = base / "preset" / f"{preset}.yaml"
        if not preset_path.exists():
            raise FileNotFoundError(f"Unknown preset {preset!r}: {preset_path} not found")
        merged = _deep_merge(merged, _read_yaml(preset_path))
    if overrides:
        merged = _deep_merge(merged, overrides)
    merged = _load_source_catalogs(merged, base)
    return _expand_env(merged)
