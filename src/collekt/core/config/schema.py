"""Typed configuration schema."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from collekt.core.config.loader import load_config
from collekt.core.request import format_sampling


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class OutputConfig:
    """Output directory and naming configuration."""

    root: Path = Path("data/collections")
    request_pattern: str = "{start:%Y%m%d}_{end:%Y%m%d}_{bbox_hash}"
    manifest_name: str = "manifest.json"


@dataclass(frozen=True)
class CacheConfig:
    """Cache and failure policy."""

    reuse_existing: bool = True
    overwrite: bool = False
    strict: bool = False


@dataclass(frozen=True)
class CredentialsConfig:
    """Credential-file hints.

    These paths point to provider-managed secret files. They must not contain the
    secret values themselves.
    """

    cdsapi_rc: Path | None = Path("$HOME/.cdsapirc")
    copernicusmarine_credentials_file: Path | None = None


@dataclass(frozen=True)
class SourceConfig:
    """Configuration for one data-source adapter.

    The bundled catalog describes each dataset's *capabilities*; which variables
    and depths to actually fetch is a downstream choice:

    - `available_variables` is the harvested allow-list a source can serve.
      The selected `variables` are validated against it; the catalog ships no
      default selection, so a bundled source must be given a selection downstream.
    - `has_depth` marks a source with a depth dimension. Such a source requires an
      explicit `depth: [min, max]` downstream; the provider validates the range.
    - `url` and `doi` are provenance metadata (product page and citation).
    """

    name: str
    kind: str
    enabled: bool = True
    path: Path = Path(".")
    filename_pattern: str = "{source}_{date:%Y%m%d}_{bbox_hash}.nc"
    available_variables: tuple[str, ...] = ()
    variables: tuple[str, ...] = ()
    has_depth: bool = False
    url: str | None = None
    doi: str | None = None
    mode: str = "auto"
    dataset_id: str | None = None
    temporal_sampling: str = "24h"
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Config:
    """Fully parsed collekt configuration."""

    output: OutputConfig = field(default_factory=OutputConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    credentials: CredentialsConfig = field(default_factory=CredentialsConfig)
    source_catalogs: tuple[str, ...] = ()
    sources: dict[str, SourceConfig] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict)


def _optional_path(value: Any) -> Path | None:
    if value in (None, ""):
        return None
    return Path(str(value)).expanduser()


def _parse_output(raw: Mapping[str, Any] | None) -> OutputConfig:
    d = dict(raw or {})
    return OutputConfig(
        root=Path(str(d.get("root", "data/collections"))).expanduser(),
        request_pattern=str(d.get("request_pattern", "{start:%Y%m%d}_{end:%Y%m%d}_{bbox_hash}")),
        manifest_name=str(d.get("manifest_name", "manifest.json")),
    )


def _parse_cache(raw: Mapping[str, Any] | None) -> CacheConfig:
    d = dict(raw or {})
    return CacheConfig(
        reuse_existing=_as_bool(d.get("reuse_existing"), True),
        overwrite=_as_bool(d.get("overwrite"), False),
        strict=_as_bool(d.get("strict"), False),
    )


def _parse_credentials(raw: Mapping[str, Any] | None) -> CredentialsConfig:
    d = dict(raw or {})
    return CredentialsConfig(
        cdsapi_rc=_optional_path(d.get("cdsapi_rc", "$HOME/.cdsapirc")),
        copernicusmarine_credentials_file=_optional_path(d.get("copernicusmarine_credentials_file")),
    )


def _parse_source(name: str, raw: Mapping[str, Any]) -> SourceConfig:
    d = dict(raw)
    available_variables = _as_str_tuple(d.get("available_variables"))
    variables = _as_str_tuple(d.get("variables"))
    _validate_selected_variables(name, variables, available_variables)
    return SourceConfig(
        name=name,
        kind=str(d.get("kind", name)),
        enabled=_as_bool(d.get("enabled"), True),
        path=Path(str(d.get("path", "."))),
        filename_pattern=str(d.get("filename_pattern", f"{name}" + "_{date:%Y%m%d}_{bbox_hash}.nc")),
        available_variables=available_variables,
        variables=variables,
        has_depth=_as_bool(d.get("has_depth"), False),
        url=None if d.get("url") is None else str(d["url"]),
        doi=None if d.get("doi") is None else str(d["doi"]),
        mode=str(d.get("mode", "auto")),
        dataset_id=None if d.get("dataset_id") is None else str(d["dataset_id"]),
        temporal_sampling=format_sampling(d.get("temporal_sampling", "24h")),
        raw=d,
    )


def _as_tuple(value: Any) -> tuple[Any, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Mapping):
        raise ValueError("expected a scalar or list, got a mapping")
    return tuple(value)


def _as_str_tuple(value: Any) -> tuple[str, ...]:
    return tuple(str(x) for x in _as_tuple(value))


def _validate_selected_variables(
    source_name: str,
    variables: tuple[str, ...],
    available_variables: tuple[str, ...],
) -> None:
    if not variables or not available_variables:
        return
    unknown = sorted(set(variables) - set(available_variables))
    if unknown:
        known = ", ".join(available_variables)
        missing = ", ".join(unknown)
        raise ValueError(f"source {source_name!r} requested unknown variables {missing}; known: {known}")


def parse_config(raw: Mapping[str, Any] | None) -> Config:
    """Parse a resolved configuration mapping."""
    d = dict(raw or {})
    sources = {
        name: _parse_source(name, source)
        for name, source in (dict(d.get("sources", {}) or {})).items()
        if isinstance(source, Mapping)
    }
    return Config(
        output=_parse_output(d.get("output")),
        cache=_parse_cache(d.get("cache")),
        credentials=_parse_credentials(d.get("credentials")),
        source_catalogs=tuple(str(x) for x in (d.get("source_catalogs") or ())),
        sources=sources,
        raw=d,
    )


def get_config(
    *,
    overrides: Mapping[str, Any] | None = None,
    conf_dir: str | Path | None = None,
) -> Config:
    """Load YAML configuration and return typed values.

    Args:
        overrides: Optional mapping merged last.
        conf_dir: Optional configuration directory.

    Returns:
        Parsed collekt configuration.
    """
    return parse_config(load_config(overrides=overrides, conf_dir=conf_dir))
