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
    request_pattern: str = "{start:%Y%m%d}_{end:%Y%m%d}_{sampling}_{bbox_hash}"
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
class TemporalConfig:
    """Temporal capability metadata for one source."""

    native_sampling: str = "24h"
    supported_sampling: tuple[str, ...] = ("24h",)
    sampling_mode: str = "instantaneous"


@dataclass(frozen=True)
class SourceConfig:
    """Configuration for one data-source adapter."""

    name: str
    kind: str
    enabled: bool = True
    variable_groups: tuple[str, ...] = ()
    path: Path = Path(".")
    filename_pattern: str = "{source}_{date:%Y%m%d}_{bbox_hash}.nc"
    default_variables: tuple[str, ...] = ()
    optional_variables: dict[str, tuple[str, ...]] = field(default_factory=dict)
    use_variables: tuple[str, ...] = ()
    variables: tuple[str, ...] = ()
    mode: str = "auto"
    dataset_id: str | None = None
    dataset_nrt: str | None = None
    dataset_my: str | None = None
    nrt_cutoff: str | None = None
    temporal: TemporalConfig = field(default_factory=TemporalConfig)
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


SourceVariableOverrides = Mapping[str, tuple[str, ...] | list[str] | str]


def _optional_path(value: Any) -> Path | None:
    if value in (None, ""):
        return None
    return Path(str(value)).expanduser()


def _parse_output(raw: Mapping[str, Any] | None) -> OutputConfig:
    d = dict(raw or {})
    return OutputConfig(
        root=Path(str(d.get("root", "data/collections"))).expanduser(),
        request_pattern=str(d.get("request_pattern", "{start:%Y%m%d}_{end:%Y%m%d}_{sampling}_{bbox_hash}")),
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
    default_variables, optional_variables = _parse_variable_catalog(d.get("variables"))
    use_variables = tuple(str(x) for x in _as_tuple(d.get("use_variables", ("default",))))
    variables = _resolve_variables(
        source_name=name,
        default_variables=default_variables,
        optional_variables=optional_variables,
        use_variables=use_variables,
    )
    groups = d.get("variable_groups") or ()
    return SourceConfig(
        name=name,
        kind=str(d.get("kind", name)),
        enabled=_as_bool(d.get("enabled"), True),
        variable_groups=tuple(str(x) for x in groups),
        path=Path(str(d.get("path", "."))),
        filename_pattern=str(d.get("filename_pattern", f"{name}" + "_{date:%Y%m%d}_{bbox_hash}.nc")),
        default_variables=default_variables,
        optional_variables=optional_variables,
        use_variables=use_variables,
        variables=variables,
        mode=str(d.get("mode", "auto")),
        dataset_id=None if d.get("dataset_id") is None else str(d["dataset_id"]),
        dataset_nrt=None if d.get("dataset_nrt") is None else str(d["dataset_nrt"]),
        dataset_my=None if d.get("dataset_my") is None else str(d["dataset_my"]),
        nrt_cutoff=None if d.get("nrt_cutoff") is None else str(d["nrt_cutoff"]),
        temporal=_parse_temporal(d.get("temporal")),
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


def _parse_variable_catalog(raw: Any) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    if isinstance(raw, Mapping):
        default = _as_str_tuple(raw.get("default", ()))
        optional_raw = raw.get("optional", {}) or {}
        if not isinstance(optional_raw, Mapping):
            raise ValueError("variables.optional must be a mapping")
        optional = {str(name): _as_str_tuple(values) for name, values in optional_raw.items()}
        return default, optional
    default = _as_str_tuple(raw or ())
    return default, {}


def _parse_temporal(raw: Mapping[str, Any] | None) -> TemporalConfig:
    d = dict(raw or {})
    native = format_sampling(d.get("native_sampling", "24h"))
    supported_raw = d.get("supported_sampling", (native,))
    supported = tuple(dict.fromkeys(format_sampling(value) for value in _as_tuple(supported_raw)))
    if native not in supported:
        supported = (native, *supported)
    return TemporalConfig(
        native_sampling=native,
        supported_sampling=supported,
        sampling_mode=str(d.get("sampling_mode", "instantaneous")),
    )


def _resolve_variables(
    *,
    source_name: str,
    default_variables: tuple[str, ...],
    optional_variables: Mapping[str, tuple[str, ...]],
    use_variables: tuple[str, ...],
) -> tuple[str, ...]:
    selected: list[str] = []
    known_variables = set(default_variables)
    for values in optional_variables.values():
        known_variables.update(values)
    for item in use_variables or ("default",):
        if item == "default":
            selected.extend(default_variables)
        elif item in optional_variables:
            selected.extend(optional_variables[item])
        elif item in known_variables:
            selected.append(item)
        else:
            known = ", ".join(["default", *sorted(optional_variables), *sorted(known_variables)])
            raise ValueError(f"source {source_name!r} requested unknown variable or group {item!r}; known: {known}")
    return tuple(dict.fromkeys(selected))


def apply_source_variable_overrides(config: Config, overrides: SourceVariableOverrides | None) -> Config:
    """Return a new config with per-source ``use_variables`` overrides applied.

    Args:
        config: Parsed configuration.
        overrides: Mapping from source name to a variable/group name or list of
            variable/group names.

    Returns:
        Parsed configuration with updated source variable selections.

    Raises:
        ValueError: If an override names an unknown source or variable/group.
    """
    if not overrides:
        return config
    raw = dict(config.raw)
    sources = dict(raw.get("sources", {}) or {})
    for source_name, use_variables in overrides.items():
        if source_name not in sources:
            raise ValueError(f"unknown source {source_name!r} in variable override")
        source = dict(sources[source_name] or {})
        source["use_variables"] = list(_as_str_tuple(use_variables))
        sources[source_name] = source
    raw["sources"] = sources
    return parse_config(raw)


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
    preset: str | None = None,
    overrides: Mapping[str, Any] | None = None,
    conf_dir: str | Path | None = None,
) -> Config:
    """Load YAML configuration and return typed values.

    Args:
        preset: Optional preset name.
        overrides: Optional mapping merged last.
        conf_dir: Optional configuration directory.

    Returns:
        Parsed collekt configuration.
    """
    return parse_config(load_config(preset=preset, overrides=overrides, conf_dir=conf_dir))
