"""User-facing dataset selection configuration."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar

import yaml

from collekt.core.config.loader import load_config
from collekt.core.config.schema import Config, SourceConfig, parse_config


def _as_tuple(value: Iterable[Any] | Any | None) -> tuple[Any, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    return tuple(value)


def _as_str_tuple(value: Iterable[Any] | Any | None) -> tuple[str, ...]:
    return tuple(str(item) for item in _as_tuple(value))


def _as_float_pair(value: Iterable[Any] | None) -> tuple[float, float] | None:
    if value is None:
        return None
    pair = tuple(float(item) for item in value)
    if len(pair) != 2:
        raise ValueError("depth must contain exactly two values: [min, max]")
    return pair


@dataclass(frozen=True)
class DatasetSelection:
    """Base class for one selected dataset."""

    key: str
    variables: tuple[str, ...] = ()

    provider: ClassVar[str]

    def __init__(self, key: str, variables: Iterable[str] | None = None) -> None:
        object.__setattr__(self, "key", str(key))
        object.__setattr__(self, "variables", _as_str_tuple(variables))

    def source_overrides(self) -> dict[str, Any]:
        """Return source fields selected by this dataset request."""
        return {"variables": list(self.variables)}

    def as_dict(self) -> dict[str, Any]:
        """Return a YAML-serializable representation."""
        data: dict[str, Any] = {"provider": self.provider, "key": self.key}
        if self.variables:
            data["variables"] = list(self.variables)
        return data


@dataclass(frozen=True, init=False)
class CMEMS(DatasetSelection):
    """Copernicus Marine dataset selection."""

    provider: ClassVar[str] = "cmems"
    depth: tuple[float, float] | None = None

    def __init__(
        self,
        key: str,
        variables: Iterable[str] | None = None,
        depth: Iterable[float] | None = None,
    ) -> None:
        super().__init__(key, variables)
        object.__setattr__(self, "depth", _as_float_pair(depth))

    def source_overrides(self) -> dict[str, Any]:
        data = super().source_overrides()
        if self.depth is not None:
            data["depth"] = list(self.depth)
        return data

    def as_dict(self) -> dict[str, Any]:
        data = super().as_dict()
        if self.depth is not None:
            data["depth"] = list(self.depth)
        return data


@dataclass(frozen=True, init=False)
class ECMWFOpenData(DatasetSelection):
    """ECMWF Open Data forecast selection."""

    provider: ClassVar[str] = "ecmwf_open_data"


@dataclass(frozen=True, init=False)
class ERA5(DatasetSelection):
    """ERA5 reanalysis selection."""

    provider: ClassVar[str] = "era5"


@dataclass(frozen=True, init=False)
class Eodyn(DatasetSelection):
    """eOdyn dataset selection."""

    provider: ClassVar[str] = "eodyn"


@dataclass(frozen=True, init=False)
class SkyTruth(DatasetSelection):
    """SkyTruth Cerulean slick selection."""

    provider: ClassVar[str] = "skytruth"
    limit: int | None = None

    def __init__(self, key: str = "skytruth", *, limit: int | None = None) -> None:
        super().__init__(key, ())
        object.__setattr__(self, "limit", limit)

    def source_overrides(self) -> dict[str, Any]:
        data: dict[str, Any] = {}
        if self.limit is not None:
            data["limit"] = self.limit
        return data

    def as_dict(self) -> dict[str, Any]:
        data = {"provider": self.provider, "key": self.key}
        if self.limit is not None:
            data["limit"] = self.limit
        return data


@dataclass(frozen=True, init=False)
class CopernicusDataSpace(DatasetSelection):
    """Copernicus Data Space product selection."""

    provider: ClassVar[str] = "copernicus_dataspace"
    max_records: int | None = None

    def __init__(self, key: str, *, max_records: int | None = None) -> None:
        super().__init__(key, ())
        object.__setattr__(self, "max_records", max_records)

    def source_overrides(self) -> dict[str, Any]:
        data: dict[str, Any] = {}
        if self.max_records is not None:
            data["max_records"] = self.max_records
        return data

    def as_dict(self) -> dict[str, Any]:
        data = {"provider": self.provider, "key": self.key}
        if self.max_records is not None:
            data["max_records"] = self.max_records
        return data


@dataclass(frozen=True, init=False)
class Hozint(DatasetSelection):
    """HOZINT threat-intelligence report selection.

    A credential-gated feature source with no variable selection; the
    `hozint-apiclient` tool resolves its own credentials and writes Parquet.
    """

    provider: ClassVar[str] = "hozint"

    def __init__(self, key: str = "hozint") -> None:
        super().__init__(key, ())

    def source_overrides(self) -> dict[str, Any]:
        return {}

    def as_dict(self) -> dict[str, Any]:
        return {"provider": self.provider, "key": self.key}


DatasetLike = CMEMS | ECMWFOpenData | ERA5 | Eodyn | SkyTruth | CopernicusDataSpace | Hozint

_PROVIDERS: dict[str, type[DatasetLike]] = {
    cls.provider: cls for cls in (CMEMS, ECMWFOpenData, ERA5, Eodyn, SkyTruth, CopernicusDataSpace, Hozint)
}


@dataclass(frozen=True, init=False)
class DatasetConfig:
    """Datasets and provider parameters requested for one collection run."""

    datasets: tuple[DatasetLike, ...] = field(default_factory=tuple)

    def __init__(self, *datasets: DatasetLike) -> None:
        object.__setattr__(self, "datasets", tuple(datasets))

    @classmethod
    def from_yaml(cls, path: str | Path) -> DatasetConfig:
        """Load a dataset-selection preset from YAML."""
        with open(path, encoding="utf-8") as handle:
            raw = yaml.safe_load(handle) or {}
        return cls.from_dict(raw)

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> DatasetConfig:
        """Build a dataset config from a mapping with a ``datasets`` list."""
        items = raw.get("datasets", raw)
        if not isinstance(items, list):
            raise ValueError("DatasetConfig YAML must contain a 'datasets' list")
        return cls(*(_dataset_from_mapping(item) for item in items))

    def as_dict(self) -> dict[str, Any]:
        """Return a YAML-serializable representation."""
        return {"datasets": [dataset.as_dict() for dataset in self.datasets]}

    def resolve(self, conf_dir: str | Path | None = None) -> Config:
        """Resolve selected datasets against the catalog.

        Args:
            conf_dir: Optional configuration directory that extends the bundled
                catalog with additional or overriding dataset definitions.

        Returns:
            Parsed configuration containing only the selected datasets.

        Raises:
            ValueError: If a selected key is unknown, its provider does not match
                the catalog source kind, or a required variable/depth is missing.
        """
        catalog = load_config(conf_dir=conf_dir)
        sources = dict(catalog.get("sources", {}) or {})
        selected: dict[str, Any] = {}
        for dataset in self.datasets:
            if dataset.key not in sources:
                raise ValueError(f"unknown dataset key {dataset.key!r}")
            raw_source = dict(sources[dataset.key] or {})
            source_kind = str(raw_source.get("kind", dataset.key))
            if source_kind != dataset.provider:
                raise ValueError(f"dataset {dataset.key!r} is a {source_kind!r} source, not {dataset.provider!r}")
            raw_source.update(dataset.source_overrides())
            selected[dataset.key] = raw_source
        raw = dict(catalog)
        raw["sources"] = selected
        config = parse_config(raw)
        _validate_selected_sources(config)
        return config


def _dataset_from_mapping(raw: Mapping[str, Any]) -> DatasetLike:
    provider = str(raw.get("provider", "")).strip()
    if provider not in _PROVIDERS:
        known = ", ".join(sorted(_PROVIDERS))
        raise ValueError(f"unknown dataset provider {provider!r}; expected one of: {known}")
    cls = _PROVIDERS[provider]
    data = dict(raw)
    data.pop("provider", None)
    return cls(**data)


def _validate_selected_sources(config: Config) -> None:
    for source in config.sources.values():
        _validate_variables(source)
        _validate_depth(source)


def _validate_variables(source: SourceConfig) -> None:
    if not source.available_variables:
        return
    if not source.variables:
        raise ValueError(
            f"dataset {source.name!r} requires variables; choose from: {', '.join(source.available_variables)}"
        )
    unknown = sorted(set(source.variables) - set(source.available_variables))
    if unknown:
        raise ValueError(
            f"dataset {source.name!r} requested unknown variables {', '.join(unknown)}; "
            f"choose from: {', '.join(source.available_variables)}"
        )


def _validate_depth(source: SourceConfig) -> None:
    if source.has_depth and source.raw.get("depth") is None:
        raise ValueError(f"dataset {source.name!r} has a depth dimension; set depth=[min, max]")
