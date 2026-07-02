"""Environment and configuration checks for collekt."""

from __future__ import annotations

import importlib.util
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from collekt.core.config import Config, SourceConfig, get_config


@dataclass(frozen=True)
class DoctorCheck:
    """One diagnostic check result."""

    name: str
    status: str
    message: str

    def as_dict(self) -> dict[str, str]:
        """Return a JSON-serializable mapping."""
        return {"name": self.name, "status": self.status, "message": self.message}


def _package_check(name: str, import_name: str) -> DoctorCheck:
    try:
        spec = importlib.util.find_spec(import_name)
    except ModuleNotFoundError:
        spec = None
    if spec is None:
        return DoctorCheck(name=name, status="warn", message=f"{import_name!r} is not installed")
    return DoctorCheck(name=name, status="ok", message=f"{import_name!r} is importable")


def _path_exists(path: Path | None) -> bool:
    return path is not None and path.expanduser().exists()


def _credential_checks(config: Config) -> list[DoctorCheck]:
    checks: list[DoctorCheck] = []
    if _path_exists(config.credentials.cdsapi_rc):
        checks.append(DoctorCheck("cds credentials", "ok", f"found {config.credentials.cdsapi_rc}"))
    else:
        checks.append(DoctorCheck("cds credentials", "warn", "CDS rc file not found; ERA5 downloads may fail"))

    cop_file = config.credentials.copernicusmarine_credentials_file
    cop_home = Path.home() / ".copernicusmarine"
    has_env = bool(
        os.environ.get("COPERNICUSMARINE_SERVICE_USERNAME") and os.environ.get("COPERNICUSMARINE_SERVICE_PASSWORD")
    )
    if _path_exists(cop_file):
        checks.append(DoctorCheck("copernicus credentials", "ok", f"found {cop_file}"))
    elif cop_home.exists():
        checks.append(DoctorCheck("copernicus credentials", "ok", f"found {cop_home}"))
    elif has_env:
        checks.append(DoctorCheck("copernicus credentials", "ok", "found Copernicus Marine environment variables"))
    else:
        checks.append(
            DoctorCheck(
                "copernicus credentials",
                "warn",
                "no Copernicus Marine credential file or environment variables found",
            )
        )
    return checks


def _source_checks(config: Config) -> list[DoctorCheck]:
    checks: list[DoctorCheck] = []
    for source in config.sources.values():
        if not source.enabled:
            continue
        variables = ",".join(source.variables) if source.variables else "-"
        checks.append(DoctorCheck(f"source {source.name}", "ok", f"{source.kind}; variables={variables}"))
    return checks


def _copernicusmarine_describe():
    try:
        import copernicusmarine
    except ImportError:
        return None
    return copernicusmarine.describe


def _cmems_dataset_ids(source: SourceConfig) -> tuple[str, ...]:
    return tuple(dict.fromkeys(x for x in (source.dataset_id, source.dataset_nrt, source.dataset_my) if x))


def _declared_cmems_variables(source: SourceConfig) -> tuple[str, ...]:
    variables = list(source.default_variables)
    for values in source.optional_variables.values():
        variables.extend(values)
    if not variables:
        variables.extend(source.variables)
    return tuple(dict.fromkeys(variables))


def _catalogue_variable_names(catalogue: Any, dataset_id: str) -> set[str]:
    names: set[str] = set()
    for product in getattr(catalogue, "products", []) or []:
        for dataset in getattr(product, "datasets", []) or []:
            if getattr(dataset, "dataset_id", None) != dataset_id:
                continue
            for version in getattr(dataset, "versions", []) or []:
                for part in getattr(version, "parts", []) or []:
                    for service in getattr(part, "services", []) or []:
                        for variable in getattr(service, "variables", []) or []:
                            short_name = getattr(variable, "short_name", None)
                            if short_name:
                                names.add(str(short_name))
    return names


def _cmems_online_checks(config: Config) -> list[DoctorCheck]:
    describe = _copernicusmarine_describe()
    if describe is None:
        return [DoctorCheck("cmems catalogue", "warn", "copernicusmarine is not installed; online checks skipped")]

    checks: list[DoctorCheck] = []
    cache: dict[str, Any] = {}
    for source in config.sources.values():
        if not source.enabled or source.kind != "cmems":
            continue
        declared_variables = set(_declared_cmems_variables(source))
        for dataset_id in _cmems_dataset_ids(source):
            check_name = f"cmems catalogue {source.name}"
            try:
                if dataset_id not in cache:
                    cache[dataset_id] = describe(dataset_id=dataset_id, disable_progress_bar=True, raise_on_error=True)
            except Exception as exc:  # noqa: BLE001 - provider/network errors are surfaced as diagnostics
                checks.append(DoctorCheck(check_name, "fail", f"{dataset_id}: catalogue lookup failed: {exc}"))
                continue

            available_variables = _catalogue_variable_names(cache[dataset_id], dataset_id)
            if not available_variables:
                checks.append(DoctorCheck(check_name, "fail", f"{dataset_id}: dataset not found in catalogue response"))
                continue
            missing = sorted(declared_variables - available_variables)
            if missing:
                checks.append(DoctorCheck(check_name, "fail", f"{dataset_id}: missing variables {', '.join(missing)}"))
            else:
                checks.append(
                    DoctorCheck(check_name, "ok", f"{dataset_id}: {len(declared_variables)} configured variables found")
                )
    return checks


def run_doctor(
    *,
    preset: str | None = None,
    conf_dir: str | Path | None = None,
    online: bool = False,
    config: Config | None = None,
) -> list[DoctorCheck]:
    """Run environment and configuration checks.

    Args:
        preset: Optional configuration preset.
        conf_dir: Optional configuration directory.
        online: If true, query provider catalogues for configured CMEMS datasets
            and variables.
        config: Optional preloaded configuration to check instead of loading from
            preset and conf_dir.

    Returns:
        Ordered diagnostic check results.
    """
    config = config or get_config(preset=preset, conf_dir=conf_dir)
    checks = [
        _package_check("copernicusmarine package", "copernicusmarine"),
        _package_check("cdsapi package", "cdsapi"),
        _package_check("ecmwf-opendata package", "ecmwf.opendata"),
        _package_check("xarray package", "xarray"),
        _package_check("damast package", "damast"),
        *_credential_checks(config),
        *_source_checks(config),
    ]
    if online:
        checks.extend(_cmems_online_checks(config))
    return checks
