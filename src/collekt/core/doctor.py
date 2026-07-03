"""Environment and configuration checks for collekt."""

from __future__ import annotations

import importlib.util
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from collekt.core.availability import Coverage, _coverage_from_catalogue, static_coverage
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
        if source.variables:
            detail = f"variables={','.join(source.variables)}"
        elif source.available_variables:
            detail = f"{len(source.available_variables)} available variables, none selected"
        else:
            detail = "no variables"
        checks.append(DoctorCheck(f"source {source.name}", "ok", f"{source.kind}; {detail}"))
    return checks


def _copernicusmarine_describe():
    try:
        import copernicusmarine
    except ImportError:
        return None
    return copernicusmarine.describe


def _cmems_dataset_ids(source: SourceConfig) -> tuple[str, ...]:
    return (source.dataset_id,) if source.dataset_id else ()


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


def _coverage_mismatches(shipped: Coverage, catalogue: Coverage) -> list[str]:
    mismatches: list[str] = []
    tolerance = 1e-5
    for name in ("west", "east", "south", "north"):
        expected = getattr(shipped, name)
        actual = getattr(catalogue, name)
        if abs(expected - actual) > tolerance:
            mismatches.append(f"{name}={expected} (catalogue {actual})")
    if shipped.start is not None and catalogue.start is not None and shipped.start != catalogue.start:
        mismatches.append(f"start={shipped.start.isoformat()} (catalogue {catalogue.start.isoformat()})")
    if shipped.end is not None and catalogue.end is not None and shipped.end != catalogue.end:
        mismatches.append(f"end={shipped.end.isoformat()} (catalogue {catalogue.end.isoformat()})")
    return mismatches


def _cmems_online_checks(config: Config) -> list[DoctorCheck]:
    describe = _copernicusmarine_describe()
    if describe is None:
        return [DoctorCheck("cmems catalogue", "warn", "copernicusmarine is not installed; online checks skipped")]

    checks: list[DoctorCheck] = []
    cache: dict[str, Any] = {}
    for source in config.sources.values():
        if not source.enabled or source.kind != "cmems":
            continue
        shipped = set(source.available_variables)
        for dataset_id in _cmems_dataset_ids(source):
            check_name = f"cmems catalogue {source.name}"
            try:
                if dataset_id not in cache:
                    cache[dataset_id] = describe(dataset_id=dataset_id, disable_progress_bar=True, raise_on_error=True)
            except Exception as exc:  # noqa: BLE001 - provider/network errors are surfaced as diagnostics
                checks.append(DoctorCheck(check_name, "fail", f"{dataset_id}: catalogue lookup failed: {exc}"))
                continue

            catalogue_variables = _catalogue_variable_names(cache[dataset_id], dataset_id)
            if not catalogue_variables:
                checks.append(DoctorCheck(check_name, "fail", f"{dataset_id}: dataset not found in catalogue response"))
                continue
            stale = sorted(shipped - catalogue_variables)
            added = sorted(catalogue_variables - shipped)
            if stale:
                checks.append(
                    DoctorCheck(
                        check_name,
                        "fail",
                        f"{dataset_id}: available_variables absent from catalogue: {', '.join(stale)}",
                    )
                )
            elif added:
                checks.append(
                    DoctorCheck(
                        check_name,
                        "warn",
                        f"{dataset_id}: catalogue also offers {', '.join(added)}; consider updating available_variables",
                    )
                )
            else:
                checks.append(
                    DoctorCheck(
                        check_name, "ok", f"{dataset_id}: {len(shipped)} available_variables match the catalogue"
                    )
                )

            shipped_coverage = static_coverage(source)
            catalogue_coverage = _coverage_from_catalogue(cache[dataset_id], dataset_id)
            if shipped_coverage is None or catalogue_coverage is None:
                continue
            coverage_name = f"cmems coverage {source.name}"
            mismatches = _coverage_mismatches(shipped_coverage, catalogue_coverage)
            if mismatches:
                checks.append(
                    DoctorCheck(
                        coverage_name,
                        "warn",
                        f"{dataset_id}: declared coverage differs from catalogue: {', '.join(mismatches)}",
                    )
                )
            else:
                checks.append(DoctorCheck(coverage_name, "ok", f"{dataset_id}: declared coverage matches catalogue"))
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
