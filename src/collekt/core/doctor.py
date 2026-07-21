"""Environment and configuration checks for collekt."""

from __future__ import annotations

from pathlib import Path

from collekt.core.config import STRUCTURAL_SOURCE_KEYS, Config, SourceConfig, get_config
from collekt.core.diagnostics import DoctorCheck, DoctorStatus, package_check
from collekt.sources.base import get_adapter, registered_kinds


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
        checks.append(DoctorCheck(f"source {source.name}", DoctorStatus.OK, f"{source.kind}; {detail}"))
        checks.extend(_unknown_raw_key_checks(source))
    return checks


def _unknown_raw_key_checks(source: SourceConfig) -> list[DoctorCheck]:
    adapter = get_adapter(source.kind)
    if adapter is None:
        return []
    unknown = sorted(set(source.raw) - STRUCTURAL_SOURCE_KEYS - adapter.known_raw_keys)
    if not unknown:
        return []
    return [
        DoctorCheck(
            f"source {source.name} config keys",
            DoctorStatus.WARN,
            f"unrecognized key(s) {', '.join(unknown)} (possible typo?); ignored by the {source.kind!r} adapter",
        )
    ]


def run_doctor(
    *,
    conf_dir: str | Path | None = None,
    online: bool = False,
    config: Config | None = None,
) -> list[DoctorCheck]:
    """Run environment and configuration checks.

    Args:
        conf_dir: Optional configuration directory.
        online: If true, allow provider diagnostic hooks to query remote
            catalogues where supported.
        config: Optional preloaded configuration to check instead of loading from
            conf_dir.

    Returns:
        Ordered diagnostic check results.
    """
    config = config or get_config(conf_dir=conf_dir)
    checks = [
        package_check("xarray package", "xarray"),
        package_check("damast package", "damast"),
    ]
    for kind in registered_kinds():
        adapter = get_adapter(kind)
        if adapter is not None and adapter.diagnose is not None:
            checks.extend(adapter.diagnose(config, online))
    checks.extend(_source_checks(config))
    return checks
