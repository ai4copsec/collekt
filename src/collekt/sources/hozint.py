"""HOZINT threat-intelligence adapter.

Wraps the ``hozint-apiclient`` command-line tool, which is invoked as a
subprocess (not imported) and writes Parquet reports into the collection
directory. Credentials are resolved by the tool itself from the environment.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from collekt.core.availability import Availability, AvailabilityMethod, AvailabilityStatus
from collekt.core.config import Config, SourceConfig
from collekt.core.diagnostics import DoctorCheck, DoctorStatus
from collekt.core.request import Request
from collekt.sources.base import (
    ProgressCallback,
    SourceAdapter,
    SourceResult,
    SourceStatus,
    null_progress,
    register_adapter,
)

TIME_FORMAT = "%Y-%m-%d"
DEFAULT_COMMAND = ("hozint-apiclient", "query", "--output-format", "parquet")


def _command(source: SourceConfig, out_dir: Path, request: Request) -> list[str]:
    cmd = [str(part) for part in source.raw.get("command", DEFAULT_COMMAND)]
    cmd += ["--output-dir", str(out_dir)]
    cmd += ["--from-time", request.start_datetime.strftime(TIME_FORMAT)]
    cmd += ["--to-time", request.end_datetime.strftime(TIME_FORMAT)]
    return cmd


def _run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=False)


def fetch_hozint(
    request: Request,
    source: SourceConfig,
    config: Config,
    request_dir: Path,
    *,
    progress: ProgressCallback = null_progress,
) -> list[SourceResult]:
    """Run the HOZINT client and collect the Parquet reports it produces."""
    out_dir = request_dir / source.path
    out_dir.mkdir(parents=True, exist_ok=True)
    dataset_id = source.dataset_id or source.name

    existing = sorted(out_dir.glob("*.parquet"))
    if existing and config.cache.reuse_existing and not config.cache.overwrite:
        return [_result(source, SourceStatus.REUSED, dataset_id, path=path) for path in existing]

    cmd = _command(source, out_dir, request)
    progress(source.name, "querying HOZINT reports")
    try:
        completed = _run(cmd)
    except FileNotFoundError as exc:
        return [_result(source, SourceStatus.SKIPPED, dataset_id, message=f"hozint-apiclient not available: {exc}")]
    if completed.returncode != 0:
        return [
            _result(
                source, SourceStatus.FAILED, dataset_id, message=f"hozint-apiclient exited with {completed.returncode}"
            )
        ]

    produced = sorted(out_dir.glob("*.parquet"))
    if not produced:
        return [
            _result(source, SourceStatus.SKIPPED, dataset_id, message="hozint-apiclient produced no parquet output")
        ]
    return [_result(source, SourceStatus.DOWNLOADED, dataset_id, path=path) for path in produced]


def _result(
    source: SourceConfig, status: SourceStatus, dataset_id: str, *, path: Path | None = None, message: str | None = None
) -> SourceResult:
    return SourceResult(
        source=source.name,
        status=status,
        path=path,
        dataset_id=dataset_id,
        variables=source.variables,
        message=message,
        format="parquet",
    )


def plan_hozint(request: Request, source: SourceConfig, config: Config, request_dir: Path) -> list[SourceResult]:
    """Plan the HOZINT query, reporting the command that would run."""
    details = {
        "provider": "hozint-apiclient",
        "method": "cli",
        "request": {"command": _command(source, request_dir / source.path, request)},
        "availability": Availability(AvailabilityStatus.NOT_CHECKED, AvailabilityMethod.NOT_CHECKED).as_dict(),
    }
    return [
        SourceResult(
            source=source.name,
            status=SourceStatus.PLANNED,
            dataset_id=source.dataset_id or source.name,
            variables=source.variables,
            format="parquet",
            details=details,
        )
    ]


def diagnose(config: Config, online: bool) -> list[DoctorCheck]:
    """Return HOZINT command-line tool diagnostics."""
    if shutil.which("hozint-apiclient"):
        return [DoctorCheck("hozint-apiclient tool", DoctorStatus.OK, "hozint-apiclient is on PATH")]
    return [
        DoctorCheck(
            "hozint-apiclient tool",
            DoctorStatus.WARN,
            "hozint-apiclient not found on PATH; HOZINT reports will be skipped",
        )
    ]


register_adapter(SourceAdapter(kind="hozint", fetch=fetch_hozint, plan=plan_hozint, diagnose=diagnose))
