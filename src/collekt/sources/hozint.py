"""HOZINT threat-intelligence adapter.

Wraps the ``hozint-apiclient`` command-line tool, which is invoked as a
subprocess (not imported) and writes Parquet reports into the collection
directory. Credentials are resolved by the tool itself from the environment.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory

from collekt.core.availability import Availability, AvailabilityMethod, AvailabilityStatus
from collekt.core.batching import request_windows
from collekt.core.config import Config, SourceConfig
from collekt.core.diagnostics import DoctorCheck, DoctorStatus
from collekt.core.request import Request
from collekt.sources.base import (
    BatchOptions,
    ProgressCallback,
    SourceAdapter,
    SourceResult,
    SourceStatus,
    null_progress,
    register_adapter,
    should_reuse_cache,
)
from collekt.sources.batching.common import batch_details, failed

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
    if should_reuse_cache(bool(existing), config):
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


def _run_batch(
    request: Request,
    source: SourceConfig,
    config: Config,
    request_dir: Path,
    options: BatchOptions,
) -> list[SourceResult]:
    """Isolate each CLI invocation and cache only successfully completed windows."""
    results = []
    for window in request_windows(request, options.days):
        item = plan_hozint(window, source, config, request_dir)[0]
        batch = batch_details(source, window.start_datetime, window.end_datetime, item.details["request"])
        directory = request_dir / source.path / "batches" / batch["id"]
        batch["request"] = {"command": _command(source, directory, window)}
        item = replace(item, details=item.details | {"batch": batch, "request": batch["request"]})
        marker = directory / ".complete.json"
        paths = None
        if should_reuse_cache(marker.exists(), config):
            try:
                completed = json.loads(marker.read_text())
                candidates = [directory / name for name in completed["outputs"]]
                if all((directory / name).is_file() for name in completed["files"]):
                    paths = candidates
            except (OSError, ValueError, KeyError, TypeError):
                pass
        if paths is not None:
            results.extend(replace(item, status=SourceStatus.REUSED, path=path) for path in paths)
            if not paths:
                results.append(failed(item, "hozint-apiclient produced no parquet output (cached completed query)"))
            continue
        if options.dry_run:
            results.append(item)
            continue
        options.progress(source.name, f"querying HOZINT batch {batch['start']} to {batch['end']}")
        directory.parent.mkdir(parents=True, exist_ok=True)
        try:
            with TemporaryDirectory(prefix=".collekt-", dir=directory.parent) as temporary:
                output = Path(temporary) / "output"
                output.mkdir()
                completed = _run(_command(source, output, window))
                if completed.returncode != 0:
                    raise RuntimeError(f"hozint-apiclient exited with {completed.returncode}")
                names = [path.name for path in sorted(output.glob("*.parquet"))]
                files = [path.name for path in output.iterdir() if path.is_file()]
                (output / marker.name).write_text(json.dumps({"outputs": names, "files": files}))
                if directory.exists():
                    shutil.rmtree(directory)
                output.replace(directory)
            if names:
                results.extend(replace(item, status=SourceStatus.DOWNLOADED, path=directory / name) for name in names)
            else:
                results.append(failed(item, "hozint-apiclient produced no parquet output"))
        except Exception as exc:  # noqa: BLE001 - isolate CLI window failures
            results.append(failed(item, exc))
    return results


register_adapter(
    SourceAdapter(
        kind="hozint",
        batch=_run_batch,
        fetch=fetch_hozint,
        plan=plan_hozint,
        diagnose=diagnose,
        known_raw_keys=frozenset({"command"}),
    )
)
