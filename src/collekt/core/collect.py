"""Collection orchestration.

Runs a request against the enabled, matching sources, dispatching each to its
registered adapter (`collekt.sources.base.register_adapter`), then writes a
manifest. The orchestration is adapter-agnostic: it knows nothing about specific
providers, only the `SourceAdapter` contract.
"""

from __future__ import annotations

import json
import shutil
from dataclasses import replace

from collekt.core.batching import validate_batch_days
from collekt.core.config import Config, selection_error
from collekt.core.manifest import with_inspection, write_manifest
from collekt.core.naming import pattern_values, request_directory
from collekt.core.reporting import summarize
from collekt.core.request import Request
from collekt.core.result import Result
from collekt.sources.base import (
    ProgressCallback,
    SourceResult,
    SourceStatus,
    get_adapter,
    null_progress,
)


def _selected_sources(cfg: Config):
    for source in cfg.sources.values():
        if not source.enabled:
            continue
        yield source


def run_collection(
    request: Request,
    *,
    config: Config,
    strict: bool | None = None,
    use_cache: bool = True,
    dry_run: bool = False,
    progress: ProgressCallback = null_progress,
    batch_days: int | None = None,
) -> Result:
    """Run collection orchestration for a request.

    Args:
        request: Region, time window, and metadata.
        config: Resolved internal source configuration.
        strict: Override config strict mode. If true, warnings become a final
            `RuntimeError` after the manifest is written.
        use_cache: If true, reuse files already staged for this exact request
            (same region and time range). If false, delete the request directory
            and download everything again.
        dry_run: If true, plan provider requests and output paths without
            creating directories, downloading data, or writing a manifest.
        progress: Optional progress callback.
        batch_days: Positive maximum UTC calendar days per retrieval group;
            `None` retains the adapters' default behavior.

    Returns:
        Collection result with files, manifest path, and summary counts.

    Raises:
        RuntimeError: If strict mode is enabled and any source was skipped or failed.
    """
    validate_batch_days(batch_days)
    cfg = config
    if not use_cache:
        cfg = replace(cfg, cache=replace(cfg.cache, reuse_existing=False, overwrite=True))

    values = pattern_values(
        source="request",
        dataset_id=None,
        region=request.region,
        start=request.start_datetime,
        end=request.end_datetime,
    )
    request_dir = request_directory(cfg.output.root, cfg.output.request_pattern, values)
    selected = list(_selected_sources(cfg))
    errors = [message for source in selected if (message := selection_error(source))]
    if batch_days is not None:
        errors.extend(_batch_errors(selected))
    if errors:
        raise ValueError("; ".join(errors))
    if not dry_run and not use_cache and request_dir.exists() and request_dir != cfg.output.root:
        shutil.rmtree(request_dir)
    if not dry_run:
        request_dir.mkdir(parents=True, exist_ok=True)

    results: list[SourceResult] = []
    for source in selected:
        adapter = get_adapter(source.kind)
        if adapter is None:
            results.append(
                SourceResult(
                    source=source.name,
                    status=SourceStatus.SKIPPED,
                    message=f"unknown source kind {source.kind!r}",
                )
            )
            continue
        if batch_days is not None:
            results.extend(
                adapter.batch(
                    request,
                    source,
                    cfg,
                    request_dir,
                    batch_days=batch_days,
                    dry_run=dry_run,
                    progress=progress,
                )
            )
        elif dry_run:
            results.extend(adapter.plan(request, source, cfg, request_dir))
        else:
            results.extend(adapter.fetch(request, source, cfg, request_dir, progress=progress))

    results = _restore_batch_provenance(results, request_dir, cfg.output.manifest_name)
    result_tuple = tuple(results) if dry_run else with_inspection(tuple(results))
    summary = summarize(result_tuple)
    manifest_path = request_dir / cfg.output.manifest_name
    if not dry_run:
        write_manifest(
            request=request,
            config=cfg,
            request_dir=request_dir,
            manifest_path=manifest_path,
            results=result_tuple,
            summary=summary,
        )
    files = tuple(result.path for result in result_tuple if result.path is not None)
    effective_strict = cfg.cache.strict if strict is None else strict
    if effective_strict and summary.has_warnings:
        raise RuntimeError(f"collection completed with warnings; see {manifest_path}")
    return Result(
        output_dir=request_dir,
        manifest_path=manifest_path,
        files=files,
        results=result_tuple,
        summary=summary,
    )


def _batch_errors(selected):
    """Collect every reason batching is impossible, before anything is downloaded.

    A source that cannot be batched is a configuration error, not a provider
    failure: reporting it up front keeps a rejected run from leaving files from
    the other sources behind with no manifest describing them.
    """
    errors = []
    for source in selected:
        adapter = get_adapter(source.kind)
        if adapter is None:
            continue
        if adapter.batch is None:
            errors.append(f"source kind {source.kind!r} does not support batch_days")
        elif adapter.batch_check is not None:
            errors.extend(f"source {source.name!r}: {reason}" for reason in adapter.batch_check(source))
    return errors


def _restore_batch_provenance(results, request_dir, manifest_name):
    """Retain the actual retrieval provenance when a later run reuses files."""
    try:
        manifest = json.loads((request_dir / manifest_name).read_text())
    except (OSError, ValueError):
        return results
    previous = {(item.get("source"), item.get("path")): item for item in manifest.get("files", [])}
    restored = []
    for result in results:
        if result.status == SourceStatus.REUSED and result.path is not None:
            relative = result.path.relative_to(request_dir) if result.path.is_relative_to(request_dir) else result.path
            old = previous.get((result.source, str(relative)), {})
            provenance = {
                key: value for key, value in (old.get("details") or {}).items() if key in {"batch", "batches"}
            }
            if provenance:
                result = replace(result, details=(result.details or {}) | provenance)
        restored.append(result)
    return restored
