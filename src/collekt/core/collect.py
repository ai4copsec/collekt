"""Collection orchestration.

Runs a request against the enabled, matching sources, dispatching each to its
registered adapter (`collekt.sources.base.register_adapter`), then writes a
manifest. The orchestration is adapter-agnostic: it knows nothing about specific
providers, only the `SourceAdapter` contract.
"""

from __future__ import annotations

import shutil
from dataclasses import replace
from pathlib import Path

from collekt.core.config import (
    Config,
    SourceConfig,
    SourceVariableOverrides,
    apply_source_variable_overrides,
    get_config,
)
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


def _source_matches_request(source: SourceConfig, request: Request) -> bool:
    groups = request.variable_groups
    return not groups or bool(set(source.variable_groups).intersection(groups))


def run_collection(
    request: Request,
    *,
    preset: str | None = None,
    config: Config | None = None,
    conf_dir: str | Path | None = None,
    strict: bool | None = None,
    source_variable_overrides: SourceVariableOverrides | None = None,
    use_datasources: list[str] | None = None,
    use_cache: bool = True,
    dry_run: bool = False,
    progress: ProgressCallback = null_progress,
) -> Result:
    """Run collection orchestration for a request.

    Args:
        request: Region, time window, and requested variable groups.
        preset: Optional configuration preset.
        config: Optional preloaded configuration.
        conf_dir: Optional configuration directory.
        strict: Override config strict mode. If true, warnings become a final
            `RuntimeError` after the manifest is written.
        source_variable_overrides: Optional per-source ``use_variables`` overrides.
        use_datasources: Optional lower-cased source names to restrict the run to.
        use_cache: If true, reuse files already staged for this exact request
            (same region and time range). If false, delete the request directory
            and download everything again.
        dry_run: If true, plan provider requests and output paths without
            creating directories, downloading data, or writing a manifest.
        progress: Optional progress callback.

    Returns:
        Collection result with files, manifest path, and summary counts.

    Raises:
        RuntimeError: If strict mode is enabled and any source was skipped or failed.
    """
    cfg = config or get_config(preset=preset, conf_dir=conf_dir)
    cfg = apply_source_variable_overrides(cfg, source_variable_overrides)
    if not use_cache:
        cfg = replace(cfg, cache=replace(cfg.cache, reuse_existing=False, overwrite=True))

    values = pattern_values(
        source="request",
        dataset_id=None,
        region=request.region,
        start=request.start_datetime,
        end=request.end_datetime,
        sampling=request.sampling_label,
    )
    request_dir = request_directory(cfg.output.root, cfg.output.request_pattern, values)
    if not dry_run and not use_cache and request_dir.exists() and request_dir != cfg.output.root:
        shutil.rmtree(request_dir)
    if not dry_run:
        request_dir.mkdir(parents=True, exist_ok=True)

    results: list[SourceResult] = []
    for source in cfg.sources.values():
        if not source.enabled or not _source_matches_request(source, request):
            continue
        if use_datasources is not None and source.name.lower() not in use_datasources:
            continue
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
        if dry_run:
            results.extend(adapter.plan(request, source, cfg, request_dir))
        else:
            results.extend(adapter.fetch(request, source, cfg, request_dir, progress=progress))

    result_tuple = tuple(results) if dry_run else with_inspection(tuple(results))
    summary = summarize(result_tuple)
    manifest_path = request_dir / cfg.output.manifest_name
    if not dry_run:
        write_manifest(
            request=request,
            config=cfg,
            preset=preset,
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
