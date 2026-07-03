"""Offline tests for collection orchestration and the manifest.

These exercise the generic engine through a registered fake adapter, so they do
not depend on any concrete provider (those arrive in phase 3).
"""

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from collekt import Fetcher
from collekt.core.config import get_config
from collekt.core.request import Region, Request
from collekt.sources.base import (
    SourceAdapter,
    SourceResult,
    SourceStatus,
    null_progress,
    register_adapter,
)

MANIFEST_SCHEMA = (
    Path(__file__).resolve().parents[3] / "src" / "collekt" / "resources" / "schema" / "manifest.schema.json"
)

FAKE_KIND = "fake"


def _fake_path(source, request_dir: Path) -> Path:
    return request_dir / source.path / f"{source.name}.dat"


def _fake_fetch(request, source, config, request_dir, *, progress=null_progress):
    if source.raw.get("fail"):
        return [
            SourceResult(
                source=source.name,
                status=SourceStatus.SKIPPED,
                dataset_id=source.dataset_id or source.name,
                variables=source.variables,
                message="not available",
            )
        ]
    path = _fake_path(source, request_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and config.cache.reuse_existing and not config.cache.overwrite:
        status = SourceStatus.REUSED
    else:
        progress(source.name, "downloading")
        path.write_text("data", encoding="utf-8")
        status = SourceStatus.DOWNLOADED
    return [
        SourceResult(
            source=source.name,
            status=status,
            path=path,
            dataset_id=source.dataset_id or source.name,
            variables=source.variables,
            format="grib2",
            details={"temporal": {"actual_sampling": source.temporal_sampling}},
        )
    ]


def _fake_plan(request, source, config, request_dir):
    return [
        SourceResult(
            source=source.name,
            status=SourceStatus.PLANNED,
            path=_fake_path(source, request_dir),
            dataset_id=source.dataset_id or source.name,
            variables=source.variables,
            format="grib2",
            details={
                "availability": {"status": "not_checked", "method": "not_checked", "reason": None, "coverage": None}
            },
        )
    ]


register_adapter(SourceAdapter(kind=FAKE_KIND, fetch=_fake_fetch, plan=_fake_plan))


def _config(tmp_path, **source_override):
    source = {"kind": FAKE_KIND, "enabled": True, "path": "fake", "filename_pattern": "{source}.dat"}
    source.update(source_override)
    return get_config(overrides={"source_catalogs": [], "output": {"root": str(tmp_path)}, "sources": {"s": source}})


def _request():
    return Request(region=Region.from_bbox((-6, 20, 35, 45)), start="2026-06-25")


def test_download_writes_manifest_with_relative_paths(tmp_path):
    result = Fetcher(_request(), config=_config(tmp_path)).download()

    assert result.summary.downloaded == 1
    assert result.summary.skipped == 0
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert set(manifest) == {
        "manifest_schema_version",
        "request",
        "summary",
        "sources",
        "files",
        "planned",
        "warnings",
        "failures",
    }
    assert manifest["manifest_schema_version"] == "0.1"
    assert "sampling" not in manifest["request"]
    assert "preset" not in manifest["request"]
    assert manifest["files"][0]["status"] == "downloaded"
    assert not Path(manifest["files"][0]["path"]).is_absolute()
    assert manifest["files"][0]["inspection"]["size_bytes"] > 0


def test_dry_run_plans_without_downloading(tmp_path):
    result = Fetcher(_request(), config=_config(tmp_path)).plan()

    assert result.summary.planned == 1
    assert result.summary.downloaded == 0
    assert result.results[0].status == SourceStatus.PLANNED
    assert not result.output_dir.exists()


def test_selected_source_runs_from_config_selection(tmp_path):
    result = Fetcher(_request(), config=_config(tmp_path)).download()

    assert result.summary.downloaded == 1


def test_source_with_available_variables_requires_a_selection(tmp_path):
    cfg = _config(tmp_path, available_variables=["uo", "vo"])
    with pytest.raises(ValueError, match="requires a variable selection"):
        Fetcher(_request(), config=cfg).download()
    # Selecting a subset of available_variables clears the error.
    ok = _config(tmp_path, available_variables=["uo", "vo"], variables=["uo"])
    assert Fetcher(_request(), config=ok).download().summary.downloaded == 1


def test_depth_source_requires_a_depth_selection(tmp_path):
    cfg = _config(tmp_path, available_variables=["uo"], variables=["uo"], has_depth=True)
    with pytest.raises(ValueError, match="has a depth dimension"):
        Fetcher(_request(), config=cfg).download()
    ok = _config(tmp_path, available_variables=["uo"], variables=["uo"], has_depth=True, depth=[0.0, 1.0])
    assert Fetcher(_request(), config=ok).download().summary.downloaded == 1


def test_cache_reuse_on_second_download(tmp_path):
    cfg = _config(tmp_path)
    first = Fetcher(_request(), config=cfg).download()
    second = Fetcher(_request(), config=cfg).download()

    assert first.summary.downloaded == 1
    assert second.summary.reused == 1


def test_use_cache_false_deletes_and_redownloads(tmp_path):
    cfg = _config(tmp_path)
    first = Fetcher(_request(), config=cfg).download()
    stale = first.output_dir / "stale.txt"
    stale.write_text("old", encoding="utf-8")

    second = Fetcher(_request(), config=cfg).download(use_cache=False)

    assert second.summary.downloaded == 1
    assert second.summary.reused == 0
    assert not stale.exists()


def test_unknown_source_kind_is_skipped(tmp_path):
    cfg = get_config(
        overrides={
            "source_catalogs": [],
            "output": {"root": str(tmp_path)},
            "sources": {"s": {"kind": "nope", "enabled": True}},
        }
    )
    result = Fetcher(_request(), config=cfg).download()

    assert result.summary.skipped == 1
    assert "unknown source kind" in result.results[0].message


def test_strict_mode_raises_on_warning(tmp_path):
    cfg = _config(tmp_path, fail=True)
    result = Fetcher(_request(), config=cfg).download()
    assert result.summary.skipped == 1

    with pytest.raises(RuntimeError, match="collection completed with warnings"):
        Fetcher(_request(), config=cfg, strict=True).download()


def test_shipped_manifest_schema_is_valid_draft_2020_12():
    schema = json.loads(MANIFEST_SCHEMA.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)


def test_fetcher_exports_directory_and_zip(tmp_path):
    import zipfile

    fetcher = Fetcher(_request(), config=_config(tmp_path / "stage"))
    result = fetcher.download()
    directory = fetcher.export_directory(tmp_path / "exported")
    archive = fetcher.export_zip(tmp_path / "exported.zip")

    assert (directory / result.manifest_path.name).exists()
    with zipfile.ZipFile(archive) as zip_file:
        names = set(zip_file.namelist())
    assert "manifest.json" in names
    assert any(name.endswith(".dat") for name in names)
