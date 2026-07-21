"""Tests for shared adapter helpers (`sources/base.py`)."""

from collekt.core.config import get_config
from collekt.sources.base import SourceStatus, missing_after_fetch, should_reuse_cache


def _source(**overrides):
    cfg = get_config(overrides={"source_catalogs": [], "sources": {"s": {"kind": "cmems", **overrides}}})
    return cfg.sources["s"]


def test_should_reuse_cache_requires_existing_file_and_reuse_enabled():
    cfg = get_config(overrides={"source_catalogs": [], "cache": {"reuse_existing": True, "overwrite": False}})
    assert should_reuse_cache(True, cfg) is True
    assert should_reuse_cache(False, cfg) is False


def test_should_reuse_cache_respects_overwrite_and_reuse_flags():
    overwrite = get_config(overrides={"source_catalogs": [], "cache": {"reuse_existing": True, "overwrite": True}})
    assert should_reuse_cache(True, overwrite) is False

    no_reuse = get_config(overrides={"source_catalogs": [], "cache": {"reuse_existing": False, "overwrite": False}})
    assert should_reuse_cache(True, no_reuse) is False


def test_missing_after_fetch_builds_a_skipped_result():
    source = _source(variables=["uo"])
    result = missing_after_fetch(source, "my-dataset", day="2024-01-30", details={"temporal": {}})

    assert result.status == SourceStatus.SKIPPED
    assert result.source == "s"
    assert result.dataset_id == "my-dataset"
    assert result.variables == ("uo",)
    assert result.day == "2024-01-30"
    assert result.message == "download completed but file is missing"


def test_missing_after_fetch_appends_extra_context():
    source = _source()
    result = missing_after_fetch(source, "my-dataset", extra="response repr")

    assert result.message == "download completed but file is missing (response repr)"
