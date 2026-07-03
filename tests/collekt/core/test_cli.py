"""Tests for the collekt command-line interface."""

import pytest

import collekt.cli.main as cli_main
from collekt.cli.main import build_parser, run
from collekt.core.config import get_config
from collekt.sources.base import (
    SourceAdapter,
    SourceResult,
    SourceStatus,
    null_progress,
    register_adapter,
)

CLI_KIND = "clitest"


def _fetch(request, source, config, request_dir, *, progress=null_progress):
    if source.raw.get("fail"):
        return [SourceResult(source=source.name, status=SourceStatus.SKIPPED, message="boom")]
    out_dir = request_dir / source.path
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{source.name}.dat"
    path.write_text("data", encoding="utf-8")
    return [SourceResult(source=source.name, status=SourceStatus.DOWNLOADED, path=path, format="grib2")]


def _plan(request, source, config, request_dir):
    return [
        SourceResult(
            source=source.name,
            status=SourceStatus.PLANNED,
            format="grib2",
            details={"availability": {"status": "not_checked", "method": "not_checked"}},
        )
    ]


register_adapter(SourceAdapter(kind=CLI_KIND, fetch=_fetch, plan=_plan))


def _config(tmp_path, *, sources=("demo",), fail=False):
    raw_sources = {}
    for name in sources:
        source = {
            "kind": CLI_KIND,
            "path": name,
            "filename_pattern": "{source}.dat",
        }
        if fail:
            source["fail"] = True
        raw_sources[name] = source
    return get_config(
        overrides={"source_catalogs": [], "output": {"root": str(tmp_path / "out")}, "sources": raw_sources}
    )


def _dataset_config_file(tmp_path):
    path = tmp_path / "datasets.yaml"
    path.write_text("datasets: []\n", encoding="utf-8")
    return path


def _fetch_argv(config_file, out, *extra):
    return [
        "fetch",
        "--bbox",
        "0",
        "20",
        "35",
        "45",
        "--dataset-config",
        str(config_file),
        "--output-dir",
        str(out),
        *extra,
    ]


def _patch_dataset_config(monkeypatch, config):
    monkeypatch.setattr(cli_main.DatasetConfig, "from_yaml", staticmethod(lambda path: config))


def test_build_parser_requires_a_command():
    with pytest.raises(SystemExit):
        build_parser().parse_args([])


def test_fetch_dry_run_prints_plan(tmp_path, capsys, monkeypatch):
    _patch_dataset_config(monkeypatch, _config(tmp_path))
    code = run(_fetch_argv(_dataset_config_file(tmp_path), tmp_path / "out", "--dry-run"))
    out = capsys.readouterr().out

    assert code == 0
    assert "dry-run plan" in out
    assert "demo" in out


def test_fetch_downloads_and_reports_summary(tmp_path, capsys, monkeypatch):
    _patch_dataset_config(monkeypatch, _config(tmp_path))
    out = tmp_path / "out"
    code = run(_fetch_argv(_dataset_config_file(tmp_path), out))

    assert code == 0
    assert "collekt collection complete" in capsys.readouterr().out
    assert list(out.rglob("demo.dat"))


def test_fetch_dataset_config_selects_sources(tmp_path, capsys, monkeypatch):
    _patch_dataset_config(monkeypatch, _config(tmp_path, sources=("demo",)))
    code = run(_fetch_argv(_dataset_config_file(tmp_path), tmp_path / "out", "--dry-run"))
    out = capsys.readouterr().out

    assert code == 0
    assert "demo" in out
    assert "other" not in out


def test_fetch_strict_failure_returns_error(tmp_path, capsys, monkeypatch):
    _patch_dataset_config(monkeypatch, _config(tmp_path, fail=True))
    code = run(_fetch_argv(_dataset_config_file(tmp_path), tmp_path / "out", "--strict"))

    assert code == 1
    assert "ERROR" in capsys.readouterr().out


def test_fetch_without_a_region_is_an_error(tmp_path, capsys, monkeypatch):
    _patch_dataset_config(monkeypatch, _config(tmp_path))
    code = run(
        [
            "fetch",
            "--start",
            "2024-01-30",
            "--dataset-config",
            str(_dataset_config_file(tmp_path)),
            "--output-dir",
            str(tmp_path / "out"),
        ]
    )

    assert code == 1
    assert "provide a region" in capsys.readouterr().out


def test_doctor_returns_zero_for_default_config(capsys):
    code = run(["doctor"])

    assert code == 0
    assert "collekt doctor" in capsys.readouterr().out


def test_config_show_prints_merged_configuration(capsys):
    code = run(["config", "show"])

    assert code == 0
    assert "manifest.json" in capsys.readouterr().out
