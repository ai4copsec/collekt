"""Tests for the collekt command-line interface."""

import pytest

from collekt.cli.main import build_parser, run
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


def _conf_dir(tmp_path, *, sources=("demo",), fail=False):
    conf = tmp_path / "conf"
    conf.mkdir()
    lines = ["sources:"]
    for name in sources:
        lines += [
            f"  {name}:",
            f"    kind: {CLI_KIND}",
            f"    path: {name}",
            '    filename_pattern: "{source}.dat"',
            "    variable_groups: [x]",
        ]
        if fail:
            lines.append("    fail: true")
    (conf / "default.yaml").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return conf


def _fetch_argv(conf, out, *extra):
    return ["fetch", "--bbox", "0", "20", "35", "45", "--conf-dir", str(conf), "--output-dir", str(out), *extra]


def test_build_parser_requires_a_command():
    with pytest.raises(SystemExit):
        build_parser().parse_args([])


def test_fetch_dry_run_prints_plan(tmp_path, capsys):
    conf = _conf_dir(tmp_path)
    code = run(_fetch_argv(conf, tmp_path / "out", "--dry-run"))
    out = capsys.readouterr().out

    assert code == 0
    assert "dry-run plan" in out
    assert "demo" in out


def test_fetch_downloads_and_reports_summary(tmp_path, capsys):
    conf = _conf_dir(tmp_path)
    out = tmp_path / "out"
    code = run(_fetch_argv(conf, out))

    assert code == 0
    assert "collekt collection complete" in capsys.readouterr().out
    assert list(out.rglob("demo.dat"))


def test_fetch_datasource_filter_restricts_sources(tmp_path, capsys):
    conf = _conf_dir(tmp_path, sources=("demo", "other"))
    code = run(_fetch_argv(conf, tmp_path / "out", "--datasource", "demo", "--dry-run"))
    out = capsys.readouterr().out

    assert code == 0
    assert "demo" in out
    assert "other" not in out


def test_fetch_strict_failure_returns_error(tmp_path, capsys):
    conf = _conf_dir(tmp_path, fail=True)
    code = run(_fetch_argv(conf, tmp_path / "out", "--strict"))

    assert code == 1
    assert "ERROR" in capsys.readouterr().out


def test_fetch_without_a_region_is_an_error(capsys):
    code = run(["fetch", "--start", "2024-01-30"])

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
