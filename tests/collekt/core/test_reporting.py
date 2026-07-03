"""Tests for reporting summary counts."""

from pathlib import Path

from rich.console import Console

from collekt.core.reporting import CliReporter, Summary, summarize
from collekt.core.result import Result
from collekt.sources.base import SourceResult, SourceStatus


def _result(status: SourceStatus, **kwargs) -> SourceResult:
    return SourceResult(source="s", status=status, **kwargs)


def test_summarize_counts_statuses():
    results = [
        _result(SourceStatus.DOWNLOADED),
        _result(SourceStatus.DOWNLOADED),
        _result(SourceStatus.REUSED),
        _result(SourceStatus.SKIPPED, message="not available"),
        _result(SourceStatus.FAILED, message="boom"),
    ]
    summary = summarize(results)
    assert summary.as_dict() == {"planned": 0, "downloaded": 2, "reused": 1, "skipped": 1, "failed": 1}
    assert summary.has_warnings is True


def test_summary_without_warnings():
    summary = Summary(downloaded=3, reused=1)
    assert summary.has_warnings is False


def test_reporter_renders_warnings_and_summary_without_error():
    from io import StringIO

    console = Console(file=StringIO(), width=100)
    reporter = CliReporter(console=console)
    reporter.warnings([_result(SourceStatus.SKIPPED, message="unavailable", day="2026-06-25")])
    reporter.summary(Summary(downloaded=1), Path("out"), Path("out/manifest.json"))
    output = console.file.getvalue()
    assert "WARNING s 2026-06-25: unavailable" in output
    assert "collekt collection complete" in output


def test_result_render_prints_summary_manifest_and_status_lines(capsys):
    result = Result(
        output_dir=Path("out"),
        manifest_path=Path("out/manifest.json"),
        files=(Path("out/a.nc"),),
        results=(
            SourceResult(source="a", status=SourceStatus.DOWNLOADED, path=Path("out/a.nc")),
            SourceResult(source="b", status=SourceStatus.SKIPPED, message="not available"),
        ),
        summary=Summary(downloaded=1, skipped=1),
    )

    text = str(result)
    assert repr(result) == text
    assert "Summary(planned=0, downloaded=1, reused=0, skipped=1, failed=0)" in text
    assert "manifest: out/manifest.json" in text
    assert "a: downloaded -> out/a.nc" in text
    assert "b: skipped -> not available" in text

    result.render()

    output = capsys.readouterr().out
    assert "Summary(planned=0, downloaded=1, reused=0, skipped=1, failed=0)" in output
    assert "manifest: out/manifest.json" in output
    assert "a: downloaded -> out/a.nc" in output
    assert "b: skipped -> not available" in output
