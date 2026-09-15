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
    # Built via Path, not hardcoded as "out/..." strings: Result.__str__ interpolates Path
    # objects directly, which render with the OS-native separator (backslashes on Windows).
    manifest_path = Path("out/manifest.json")
    a_path = Path("out/a.nc")
    result = Result(
        output_dir=Path("out"),
        manifest_path=manifest_path,
        files=(a_path,),
        results=(
            SourceResult(source="a", status=SourceStatus.DOWNLOADED, path=a_path),
            SourceResult(source="b", status=SourceStatus.SKIPPED, message="not available"),
        ),
        summary=Summary(downloaded=1, skipped=1),
    )

    text = str(result)
    assert repr(result) == text
    assert "Summary(planned=0, downloaded=1, reused=0, skipped=1, failed=0)" in text
    assert f"manifest: {manifest_path}" in text
    assert f"a: downloaded -> {a_path}" in text
    assert "b: skipped -> not available" in text

    result.render()

    output = capsys.readouterr().out
    assert "Summary(planned=0, downloaded=1, reused=0, skipped=1, failed=0)" in output
    assert f"manifest: {manifest_path}" in output
    assert f"a: downloaded -> {a_path}" in output
    assert "b: skipped -> not available" in output
