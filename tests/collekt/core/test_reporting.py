"""Tests for reporting summary counts."""

from io import StringIO
from pathlib import Path

from rich.console import Console

from collekt.core.reporting import CliReporter, Summary, summarize
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
    console = Console(file=StringIO(), width=100)
    reporter = CliReporter(console=console)
    reporter.warnings([_result(SourceStatus.SKIPPED, message="unavailable", day="2026-06-25")])
    reporter.summary(Summary(downloaded=1), Path("out"), Path("out/manifest.json"))
    output = console.file.getvalue()
    assert "WARNING s 2026-06-25: unavailable" in output
    assert "collekt collection complete" in output
