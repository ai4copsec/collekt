"""Offline tests for the `local` archive adapter.

Builds a small damast-partitioned Parquet archive in a tmp dir, then exercises
`fetch_local`/`plan_local` through the normal `Fetcher` entry point - the same
pattern as the eOdyn archive tests.
"""

import pytest

from collekt import Fetcher
from collekt.core.config import get_config
from collekt.core.request import Region, Request

damast = pytest.importorskip("damast")


def _write_archive(archive_root):
    import datetime as dt

    import polars

    df = polars.DataFrame(
        {
            "mmsi": [1, 1, 2, 2, 3],
            "timestamp": [
                dt.datetime(2026, 1, 1, 3),
                dt.datetime(2026, 1, 1, 5),
                dt.datetime(2026, 1, 2, 1),
                dt.datetime(2026, 1, 2, 2),
                dt.datetime(2026, 1, 3, 10),
            ],
            "lat": [40.0, 41.0, 41.5, 90.0, 42.0],
            "lon": [4.0, 5.0, 5.5, 100.0, 6.0],
        }
    )
    columns = [
        damast.core.DataSpecification(name="mmsi"),
        damast.core.DataSpecification(name="timestamp"),
        damast.core.DataSpecification(name="lat"),
        damast.core.DataSpecification(name="lon"),
    ]
    adf = damast.core.AnnotatedDataFrame(df, damast.core.MetaData(columns=columns))
    adf.export_partitioned(archive_root, damast.core.ByTime("timestamp", every="1d"))


def _config(tmp_path, archive_root, **override):
    source = {
        "kind": "local",
        "enabled": True,
        "path": "local/ais",
        "filename_pattern": "ais_{dataset_id}_{date:%Y%m%d}_{bbox_hash}.parquet",
        "dataset_id": "ais-test-archive",
        "archive_root": str(archive_root),
        "layout": "time:timestamp+daily:%Y-%m-%d",
        "region_columns": ["lat", "lon"],
        "temporal_sampling": "24h",
    }
    source.update(override)
    return get_config(
        overrides={
            "source_catalogs": [],
            "output": {"root": str(tmp_path / "out")},
            "sources": {"ais_archive": source},
        }
    )


def _request(bbox=(0, 10, 38, 45), start="2026-01-01", end=None):
    return Request(region=Region.from_bbox(bbox), start=start, end=end)


def test_local_reads_matching_day_and_filters_by_region(tmp_path):
    archive = tmp_path / "archive"
    _write_archive(archive)
    cfg = _config(tmp_path, archive)

    result = Fetcher(_request(), config=cfg).download()

    assert result.summary.downloaded == 1
    loaded = damast.core.AnnotatedDataFrame.from_files([str(result.files[0])], metadata_required=False)
    rows = loaded.dataframe.collected()
    assert rows.height == 2
    assert set(rows["mmsi"].to_list()) == {1}


def test_local_skips_rows_outside_region(tmp_path):
    archive = tmp_path / "archive"
    _write_archive(archive)
    cfg = _config(tmp_path, archive)

    result = Fetcher(_request(start="2026-01-02"), config=cfg).download()

    # mmsi 2's second row (lat 90/lon 100) falls outside the request bbox - only one row left.
    assert result.summary.downloaded == 1
    loaded = damast.core.AnnotatedDataFrame.from_files([str(result.files[0])], metadata_required=False)
    assert loaded.dataframe.collected().height == 1


def test_local_skips_day_with_no_archive_file(tmp_path):
    archive = tmp_path / "archive"
    archive.mkdir()
    cfg = _config(tmp_path, archive)

    result = Fetcher(_request(), config=cfg).download()

    assert result.summary.skipped == 1
    assert "no archive file matches" in result.results[0].message


def test_local_skips_when_no_rows_in_region(tmp_path):
    archive = tmp_path / "archive"
    _write_archive(archive)
    cfg = _config(tmp_path, archive)

    # mmsi 3's only row (2026-01-03) is far outside this bbox.
    result = Fetcher(_request(bbox=(-170, -160, -80, -70), start="2026-01-03"), config=cfg).download()

    assert result.summary.skipped == 1
    assert "no rows within the request region" in result.results[0].message


def test_local_reuses_cached_output_on_second_run(tmp_path):
    archive = tmp_path / "archive"
    _write_archive(archive)
    cfg = _config(tmp_path, archive)
    request = _request()

    first = Fetcher(request, config=cfg).download()
    assert first.summary.downloaded == 1

    second = Fetcher(request, config=cfg).download()
    assert second.summary.reused == 1


def test_local_dry_run_plans_days(tmp_path):
    archive = tmp_path / "archive"
    _write_archive(archive)
    cfg = _config(tmp_path, archive)

    result = Fetcher(_request(start="2026-01-01", end="2026-01-02"), config=cfg).plan()

    assert result.summary.planned == 2
    assert not result.output_dir.exists()


def test_local_requires_archive_root(tmp_path):
    cfg = _config(tmp_path, tmp_path / "archive")
    cfg.sources["ais_archive"].raw["archive_root"] = ""

    with pytest.raises(ValueError, match="archive_root"):
        Fetcher(_request(), config=cfg).download()


def test_local_requires_region_columns(tmp_path):
    cfg = _config(tmp_path, tmp_path / "archive", region_columns=[])

    with pytest.raises(ValueError, match="region_columns"):
        Fetcher(_request(), config=cfg).download()


def test_local_requires_a_time_based_layout(tmp_path):
    # A `column:`-only (or plain-path) layout has no time dimension to split per-day results
    # by, so it is rejected rather than silently producing duplicate/unfiltered-by-day output.
    cfg = _config(tmp_path, tmp_path / "archive", layout="column:mmsi:vessel_{mmsi}")

    with pytest.raises(ValueError, match="'layout' must be a 'time:' or 'time\\+column:'"):
        Fetcher(_request(), config=cfg).download()


def _write_flat_archive(archive_root):
    """Write an *unpartitioned* archive: two plain Parquet files, no time-encoded naming."""
    import datetime as dt

    import polars

    archive_root.mkdir(parents=True, exist_ok=True)
    columns = [
        damast.core.DataSpecification(name="mmsi"),
        damast.core.DataSpecification(name="timestamp"),
        damast.core.DataSpecification(name="lat"),
        damast.core.DataSpecification(name="lon"),
    ]
    part1 = polars.DataFrame(
        {
            "mmsi": [1, 1],
            "timestamp": [dt.datetime(2026, 1, 1, 3), dt.datetime(2026, 1, 1, 5)],
            "lat": [40.0, 41.0],
            "lon": [4.0, 5.0],
        }
    )
    part2 = polars.DataFrame(
        {
            "mmsi": [2, 2, 3],
            "timestamp": [dt.datetime(2026, 1, 2, 1), dt.datetime(2026, 1, 2, 2), dt.datetime(2026, 1, 3, 10)],
            "lat": [41.5, 90.0, 42.0],
            "lon": [5.5, 100.0, 6.0],
        }
    )
    damast.core.AnnotatedDataFrame(part1, damast.core.MetaData(columns=columns)).export(archive_root / "part-a.parquet")
    damast.core.AnnotatedDataFrame(part2, damast.core.MetaData(columns=columns)).export(archive_root / "part-b.parquet")


def _glob_config(tmp_path, archive_root, **override):
    source = {
        "kind": "local",
        "enabled": True,
        "path": "local/ais",
        "filename_pattern": "ais_{dataset_id}_{date:%Y%m%d}_{bbox_hash}.parquet",
        "dataset_id": "ais-test-archive",
        "archive_root": str(archive_root),
        "file_pattern": "*.parquet",
        "time_column": "timestamp",
        "region_columns": ["lat", "lon"],
        "temporal_sampling": "24h",
    }
    source.update(override)
    return get_config(
        overrides={
            "source_catalogs": [],
            "output": {"root": str(tmp_path / "out")},
            "sources": {"ais_archive": source},
        }
    )


def test_local_glob_reads_matching_day_and_filters_by_region(tmp_path):
    archive = tmp_path / "archive"
    _write_flat_archive(archive)
    cfg = _glob_config(tmp_path, archive)

    result = Fetcher(_request(), config=cfg).download()

    assert result.summary.downloaded == 1
    loaded = damast.core.AnnotatedDataFrame.from_files([str(result.files[0])], metadata_required=False)
    rows = loaded.dataframe.collected()
    assert rows.height == 2
    assert set(rows["mmsi"].to_list()) == {1}


def test_local_glob_skips_rows_outside_region(tmp_path):
    archive = tmp_path / "archive"
    _write_flat_archive(archive)
    cfg = _glob_config(tmp_path, archive)

    result = Fetcher(_request(start="2026-01-02"), config=cfg).download()

    # mmsi 2's second row (lat 90/lon 100) falls outside the request bbox - only one row left.
    assert result.summary.downloaded == 1
    loaded = damast.core.AnnotatedDataFrame.from_files([str(result.files[0])], metadata_required=False)
    assert loaded.dataframe.collected().height == 1


def test_local_glob_skips_day_with_no_matching_files(tmp_path):
    archive = tmp_path / "archive"
    archive.mkdir()
    cfg = _glob_config(tmp_path, archive)

    result = Fetcher(_request(), config=cfg).download()

    assert result.summary.skipped == 1
    assert "no file matches" in result.results[0].message


def test_local_glob_requires_time_column(tmp_path):
    cfg = _glob_config(tmp_path, tmp_path / "archive", time_column="")

    with pytest.raises(ValueError, match="'file_pattern' requires 'time_column'"):
        Fetcher(_request(), config=cfg).download()


def test_local_rejects_layout_and_file_pattern_together(tmp_path):
    cfg = _glob_config(tmp_path, tmp_path / "archive", layout="time:timestamp+daily:%Y-%m-%d")

    with pytest.raises(ValueError, match="mutually exclusive"):
        Fetcher(_request(), config=cfg).download()


def test_local_requires_layout_or_file_pattern(tmp_path):
    cfg = _config(tmp_path, tmp_path / "archive", layout="")

    with pytest.raises(ValueError, match="one of 'layout' or 'file_pattern' is required"):
        Fetcher(_request(), config=cfg).download()


def test_local_time_plus_column_layout_globs_within_the_day(tmp_path):
    import datetime as dt

    import polars

    archive = tmp_path / "archive"
    df = polars.DataFrame(
        {
            "mmsi": [1, 2],
            "timestamp": [dt.datetime(2026, 1, 1, 3)] * 2,
            "lat": [40.0, 41.0],
            "lon": [4.0, 5.0],
        }
    )
    columns = [
        damast.core.DataSpecification(name="mmsi"),
        damast.core.DataSpecification(name="timestamp"),
        damast.core.DataSpecification(name="lat"),
        damast.core.DataSpecification(name="lon"),
    ]
    adf = damast.core.AnnotatedDataFrame(df, damast.core.MetaData(columns=columns))
    # "time+column:timestamp+daily+mmsi:%Y-%m-%d/mmsi_{mmsi}" writes here (SaveAs uses the
    # same template syntax as ByExpr's filename_fn - see SaveAs._build_strategy).
    adf.export_partitioned(
        archive, damast.core.ByExpr(polars.col("mmsi"), filename_fn=lambda k: f"2026-01-01/mmsi_{k}")
    )
    cfg = _config(tmp_path, archive, layout="time+column:timestamp+daily+mmsi:%Y-%m-%d/mmsi_{mmsi}")

    result = Fetcher(_request(start="2026-01-01"), config=cfg).download()

    assert result.summary.downloaded == 1
    loaded = damast.core.AnnotatedDataFrame.from_files([str(result.files[0])], metadata_required=False)
    assert loaded.dataframe.collected().height == 2


def _write_monthly_archive(archive_root):
    """Write an archive partitioned *coarser* than a day - one file for the whole month."""
    import datetime as dt

    import polars

    df = polars.DataFrame(
        {
            "mmsi": [1, 2, 3],
            "timestamp": [dt.datetime(2026, 1, 1, 3), dt.datetime(2026, 1, 2, 3), dt.datetime(2026, 1, 20, 3)],
            "lat": [40.0, 41.0, 42.0],
            "lon": [4.0, 5.0, 6.0],
        }
    )
    columns = [damast.core.DataSpecification(name=n) for n in ("mmsi", "timestamp", "lat", "lon")]
    adf = damast.core.AnnotatedDataFrame(df, damast.core.MetaData(columns=columns))
    adf.export_partitioned(archive_root, damast.core.ByTime("timestamp", every="1mo"))


def test_local_layout_coarser_than_a_day_still_splits_rows_per_day(tmp_path):
    # A monthly `time:` layout resolves every request day to the same month file. Row filtering
    # is what scopes a day, so each day must still get only its own rows - not the whole bucket.
    archive = tmp_path / "archive"
    _write_monthly_archive(archive)
    cfg = _config(tmp_path, archive, layout="time:timestamp+1mo:%Y-%m-%d")

    result = Fetcher(_request(start="2026-01-01", end="2026-01-02"), config=cfg).download()

    assert result.summary.downloaded == 2
    import polars

    per_day = {f.name: polars.read_parquet(f) for f in result.files}
    assert [d.height for d in per_day.values()] == [1, 1]
    # 2026-01-20 shares the month file but lies outside the request entirely.
    for frame in per_day.values():
        assert frame["timestamp"].to_list()[0].day in {1, 2}
    assert len({f.name for f in result.files}) == 2


def test_local_keeps_the_last_instant_of_a_day(tmp_path):
    # The day filter is half-open [start, next_day), so the last instant of a day belongs to that
    # day and the first instant of the next one does not, at any column resolution. Pins the
    # boundary against a future switch back to an explicit end bound, which has to be spelled
    # 23:59:59.999999999 to be right for a nanosecond column.
    import datetime as dt

    import polars

    epoch = dt.datetime(1970, 1, 1)

    def _ns(moment, extra=0):
        return int((moment - epoch).total_seconds()) * 1_000_000_000 + extra

    archive = tmp_path / "archive"
    archive.mkdir()
    df = polars.DataFrame(
        {
            "mmsi": [1, 2],
            "timestamp": polars.Series(
                [_ns(dt.datetime(2026, 1, 1, 23, 59, 59), 999_999_999), _ns(dt.datetime(2026, 1, 2))]
            ).cast(polars.Datetime("ns")),
            "lat": [40.0, 41.0],
            "lon": [4.0, 5.0],
        }
    )
    columns = [damast.core.DataSpecification(name=n) for n in ("mmsi", "timestamp", "lat", "lon")]
    damast.core.AnnotatedDataFrame(df, damast.core.MetaData(columns=columns)).export(archive / "all.parquet")
    cfg = _glob_config(tmp_path, archive)

    result = Fetcher(_request(start="2026-01-01"), config=cfg).download()

    assert result.summary.downloaded == 1
    rows = polars.read_parquet(result.files[0])
    assert rows.height == 1
    assert rows["mmsi"].to_list() == [1]


def test_local_selects_only_the_requested_variables(tmp_path):
    # A `variables` subset narrows the frame, so the archive's metadata has to be narrowed with
    # it - `AnnotatedDataFrame` rejects metadata describing a column the frame no longer has.
    import polars

    archive = tmp_path / "archive"
    _write_archive(archive)
    cfg = _config(tmp_path, archive, variables=["mmsi", "lat", "lon"])

    result = Fetcher(_request(), config=cfg).download()

    assert result.summary.downloaded == 1
    rows = polars.read_parquet(result.files[0])
    assert rows.columns == ["mmsi", "lat", "lon"]
    assert rows.height == 2


def test_local_glob_selects_only_the_requested_variables(tmp_path):
    import polars

    archive = tmp_path / "archive"
    _write_flat_archive(archive)
    cfg = _glob_config(tmp_path, archive, variables=["mmsi", "lat", "lon"])

    result = Fetcher(_request(), config=cfg).download()

    assert result.summary.downloaded == 1
    assert polars.read_parquet(result.files[0]).columns == ["mmsi", "lat", "lon"]


def test_local_layout_matches_compression_suffixed_files(tmp_path):
    # `SaveAs.expected_paths` appends damast's own extension, so an archive written as
    # `<name>.zst.parquet` has to be matched despite the extra suffix.
    import datetime as dt

    import polars

    archive = tmp_path / "archive"
    archive.mkdir()
    df = polars.DataFrame(
        {
            "mmsi": [1],
            "timestamp": [dt.datetime(2026, 1, 1, 3)],
            "lat": [40.0],
            "lon": [4.0],
        }
    )
    columns = [damast.core.DataSpecification(name=n) for n in ("mmsi", "timestamp", "lat", "lon")]
    adf = damast.core.AnnotatedDataFrame(df, damast.core.MetaData(columns=columns))
    adf.export(archive / "2026-01-01.zst.parquet")
    cfg = _config(tmp_path, archive)

    result = Fetcher(_request(start="2026-01-01"), config=cfg).download()

    assert result.summary.downloaded == 1
    assert polars.read_parquet(result.files[0]).height == 1


def test_local_layout_ignores_unrelated_files_with_a_similar_name(tmp_path):
    # The compression fallback allows one extra dot-separated segment - not an arbitrary suffix.
    import datetime as dt

    import polars

    archive = tmp_path / "archive"
    archive.mkdir()
    df = polars.DataFrame({"mmsi": [1], "timestamp": [dt.datetime(2026, 1, 1, 3)], "lat": [40.0], "lon": [4.0]})
    columns = [damast.core.DataSpecification(name=n) for n in ("mmsi", "timestamp", "lat", "lon")]
    adf = damast.core.AnnotatedDataFrame(df, damast.core.MetaData(columns=columns))
    adf.export(archive / "2026-01-01_v2.parquet")
    cfg = _config(tmp_path, archive)

    result = Fetcher(_request(start="2026-01-01"), config=cfg).download()

    assert result.summary.skipped == 1
    assert "no archive file matches" in result.results[0].message


def test_local_raises_when_a_configured_column_is_missing(tmp_path):
    # A column the archive does not have is a misconfiguration affecting every day, so it is
    # raised rather than reported as a per-day skip indistinguishable from "no data here".
    archive = tmp_path / "archive"
    _write_archive(archive)
    cfg = _config(tmp_path, archive, region_columns=["latitude", "lon"])

    with pytest.raises(ValueError, match="latitude not found in the archive"):
        Fetcher(_request(), config=cfg).download()
