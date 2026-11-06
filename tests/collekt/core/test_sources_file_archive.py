"""Offline tests for the `file_archive` adapter and the bundled AIS catalogs.

Small zipped CSVs shaped like the AISDK and Marine Cadastre downloads are served through a
monkeypatched `requests.get`, then fetched through the normal `Fetcher` entry point.
"""

import io
import zipfile
from datetime import date

import pytest

from collekt import Fetcher
from collekt.core.config import get_config
from collekt.core.doctor import _unknown_raw_key_checks
from collekt.core.request import Region, Request
from collekt.sources.file_archive import SPEC_DIR, parse_archive_spec, remote_file_for_day

damast = pytest.importorskip("damast")
pl = pytest.importorskip("polars")

# Headers as served by the providers (checked against real downloads, 2026-11).
AISDK_HEADER = (
    "# Timestamp,Type of mobile,MMSI,Latitude,Longitude,Navigational status,ROT,SOG,COG,Heading,IMO,"
    "Callsign,Name,Ship type,Cargo type,Width,Length,Type of position fixing device,Draught,Destination,"
    "ETA,Data source type,A,B,C,D"
)
MARINE_CADASTRE_HEADER = (
    "MMSI,BaseDateTime,LAT,LON,SOG,COG,Heading,VesselName,IMO,CallSign,VesselType,Status,Length,Width,"
    "Draft,Cargo,TransceiverClass"
)
BASE_URL = "https://archive.test"


def _aisdk_row(stamp: str, mmsi: int, lat: float, lon: float) -> str:
    return (
        f"{stamp},Class A,{mmsi},{lat},{lon},Under way using engine,0.0,10.5,90.0,91,Unknown,OXYZ,NAME,"
        "Cargo,,12,80,GPS,4.5,AARHUS,,AIS,10,70,6,6"
    )


def _aisdk_csv(day: date) -> str:
    stamp = day.strftime("%d/%m/%Y")
    rows = [
        _aisdk_row(f"{stamp} 00:00:00", 1, 56.0, 10.0),
        _aisdk_row(f"{stamp} 23:59:59", 2, 56.5, 11.0),
        _aisdk_row(f"{stamp} 12:00:00", 3, 70.0, 30.0),  # outside the request bbox
    ]
    return "\n".join([AISDK_HEADER, *rows]) + "\n"


def _zip(members: dict[str, str]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as bundle:
        for name, text in members.items():
            bundle.writestr(name, text)
    return buffer.getvalue()


class _Response:
    def __init__(self, payload: bytes | None):
        self.payload = payload
        self.status_code = 404 if payload is None else 200

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        pass

    def iter_content(self, chunk_size):
        yield self.payload


@pytest.fixture
def served(monkeypatch):
    """Serve `files[url] = bytes` through `requests.get`; record every requested URL."""
    import requests

    files: dict[str, bytes] = {}
    calls: list[str] = []

    def fake_get(url, **kwargs):
        calls.append(url)
        return _Response(files.get(url))

    monkeypatch.setattr(requests, "get", fake_get)
    return files, calls


def _source(tmp_path, **override):
    source = {
        "kind": "file_archive",
        "path": "ais/test",
        "filename_pattern": "ais_{date:%Y%m%d}_{bbox_hash}.parquet",
        "dataset_id": "ais-test",
        "archive_root": str(tmp_path / "mirror"),
        "spec": "aisdk.spec.yaml",
        "periods": [
            {
                "every": "month",
                "until": "2024-02-29",
                "url": BASE_URL + "/{date:%Y}/aisdk-{date:%Y-%m}.zip",
                "member": "aisdk-{date:%Y-%m-%d}.csv",
            },
            {"every": "day", "start": "2024-03-01", "url": BASE_URL + "/{date:%Y}/aisdk-{date:%Y-%m-%d}.zip"},
        ],
        "time_column": "# Timestamp",
        "time_format": "%d/%m/%Y %H:%M:%S",
        "region_columns": ["Latitude", "Longitude"],
    }
    source.update(override)
    return source


def _config(tmp_path, out="out", **override):
    return get_config(
        overrides={
            "source_catalogs": [],
            "output": {"root": str(tmp_path / out)},
            "sources": {"ais": _source(tmp_path, **override)},
        }
    )


def _request(start="2024-03-01", end=None):
    return Request(region=Region.from_bbox((5, 15, 54, 58)), start=start, end=end)


def _serve_daily(files, day: date):
    files[f"{BASE_URL}/{day:%Y}/aisdk-{day:%Y-%m-%d}.zip"] = _zip({f"aisdk-{day:%Y-%m-%d}.csv": _aisdk_csv(day)})


def _rows(path):
    return damast.core.AnnotatedDataFrame.from_files([str(path)], metadata_required=False)


def test_remote_file_for_day_picks_the_covering_period(tmp_path):
    spec = parse_archive_spec(get_config(overrides={"sources": {"ais": _source(tmp_path)}}).sources["ais"])

    monthly = remote_file_for_day(spec, date(2023, 1, 5))
    daily = remote_file_for_day(spec, date(2024, 3, 1))

    assert monthly.url == f"{BASE_URL}/2023/aisdk-2023-01.zip"
    assert monthly.member == "aisdk-2023-01-05.csv"
    assert remote_file_for_day(spec, date(2023, 1, 31)).url == monthly.url
    assert daily.url == f"{BASE_URL}/2024/aisdk-2024-03-01.zip"
    assert daily.member is None


def test_parquet_mode_converts_with_provider_spec_and_filters_day_and_region(tmp_path, served):
    files, _ = served
    _serve_daily(files, date(2024, 3, 1))

    result = Fetcher(_request(), config=_config(tmp_path)).download()

    assert result.summary.downloaded == 1
    partition = tmp_path / "mirror" / "2024" / "03" / "2024-03-01.parquet"
    assert sorted(p.name for p in (tmp_path / "mirror").rglob("*.parquet")) == [partition.name]
    assert (tmp_path / "mirror" / ".mirrored" / "aisdk-2024-03-01.json").is_file()
    converted = _rows(partition)
    assert converted.lazyframe.collect_schema()["# Timestamp"] == pl.Datetime("us", "UTC")
    assert converted.metadata["Latitude"].unit is not None
    assert converted.metadata["Latitude"].description.startswith("Latitude coordinate")

    output = _rows(result.files[0])
    assert sorted(output.dataframe.collected()["MMSI"].to_list()) == [1, 2]


def test_spec_only_keeps_csv_untouched_and_matches_parquet_output(tmp_path, served):
    files, _ = served
    _serve_daily(files, date(2024, 3, 1))
    parquet = Fetcher(_request(), config=_config(tmp_path, out="parquet")).download()

    spec_only_cfg = _config(tmp_path, out="spec_only", convert="spec_only", archive_root=str(tmp_path / "raw"))
    spec_only = Fetcher(_request(), config=spec_only_cfg).download()

    mirrored = tmp_path / "raw" / "aisdk-2024-03-01"
    assert (mirrored / "aisdk-2024-03-01.csv").read_text() == _aisdk_csv(date(2024, 3, 1))
    sidecar = damast.core.MetaData.load_yaml(mirrored / "aisdk-2024-03-01.spec.yaml")
    assert sidecar["MMSI"].description.startswith("Maritime Mobile Service Identity")
    left = _rows(parquet.files[0]).dataframe.collected().sort("MMSI")
    right = _rows(spec_only.files[0]).dataframe.collected().sort("MMSI")
    assert left["MMSI"].to_list() == right["MMSI"].to_list()
    assert left["# Timestamp"].to_list() == right["# Timestamp"].to_list()


def test_monthly_archive_is_downloaded_once_and_each_day_reads_its_member(tmp_path, served):
    files, calls = served
    days = [date(2023, 1, 1), date(2023, 1, 2)]
    files[f"{BASE_URL}/2023/aisdk-2023-01.zip"] = _zip({f"aisdk-{d:%Y-%m-%d}.csv": _aisdk_csv(d) for d in days})

    result = Fetcher(_request(start="2023-01-01", end="2023-01-02"), config=_config(tmp_path)).download()

    assert calls == [f"{BASE_URL}/2023/aisdk-2023-01.zip"]
    assert result.summary.downloaded == 2
    for day, path in zip(days, sorted(result.files), strict=True):
        stamps = _rows(path).dataframe.collected()["# Timestamp"]
        assert {stamp.date() for stamp in stamps} == {day}


def test_rows_from_two_remote_files_for_the_same_day_are_merged(tmp_path, served):
    files, _ = served
    day = date(2024, 3, 1)
    _serve_daily(files, day)
    # The next day's file also carries one late row of 2024-03-01, plus a duplicate of an existing row.
    late = _aisdk_row("01/03/2024 23:59:59", 9, 57.0, 12.0)
    duplicate = _aisdk_row("01/03/2024 00:00:00", 1, 56.0, 10.0)
    next_day = _aisdk_csv(date(2024, 3, 2)) + f"{late}\n{duplicate}\n"
    files[f"{BASE_URL}/2024/aisdk-2024-03-02.zip"] = _zip({"aisdk-2024-03-02.csv": next_day})

    Fetcher(_request(start="2024-03-01", end="2024-03-02"), config=_config(tmp_path)).download()

    partition = _rows(tmp_path / "mirror" / "2024" / "03" / "2024-03-01.parquet").dataframe.collected()
    assert sorted(partition["MMSI"].to_list()) == [1, 2, 3, 9]


def test_parquet_mirror_is_a_damast_archive_the_local_adapter_reads(tmp_path, served):
    files, _ = served
    _serve_daily(files, date(2024, 3, 1))
    Fetcher(_request(), config=_config(tmp_path)).download()
    local_source = {
        "kind": "local",
        "path": "local",
        "filename_pattern": "local_{date:%Y%m%d}_{bbox_hash}.parquet",
        "archive_root": str(tmp_path / "mirror"),
        "layout": "time:# Timestamp+daily:%Y/%m/%Y-%m-%d",
        "region_columns": ["Latitude", "Longitude"],
    }
    cfg = get_config(
        overrides={"source_catalogs": [], "output": {"root": str(tmp_path / "local")}, "sources": {"m": local_source}}
    )

    result = Fetcher(_request(), config=cfg).download()

    assert sorted(_rows(result.files[0]).dataframe.collected()["MMSI"].to_list()) == [1, 2]


def test_missing_remote_file_skips_the_day(tmp_path, served):
    result = Fetcher(_request(), config=_config(tmp_path)).download()

    assert result.summary.downloaded == 0
    assert result.summary.skipped == 1
    assert not (tmp_path / "mirror" / ".mirrored").exists()
    assert not list((tmp_path / "mirror").rglob("*.parquet"))


def test_second_request_reuses_the_mirror_without_downloading(tmp_path, served):
    files, calls = served
    _serve_daily(files, date(2024, 3, 1))
    Fetcher(_request(), config=_config(tmp_path, out="first")).download()

    second = Fetcher(_request(), config=_config(tmp_path, out="second")).download()

    assert second.summary.downloaded == 1
    assert len(calls) == 1


def test_dry_run_plans_urls_without_network_or_disk(tmp_path, served):
    _, calls = served

    result = Fetcher(_request(start="2024-03-01", end="2024-03-02"), config=_config(tmp_path)).plan()

    assert result.summary.planned == 2
    assert calls == []
    assert not (tmp_path / "mirror").exists()


def test_without_a_spec_metadata_is_inferred(tmp_path, served):
    files, _ = served
    _serve_daily(files, date(2024, 3, 1))

    result = Fetcher(_request(), config=_config(tmp_path, spec=None)).download()

    assert result.summary.downloaded == 1
    assert _rows(result.files[0]).metadata["MMSI"].description is None


def test_missing_configured_column_is_raised(tmp_path, served):
    files, _ = served
    _serve_daily(files, date(2024, 3, 1))

    with pytest.raises(ValueError, match="not found in the archive"):
        Fetcher(_request(), config=_config(tmp_path, region_columns=["lat", "lon"])).download()


@pytest.mark.parametrize(
    ("spec_file", "header"),
    [("aisdk.spec.yaml", AISDK_HEADER), ("marine_cadastre.spec.yaml", MARINE_CADASTRE_HEADER)],
)
def test_bundled_specs_describe_the_provider_columns(spec_file, header):
    metadata = damast.core.MetaData.load_yaml(SPEC_DIR / spec_file)

    assert {column.name for column in metadata.columns} == set(header.split(","))


@pytest.mark.parametrize("key", ["aisdk_ais", "marine_cadastre_ais"])
def test_bundled_ais_catalogs_parse_and_have_no_unknown_keys(key):
    source = get_config().sources[key]

    spec = parse_archive_spec(source)

    assert spec.spec_path is not None and spec.spec_path.is_file()
    assert remote_file_for_day(spec, date(2023, 6, 1)) is not None
    assert _unknown_raw_key_checks(source) == []


@pytest.mark.parametrize(
    ("day", "url", "member"),
    [
        (date(2024, 2, 29), "http://aisdata.ais.dk/2024/aisdk-2024-02.zip", "aisdk-2024-02-29.csv"),
        (date(2024, 3, 1), "http://aisdata.ais.dk/2024/aisdk-2024-03-01.zip", None),
        (date(2025, 2, 26), "http://aisdata.ais.dk/2025/aisdk-2025-02-26.zip", None),
        (date(2025, 2, 27), "http://aisdata.ais.dk/aisdk-2025-02-27.zip", None),
        (date(2026, 11, 3), "http://aisdata.ais.dk/aisdk-2026-11-03.zip", None),
    ],
)
def test_bundled_aisdk_catalog_follows_the_provider_layout_changes(day, url, member):
    spec = parse_archive_spec(get_config().sources["aisdk_ais"])

    remote = remote_file_for_day(spec, day)

    assert (remote.url, remote.member) == (url, member)
