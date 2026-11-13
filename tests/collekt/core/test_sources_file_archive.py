"""Offline tests for the `file_archive` adapter and the bundled AIS catalogs.

Small zipped or compressed CSVs shaped like the AISDK and Marine Cadastre downloads, and index
pages listing them, are served through a monkeypatched `requests.get`, then fetched through the
normal `Fetcher` entry point.
"""

import io
import zipfile
from datetime import date
from pathlib import Path

import pytest

from collekt import Fetcher
from collekt.core.config import get_config
from collekt.core.doctor import _unknown_raw_key_checks
from collekt.core.request import Region, Request
from collekt.sources.file_archive import (
    SPEC_DIR,
    latest_published_day,
    parse_archive_spec,
    remote_file_for_day,
    resolve_archive_root,
)

damast = pytest.importorskip("damast")
pl = pytest.importorskip("polars")

# Headers as served by the providers (checked against real downloads, 2026-11).
AISDK_HEADER = (
    "# Timestamp,Type of mobile,MMSI,Latitude,Longitude,Navigational status,ROT,SOG,COG,Heading,IMO,"
    "Callsign,Name,Ship type,Cargo type,Width,Length,Type of position fixing device,Draught,Destination,"
    "ETA,Data source type,A,B,C,D"
)
MARINE_CADASTRE_HEADER = (
    "mmsi,base_date_time,longitude,latitude,sog,cog,heading,vessel_name,imo,call_sign,vessel_type,status,"
    "length,width,draft,cargo,transceiver"
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

    @property
    def text(self):
        return self.payload.decode()

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
    """Serve `files[url] = bytes` through `requests.get` (an exception value is raised); record every requested URL."""
    import requests

    files: dict[str, bytes | Exception] = {}
    calls: list[str] = []

    def fake_get(url, **kwargs):
        calls.append(url)
        payload = files.get(url)
        if isinstance(payload, Exception):
            raise payload
        return _Response(payload)

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
    assert (tmp_path / "mirror" / ".mirrored" / "parquet" / "aisdk-2024-03-01.json").is_file()
    assert not list((tmp_path / "mirror" / ".downloads").iterdir())
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


def test_members_of_one_monthly_zip_sharing_a_day_are_merged(tmp_path, served):
    files, _ = served
    # The second day's file also carries a late row of the first day.
    late = _aisdk_row("01/01/2023 23:59:30", 9, 57.0, 12.0)
    second = _aisdk_csv(date(2023, 1, 2)) + f"{late}\n"
    members = {"aisdk-2023-01-01.csv": _aisdk_csv(date(2023, 1, 1)), "aisdk-2023-01-02.csv": second}
    files[f"{BASE_URL}/2023/aisdk-2023-01.zip"] = _zip(members)

    result = Fetcher(_request(start="2023-01-01"), config=_config(tmp_path)).download()

    assert result.summary.downloaded == 1
    partition = _rows(tmp_path / "mirror" / "2023" / "01" / "2023-01-01.parquet").dataframe.collected()
    assert sorted(partition["MMSI"].to_list()) == [1, 2, 3, 9]


def test_rows_with_unparseable_timestamps_are_dropped(tmp_path, served):
    files, _ = served
    broken = _aisdk_row("not a date", 7, 56.0, 10.0)
    csv = _aisdk_csv(date(2024, 3, 1)) + f"{broken}\n"
    files[f"{BASE_URL}/2024/aisdk-2024-03-01.zip"] = _zip({"aisdk-2024-03-01.csv": csv})

    result = Fetcher(_request(), config=_config(tmp_path)).download()

    assert result.summary.downloaded == 1
    assert sorted(_rows(result.files[0]).dataframe.collected()["MMSI"].to_list()) == [1, 2]


@pytest.mark.parametrize(
    ("override", "match"),
    [({"time_format": "%Y-%m-%d %H:%M:%S"}, "matches time_format"), ({"time_column": "Time"}, "not found")],
)
def test_configuration_mistake_is_raised_and_the_download_kept(tmp_path, served, override, match):
    files, calls = served
    _serve_daily(files, date(2024, 3, 1))

    with pytest.raises(ValueError, match=match):
        Fetcher(_request(), config=_config(tmp_path, **override)).download()
    result = Fetcher(_request(), config=_config(tmp_path)).download()

    assert result.summary.downloaded == 1
    assert len(calls) == 1


def test_switching_convert_mode_on_one_archive_root_mirrors_again(tmp_path, served):
    files, _ = served
    _serve_daily(files, date(2024, 3, 1))
    Fetcher(_request(), config=_config(tmp_path, out="parquet")).download()

    result = Fetcher(_request(), config=_config(tmp_path, out="spec_only", convert="spec_only")).download()

    assert result.summary.downloaded == 1
    assert (tmp_path / "mirror" / "aisdk-2024-03-01" / "aisdk-2024-03-01.csv").is_file()


def test_members_with_the_same_name_in_different_folders(tmp_path, served):
    files, _ = served
    day = date(2024, 3, 1)
    rows = _aisdk_csv(day).splitlines()
    first = "\n".join([rows[0], rows[1]]) + "\n"
    second = "\n".join([rows[0], rows[2]]) + "\n"
    files[f"{BASE_URL}/2024/aisdk-2024-03-01.zip"] = _zip({"a/day.csv": first, "b/day.csv": second})

    parquet = Fetcher(_request(), config=_config(tmp_path, out="parquet")).download()
    spec_only_cfg = _config(tmp_path, out="spec_only", convert="spec_only", archive_root=str(tmp_path / "raw"))
    spec_only = Fetcher(_request(), config=spec_only_cfg).download()

    assert sorted(_rows(parquet.files[0]).dataframe.collected()["MMSI"].to_list()) == [1, 2]
    assert spec_only.summary.skipped == 1
    assert "several CSV members are named day.csv" in spec_only.results[0].message


def test_unreadable_mirrored_file_skips_only_its_day(tmp_path, served, monkeypatch):
    import collekt.sources.file_archive as file_archive

    files, _ = served
    for day in (date(2024, 3, 1), date(2024, 3, 2)):
        _serve_daily(files, day)
    real_read = file_archive.read_day_subset

    def read(files, *, day, **kwargs):
        if day == date(2024, 3, 1):
            raise pl.exceptions.ComputeError("malformed line")
        return real_read(files, day=day, **kwargs)

    monkeypatch.setattr(file_archive, "read_day_subset", read)

    result = Fetcher(_request(start="2024-03-01", end="2024-03-02"), config=_config(tmp_path)).download()

    assert result.summary.downloaded == 1
    assert result.summary.skipped == 1
    assert "malformed line" in result.results[0].message


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


@pytest.mark.parametrize("key", ["aisdk_ais", "marine_cadastre_ais", "marine_cadastre_ais_west"])
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


DENMARK = {"west": 3.25, "east": 16.51, "south": 54.36, "north": 58.26, "start": "2006-03-01", "end": "2024-12-31"}


def test_request_outside_the_coverage_region_is_skipped_without_downloading(tmp_path, served):
    files, calls = served
    _serve_daily(files, date(2024, 3, 1))
    gulf_of_mexico = Region.from_bbox((-90.5, -89.5, 29.0, 30.0))

    result = Fetcher(
        Request(region=gulf_of_mexico, start="2024-03-01"), config=_config(tmp_path, coverage=DENMARK)
    ).download()

    assert result.summary.skipped == 1
    assert "outside the available coverage" in result.results[0].message
    assert calls == []


def test_day_outside_the_coverage_dates_is_skipped_without_downloading(tmp_path, served):
    files, calls = served
    _serve_daily(files, date(2025, 1, 2))

    result = Fetcher(_request(start="2025-01-02"), config=_config(tmp_path, coverage=DENMARK)).download()

    assert result.summary.skipped == 1
    assert "outside the available date range" in result.results[0].message
    assert calls == []


def test_request_inside_the_coverage_is_fetched(tmp_path, served):
    files, _ = served
    _serve_daily(files, date(2024, 3, 1))

    result = Fetcher(_request(), config=_config(tmp_path, coverage=DENMARK)).download()

    assert result.summary.downloaded == 1


def test_entries_differing_only_in_region_share_the_mirror_and_never_duplicate_rows(tmp_path, served):
    files, calls = served
    rows = [
        _aisdk_row("01/03/2024 01:00:00", 1, 30.0, -170.0),  # east of the antimeridian
        _aisdk_row("01/03/2024 02:00:00", 2, 30.0, 178.0),  # west of it
    ]
    csv = "\n".join([AISDK_HEADER, *rows]) + "\n"
    files[f"{BASE_URL}/2024/aisdk-2024-03-01.zip"] = _zip({"aisdk-2024-03-01.csv": csv})
    span = {"south": 20.0, "north": 40.0, "start": "2024-01-01", "end": "2024-12-31"}
    east = _source(tmp_path, path="east", coverage={"west": -180.0, "east": -65.0, **span})
    west = _source(tmp_path, path="west", coverage={"west": 167.0, "east": 180.0, **span})
    cfg = get_config(
        overrides={"source_catalogs": [], "output": {"root": str(tmp_path / "out")}, "sources": {"e": east, "w": west}}
    )
    world = Region.from_bbox((-180, 180, -90, 90))

    result = Fetcher(Request(region=world, start="2024-03-01"), config=cfg).download()

    assert calls == [f"{BASE_URL}/2024/aisdk-2024-03-01.zip"]
    assert result.summary.downloaded == 2
    by_source = {item.source: _rows(item.path).dataframe.collected()["MMSI"].to_list() for item in result.results}
    assert by_source == {"e": [1], "w": [2]}


def test_bundled_marine_cadastre_entries_differ_only_in_region_and_output():
    sources = get_config().sources
    east, west = sources["marine_cadastre_ais"].raw, sources["marine_cadastre_ais_west"].raw

    differing = {key for key in east.keys() | west.keys() if east.get(key) != west.get(key)}

    assert differing == {"coverage", "path", "filename_pattern"}
    assert east["coverage"]["east"] < west["coverage"]["west"]  # the two boxes do not overlap


def test_relative_archive_root_resolves_under_the_environment_variable(tmp_path, monkeypatch):
    monkeypatch.setenv("COLLEKT_ARCHIVE_ROOT", str(tmp_path / "archives"))

    assert resolve_archive_root("aisdk") == tmp_path / "archives" / "aisdk"
    assert parse_archive_spec(get_config().sources["aisdk_ais"]).archive_root == tmp_path / "archives" / "aisdk"


def test_relative_archive_root_defaults_to_the_user_cache(monkeypatch):
    monkeypatch.delenv("COLLEKT_ARCHIVE_ROOT", raising=False)

    assert resolve_archive_root("aisdk") == Path("~/.cache/collekt/archive/aisdk").expanduser()


def test_absolute_archive_root_ignores_the_environment_variable(tmp_path, monkeypatch):
    monkeypatch.setenv("COLLEKT_ARCHIVE_ROOT", str(tmp_path / "archives"))

    assert resolve_archive_root(tmp_path / "mine") == tmp_path / "mine"


def test_archive_root_can_be_set_in_a_dotenv_file(tmp_path, monkeypatch):
    from dotenv import find_dotenv, load_dotenv

    # setenv first so monkeypatch restores the variable that load_dotenv writes into os.environ.
    monkeypatch.setenv("COLLEKT_ARCHIVE_ROOT", "unset")
    monkeypatch.delenv("COLLEKT_ARCHIVE_ROOT")
    (tmp_path / ".env").write_text(f"COLLEKT_ARCHIVE_ROOT={tmp_path / 'from-dotenv'}\n")
    monkeypatch.chdir(tmp_path)

    # The same call collekt makes on import.
    load_dotenv(find_dotenv(usecwd=True))

    assert resolve_archive_root("aisdk") == tmp_path / "from-dotenv" / "aisdk"


# Marine Cadastre "Broadcast Points": one zstd-compressed CSV per day, listed on yearly index pages.
MC_URL = BASE_URL + "/csv{date:%Y}/ais-{date:%Y-%m-%d}.csv.zst"
MC_INDEX = BASE_URL + "/csv{date:%Y}/index.html"


def _mc_csv(day: date) -> str:
    rows = [
        f"1,{day:%Y-%m-%d} 00:00:01,10.0,56.0,0.0,199.7,263,ONE,,WDE6775,,0,35,10,,91,A",
        f"2,{day:%Y-%m-%d} 23:59:59,11.0,56.5,6.3,161.4,,TWO,IMO9010149,HP6387,1020,0,92,20,6.91,99,B",
        f"3,{day:%Y-%m-%d} 12:00:00,30.0,70.0,0.1,,,THREE,,AENA,33,,,,,33,A",  # outside the request bbox
    ]
    return "\n".join([MARINE_CADASTRE_HEADER, *rows]) + "\n"


def _zstd(text: str) -> bytes:
    pa = pytest.importorskip("pyarrow")
    sink = pa.BufferOutputStream()
    with pa.CompressedOutputStream(sink, "zstd") as out:
        out.write(text.encode())
    return sink.getvalue().to_pybytes()


def _index(names) -> bytes:
    links = "".join(f'<tr><td><a href="{name}">{name}</a></td></tr>' for name in names)
    return (
        f'<html><body><table><tr><td><a href="../">Parent Directory</a></td></tr>{links}</table></body></html>'.encode()
    )


def _mc_source(tmp_path, **override):
    period = {"every": "day", "start": "2015-01-01", "url": MC_URL, "index": MC_INDEX}
    return _source(
        tmp_path,
        spec="marine_cadastre.spec.yaml",
        periods=[period],
        time_column="base_date_time",
        time_format="%Y-%m-%d %H:%M:%S",
        region_columns=["latitude", "longitude"],
        **override,
    )


def _mc_config(tmp_path, **override):
    return get_config(
        overrides={
            "source_catalogs": [],
            "output": {"root": str(tmp_path / "out")},
            "sources": {"ais": _mc_source(tmp_path, **override)},
        }
    )


def _serve_mc(files, days, *, listed=None):
    """Serve each day's compressed CSV, and per year an index listing `listed` (default: `days`)."""
    for day in days:
        files[format_url(MC_URL, day)] = _zstd(_mc_csv(day))
    by_year: dict[int, list[str]] = {}
    for day in days if listed is None else listed:
        by_year.setdefault(day.year, []).append(f"ais-{day:%Y-%m-%d}.csv.zst")
    for year, names in by_year.items():
        files[format_url(MC_INDEX, date(year, 1, 1))] = _index(names)


def format_url(template: str, day: date) -> str:
    return template.format(date=day)


def test_single_compressed_csv_is_converted_without_unzipping(tmp_path, served):
    files, _ = served
    _serve_mc(files, [date(2024, 3, 1)])

    result = Fetcher(_request(), config=_mc_config(tmp_path)).download()

    assert result.summary.downloaded == 1
    assert (tmp_path / "mirror" / ".mirrored" / "parquet" / "ais-2024-03-01.json").is_file()
    assert not list((tmp_path / "mirror" / ".downloads").iterdir())
    output = _rows(result.files[0])
    assert sorted(output.dataframe.collected()["mmsi"].to_list()) == [1, 2]
    assert output.metadata["latitude"].description.startswith("Latitude coordinate")


def test_spec_only_rejects_a_single_compressed_csv(tmp_path, served):
    files, _ = served
    _serve_mc(files, [date(2024, 3, 1)])

    with pytest.raises(ValueError, match="spec_only' needs zipped CSVs"):
        Fetcher(_request(), config=_mc_config(tmp_path, convert="spec_only")).download()


def test_days_after_the_latest_indexed_file_are_skipped_without_downloading(tmp_path, served):
    files, calls = served
    _serve_mc(files, [date(2024, 3, 1), date(2024, 3, 2)])

    result = Fetcher(_request(start="2024-03-02", end="2024-03-04"), config=_mc_config(tmp_path)).download()

    by_day = {item.day: item for item in result.results}
    assert by_day["2024-03-02"].status == "downloaded"
    for day in ("2024-03-03", "2024-03-04"):
        assert by_day[day].status == "skipped"
        assert "outside the available date range (-inf to 2024-03-02)" in by_day[day].message
    assert format_url(MC_URL, date(2024, 3, 3)) not in calls


def test_dry_run_also_ends_at_the_latest_indexed_file(tmp_path, served):
    files, _ = served
    _serve_mc(files, [date(2024, 3, 1)])

    result = Fetcher(_request(start="2024-03-01", end="2024-03-02"), config=_mc_config(tmp_path)).plan()

    assert result.summary.planned == 1
    assert result.summary.skipped == 1
    assert not (tmp_path / "mirror").exists()


def test_latest_published_day_falls_back_to_an_earlier_year(tmp_path, served):
    files, calls = served
    _serve_mc(files, [], listed=[date(2024, 12, 30), date(2024, 12, 31)])  # no 2025 index yet
    spec = parse_archive_spec(_mc_config(tmp_path).sources["ais"])

    assert latest_published_day(spec, date(2025, 2, 1)) == date(2024, 12, 31)
    assert calls == [format_url(MC_INDEX, date(2025, 1, 1)), format_url(MC_INDEX, date(2024, 1, 1))]


def test_unreadable_index_falls_back_to_the_static_coverage(tmp_path, served):
    import requests

    files, _ = served
    _serve_mc(files, [date(2024, 3, 1)])
    files[format_url(MC_INDEX, date(2024, 1, 1))] = requests.ConnectionError("offline")

    result = Fetcher(_request(), config=_mc_config(tmp_path)).download()

    assert result.summary.downloaded == 1


def test_bundled_marine_cadastre_catalog_reads_the_broadcast_points_files():
    spec = parse_archive_spec(get_config().sources["marine_cadastre_ais"])

    remote = remote_file_for_day(spec, date(2025, 7, 4))

    assert remote.url == "https://noaaocm.blob.core.windows.net/ais/csv2/csv2025/ais-2025-07-04.csv.zst"
    assert spec.periods[0].index == "https://noaaocm.blob.core.windows.net/ais/csv2/csv{date:%Y}/index.html"
