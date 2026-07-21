"""Tests for the doctor diagnostics."""

from datetime import UTC, date, datetime
from types import SimpleNamespace

from collekt.core.config import parse_config
from collekt.core.doctor import DoctorCheck, run_doctor


def test_run_doctor_reports_packages_and_sources():
    cfg = parse_config({"sources": {"s": {"kind": "cmems", "dataset_id": "x", "variables": ["uo"]}}})
    checks = run_doctor(config=cfg)

    by_name = {check.name: check for check in checks}
    assert all(isinstance(check, DoctorCheck) for check in checks)
    assert "copernicusmarine package" in by_name
    assert "xarray package" in by_name
    assert by_name["source s"].status == "ok"
    assert "cmems" in by_name["source s"].message


def test_run_doctor_flags_a_typo_in_a_source_raw_key():
    # 'pad_dg' instead of 'pad_deg' would otherwise be silently ignored: the
    # adapter just falls back to its default with no error or warning.
    cfg = parse_config(
        {
            "sources": {
                "s": {
                    "kind": "era5",
                    "available_variables": ["10m_u_component_of_wind"],
                    "variables": ["10m_u_component_of_wind"],
                    "pad_dg": 1.0,
                }
            }
        }
    )
    checks = run_doctor(config=cfg)

    by_name = {check.name: check for check in checks}
    assert by_name["source s config keys"].status == "warn"
    assert "pad_dg" in by_name["source s config keys"].message


def test_run_doctor_does_not_flag_known_raw_keys():
    cfg = parse_config(
        {
            "sources": {
                "s": {
                    "kind": "era5",
                    "available_variables": ["10m_u_component_of_wind"],
                    "variables": ["10m_u_component_of_wind"],
                    "pad_deg": 1.0,
                    "data_format": "netcdf",
                }
            }
        }
    )
    checks = run_doctor(config=cfg)

    assert "source s config keys" not in {check.name for check in checks}


def test_run_doctor_online_flags_stale_available_variables(monkeypatch):
    cfg = parse_config(
        {"sources": {"s": {"kind": "cmems", "dataset_id": "glo", "available_variables": ["uo", "missing"]}}}
    )

    def describe(**kwargs):
        variable = SimpleNamespace(short_name="uo")
        dataset = SimpleNamespace(
            dataset_id="glo",
            versions=[SimpleNamespace(parts=[SimpleNamespace(services=[SimpleNamespace(variables=[variable])])])],
        )
        return SimpleNamespace(products=[SimpleNamespace(datasets=[dataset])])

    monkeypatch.setattr("collekt.sources.cmems._copernicusmarine_describe", lambda: describe)
    checks = run_doctor(config=cfg, online=True)

    cmems = next(check for check in checks if check.name == "cmems catalogue s")
    assert cmems.status == "fail"
    assert "missing" in cmems.message


def test_run_doctor_online_checks_declared_coverage(monkeypatch):
    cfg = parse_config(
        {
            "sources": {
                "s": {
                    "kind": "cmems",
                    "dataset_id": "glo",
                    "available_variables": ["uo"],
                    "coverage": {
                        "longitude": [-10.0, 10.0],
                        "latitude": [40.0, 50.0],
                        "temporal": {"start": "2020-01-01", "end": None, "kind": "rolling"},
                    },
                }
            }
        }
    )

    def _ms(day: date):
        return (
            datetime(day.year, day.month, day.day, tzinfo=UTC) - datetime(1970, 1, 1, tzinfo=UTC)
        ).total_seconds() * 1000

    def describe(**_kwargs):
        variable = SimpleNamespace(
            short_name="uo",
            bbox=[-10.0, 40.0, 10.0, 50.0],
            coordinates=[
                SimpleNamespace(
                    coordinate_id="time",
                    coordinate_unit="milliseconds since 1970-01-01 00:00:00",
                    minimum_value=_ms(date(2020, 1, 1)),
                    maximum_value=_ms(date(2026, 7, 1)),
                )
            ],
        )
        dataset = SimpleNamespace(
            dataset_id="glo",
            versions=[SimpleNamespace(parts=[SimpleNamespace(services=[SimpleNamespace(variables=[variable])])])],
        )
        return SimpleNamespace(products=[SimpleNamespace(datasets=[dataset])])

    monkeypatch.setattr("collekt.sources.cmems._copernicusmarine_describe", lambda: describe)
    checks = run_doctor(config=cfg, online=True)

    coverage = next(check for check in checks if check.name == "cmems coverage s")
    assert coverage.status == "ok"
