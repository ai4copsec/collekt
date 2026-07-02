"""Tests for the doctor diagnostics."""

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


def test_run_doctor_online_flags_missing_variables(monkeypatch):
    cfg = parse_config(
        {"sources": {"s": {"kind": "cmems", "dataset_id": "glo", "variables": {"default": ["uo", "missing"]}}}}
    )

    def describe(**kwargs):
        variable = SimpleNamespace(short_name="uo")
        dataset = SimpleNamespace(
            dataset_id="glo",
            versions=[SimpleNamespace(parts=[SimpleNamespace(services=[SimpleNamespace(variables=[variable])])])],
        )
        return SimpleNamespace(products=[SimpleNamespace(datasets=[dataset])])

    monkeypatch.setattr("collekt.core.doctor._copernicusmarine_describe", lambda: describe)
    checks = run_doctor(config=cfg, online=True)

    cmems = next(check for check in checks if check.name == "cmems catalogue s")
    assert cmems.status == "fail"
    assert "missing" in cmems.message
