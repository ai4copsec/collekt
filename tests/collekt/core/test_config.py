"""Tests for the YAML configuration mechanism.

collekt ships a curated source catalog plus the loading mechanism; presets and
variable-group tagging are left to downstream. A downstream `conf_dir` can select
the bundled catalogs by name and extend or override individual sources.
"""

import textwrap
from pathlib import Path

import pytest

import collekt
from collekt.core.config import Config, default_conf_dir, get_config, load_config


def test_default_conf_dir_is_bundled_inside_the_package():
    conf_dir = default_conf_dir()
    assert conf_dir.is_dir()
    assert conf_dir.name == "conf"
    assert conf_dir.parent == Path(collekt.__file__).resolve().parent
    assert (conf_dir / "default.yaml").is_file()


def test_default_config_loads_the_bundled_catalog():
    cfg = get_config()
    assert isinstance(cfg, Config)
    assert "cmems_global" in cfg.source_catalogs
    glorys = cfg.sources["cmems_glorys_my"]
    # The catalog advertises capabilities but ships no selection: `available_variables`
    # is the allow-list, `variables` (the selection) is empty until a downstream picks.
    assert {"uo", "vo", "thetao"} <= set(glorys.available_variables)
    assert glorys.variables == ()
    assert glorys.has_depth is True
    assert glorys.url and glorys.doi
    assert cfg.sources["cmems_glorys_nrt"].available_variables == ("uo", "vo")
    assert cfg.sources["cmems_glorys_nrt"].raw["coverage"]["temporal"]["kind"] == "rolling"
    assert cfg.sources["cmems_glorys_my"].raw["coverage"]["temporal"]["kind"] == "archive"
    assert cfg.sources["cmems_glorys_my"].raw["coverage"]["temporal"]["end"]
    assert cfg.sources["cmems_duacs_my"].has_depth is False
    assert cfg.sources["cmems_med_currents_my"].kind == "cmems"
    assert cfg.sources["cmems_med_currents_nrt_15min"].temporal_sampling == "15min"
    assert cfg.sources["cmems_med_currents_nrt_2d_hourly"].has_depth is False
    assert cfg.sources["cmems_med_currents_nrt_3d_hourly"].has_depth is True
    assert cfg.sources["cmems_ibi_currents"].available_variables == (
        "bottomT",
        "mlotst",
        "so",
        "thetao",
        "uo",
        "vo",
        "zos",
    )
    assert cfg.sources["cmems_ibi_waves"].temporal_sampling == "1h"
    assert cfg.sources["cmems_ibi_waves"].raw["coverage"]["longitude"] == [-19.0, 5.000736]
    assert cfg.sources["cmems_atl_chl_obs_my"].doi == "10.48670/moi-00286"
    assert cfg.sources["cmems_ibi_bgc_model"].has_depth is True
    assert cfg.sources["cmems_nws_currents"].available_variables == ("ubar", "uo", "vbar", "vo", "wo")
    assert cfg.sources["cmems_nws_waves"].temporal_sampling == "1h"
    assert cfg.sources["cmems_nws_chl_obs"].available_variables == ("CHL", "SPM", "TUR")
    assert cfg.sources["cmems_nws_bgc_model"].has_depth is True
    assert cfg.sources["eodyn_osmose_currents"].kind == "eodyn"
    assert "skytruth" in cfg.sources
    assert cfg.cache.reuse_existing is True
    assert cfg.output.manifest_name == "manifest.json"


def test_available_variables_gate_the_selection(tmp_path):
    conf = tmp_path / "conf"
    conf.mkdir()
    (conf / "default.yaml").write_text(
        textwrap.dedent(
            """
            sources:
              grid:
                kind: cmems
                available_variables: [uo, vo, thetao]
            """
        ),
        encoding="utf-8",
    )
    # No selection resolves to nothing (a bundled source ships no default selection).
    assert get_config(conf_dir=conf).sources["grid"].variables == ()
    # A subset of available_variables resolves; an unknown name is rejected.
    picked = get_config(conf_dir=conf, overrides={"sources": {"grid": {"use_variables": ["uo", "vo"]}}})
    assert picked.sources["grid"].variables == ("uo", "vo")
    with pytest.raises(ValueError, match="unknown variable or group"):
        get_config(conf_dir=conf, overrides={"sources": {"grid": {"use_variables": ["nope"]}}})


def test_conf_dir_selects_bundled_catalogs_by_name(tmp_path):
    conf = tmp_path / "conf"
    conf.mkdir()
    (conf / "default.yaml").write_text("source_catalogs: [cmems_global]\n", encoding="utf-8")

    cfg = get_config(conf_dir=conf)

    assert "cmems_glorys_my" in cfg.sources  # resolved from the bundled catalog
    assert "cmems_med_currents_my" not in cfg.sources  # not requested


def test_conf_dir_extends_and_overrides_bundled_sources(tmp_path):
    conf = tmp_path / "conf"
    conf.mkdir()
    (conf / "default.yaml").write_text(
        textwrap.dedent(
            """
            source_catalogs: [cmems_global]
            sources:
              cmems_glorys_my:
                enabled: false
              my_source:
                kind: cmems
                variables: [x]
            """
        ),
        encoding="utf-8",
    )

    cfg = get_config(conf_dir=conf)

    assert cfg.sources["cmems_glorys_my"].enabled is False  # overridden
    assert cfg.sources["my_source"].variables == ("x",)  # added


def test_unknown_preset_raises():
    with pytest.raises(FileNotFoundError, match="Unknown preset"):
        load_config(preset="missing")


def test_unknown_source_catalog_raises(tmp_path):
    conf = tmp_path / "conf"
    conf.mkdir()
    (conf / "default.yaml").write_text("source_catalogs: [missing]\n", encoding="utf-8")

    with pytest.raises(FileNotFoundError, match="Unknown source catalog"):
        load_config(conf_dir=conf)


def test_custom_conf_dir_and_env_expansion(tmp_path, monkeypatch):
    monkeypatch.setenv("COLLEKT_TEST_ROOT", str(tmp_path / "out"))
    conf = tmp_path / "conf"
    conf.mkdir()
    (conf / "default.yaml").write_text(
        textwrap.dedent(
            """
            output:
              root: "$COLLEKT_TEST_ROOT"
            sources:
              local:
                kind: cmems
                variables:
                  default: [u, v]
                  optional:
                    speed: [speed]
                use_variables: [default, speed]
            """
        ),
        encoding="utf-8",
    )

    cfg = get_config(conf_dir=conf)

    assert cfg.output.root == tmp_path / "out"
    assert cfg.sources["local"].variables == ("u", "v", "speed")
    assert cfg.sources["local"].temporal_sampling == "24h"


def test_flat_variable_list_is_still_supported(tmp_path):
    conf = tmp_path / "conf"
    conf.mkdir()
    (conf / "default.yaml").write_text(
        textwrap.dedent(
            """
            sources:
              local:
                kind: cmems
                variables: [u, v]
            """
        ),
        encoding="utf-8",
    )

    cfg = get_config(conf_dir=conf)

    assert cfg.sources["local"].default_variables == ("u", "v")
    assert cfg.sources["local"].variables == ("u", "v")


def test_temporal_config_is_normalized(tmp_path):
    conf = tmp_path / "conf"
    conf.mkdir()
    (conf / "default.yaml").write_text(
        textwrap.dedent(
            """
            sources:
              local:
                kind: era5
                variables: [u, v]
                temporal_sampling: 1 hour
            """
        ),
        encoding="utf-8",
    )

    cfg = get_config(conf_dir=conf)

    assert cfg.sources["local"].temporal_sampling == "1h"


def test_use_variables_expands_optional_groups_and_deduplicates(tmp_path):
    conf = tmp_path / "conf"
    conf.mkdir()
    (conf / "default.yaml").write_text(
        textwrap.dedent(
            """
            sources:
              waves:
                kind: cmems
                variables:
                  default: [VHM0, VTPK]
                  optional:
                    stokes: [VSDX, VSDY]
            """
        ),
        encoding="utf-8",
    )

    cfg = get_config(
        conf_dir=conf,
        overrides={"sources": {"waves": {"use_variables": ["default", "stokes", "VHM0"]}}},
    )
    assert cfg.sources["waves"].variables == ("VHM0", "VTPK", "VSDX", "VSDY")


def test_unknown_requested_variable_or_group_raises(tmp_path):
    conf = tmp_path / "conf"
    conf.mkdir()
    (conf / "default.yaml").write_text(
        textwrap.dedent(
            """
            sources:
              waves:
                kind: cmems
                variables:
                  default: [VHM0]
            """
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="unknown variable or group"):
        get_config(conf_dir=conf, overrides={"sources": {"waves": {"use_variables": ["default", "imaginary"]}}})
