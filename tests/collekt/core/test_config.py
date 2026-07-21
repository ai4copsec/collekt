"""Tests for the YAML configuration mechanism.

collekt ships curated source catalogs as capability metadata. Public presets are
`DatasetConfig` YAML files that select source keys and provider parameters.
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


def test_dataset_config_resolves_selected_catalog_entries(tmp_path):
    config = collekt.DatasetConfig(
        collekt.CMEMS("cmems_glorys_my", variables=["uo", "vo"], depth=[1.0, 1.1]),
        collekt.CMEMS("cmems_duacs_my", variables=["ugos", "vgos"]),
    )

    resolved = config.resolve()

    assert tuple(resolved.sources) == ("cmems_glorys_my", "cmems_duacs_my")
    assert resolved.sources["cmems_glorys_my"].variables == ("uo", "vo")
    assert resolved.sources["cmems_glorys_my"].raw["depth"] == [1.0, 1.1]
    assert resolved.sources["cmems_duacs_my"].variables == ("ugos", "vgos")

    preset = tmp_path / "datasets.yaml"
    preset.write_text(
        textwrap.dedent(
            """
            datasets:
              - provider: cmems
                key: cmems_duacs_my
                variables: [ugos, vgos]
            """
        ),
        encoding="utf-8",
    )

    loaded = collekt.DatasetConfig.from_yaml(preset)
    assert loaded.as_dict() == {
        "datasets": [{"provider": "cmems", "key": "cmems_duacs_my", "variables": ["ugos", "vgos"]}]
    }


def test_eodyn_dataset_selection_uses_brand_casing():
    selection = collekt.eOdyn("eodyn_osmose_currents")

    assert selection.as_dict() == {"provider": "eodyn", "key": "eodyn_osmose_currents"}
    assert "eOdyn" in collekt.__all__
    assert "Eodyn" not in collekt.__all__


def test_dataset_config_resolves_a_downstream_dataset_via_conf_dir(tmp_path):
    # A conf_dir lets DatasetConfig select a dataset collekt does not ship.
    conf = tmp_path / "conf"
    (conf / "source").mkdir(parents=True)
    (conf / "default.yaml").write_text("source_catalogs: [ocean]\n", encoding="utf-8")
    (conf / "source" / "ocean.yaml").write_text(
        textwrap.dedent(
            """
            sources:
              my_currents:
                kind: cmems
                dataset_id: MY-OCEAN-CURRENTS
                available_variables: [uo, vo]
            """
        ),
        encoding="utf-8",
    )

    resolved = collekt.DatasetConfig(collekt.CMEMS("my_currents", variables=["uo"])).resolve(conf_dir=conf)

    assert resolved.sources["my_currents"].dataset_id == "MY-OCEAN-CURRENTS"
    assert resolved.sources["my_currents"].variables == ("uo",)
    # Without the conf_dir the key is unknown.
    with pytest.raises(ValueError, match="unknown dataset key"):
        collekt.DatasetConfig(collekt.CMEMS("my_currents", variables=["uo"])).resolve()


def test_dataset_config_validates_variables_and_depth():
    with pytest.raises(ValueError, match="unknown variable"):
        collekt.DatasetConfig(collekt.CMEMS("cmems_duacs_my", variables=["imaginary"])).resolve()

    with pytest.raises(ValueError, match="has a depth dimension"):
        collekt.DatasetConfig(collekt.CMEMS("cmems_glorys_my", variables=["uo"])).resolve()


def test_dataset_config_resolves_a_feature_source():
    # HOZINT is a first-class feature source: no variables, selected by key.
    resolved = collekt.DatasetConfig(collekt.Hozint()).resolve()
    assert tuple(resolved.sources) == ("hozint",)
    assert resolved.sources["hozint"].kind == "hozint"
    assert resolved.sources["hozint"].variables == ()


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
    picked = get_config(conf_dir=conf, overrides={"sources": {"grid": {"variables": ["uo", "vo"]}}})
    assert picked.sources["grid"].variables == ("uo", "vo")
    with pytest.raises(ValueError, match="unknown variables"):
        get_config(conf_dir=conf, overrides={"sources": {"grid": {"variables": ["nope"]}}})


def test_conf_dir_extends_the_bundled_catalog(tmp_path):
    # An overlay conf_dir adds its own catalog; the bundled datasets stay available.
    conf = tmp_path / "conf"
    (conf / "source").mkdir(parents=True)
    (conf / "default.yaml").write_text("source_catalogs: [ocean]\n", encoding="utf-8")
    (conf / "source" / "ocean.yaml").write_text(
        textwrap.dedent(
            """
            sources:
              my_currents:
                kind: cmems
                dataset_id: MY-OCEAN-CURRENTS
                available_variables: [uo, vo]
            """
        ),
        encoding="utf-8",
    )

    cfg = get_config(conf_dir=conf)

    assert cfg.sources["my_currents"].dataset_id == "MY-OCEAN-CURRENTS"  # added downstream
    assert "cmems_glorys_my" in cfg.sources  # bundled global catalog still present
    assert "cmems_med_currents_my" in cfg.sources  # ...and the other bundled catalogs too


def test_conf_dir_overrides_a_bundled_source(tmp_path):
    # Overriding a bundled dataset needs no re-listing of the shipped catalogs.
    conf = tmp_path / "conf"
    conf.mkdir()
    (conf / "default.yaml").write_text(
        textwrap.dedent(
            """
            sources:
              cmems_glorys_my:
                enabled: false
              my_source:
                kind: cmems
                available_variables: [x]
            """
        ),
        encoding="utf-8",
    )

    cfg = get_config(conf_dir=conf)

    assert cfg.sources["cmems_glorys_my"].enabled is False  # bundled source overridden
    assert cfg.sources["my_source"].available_variables == ("x",)  # added
    assert "cmems_duacs_my" in cfg.sources  # untouched bundled datasets remain


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
                variables: [u, v, speed]
            """
        ),
        encoding="utf-8",
    )

    cfg = get_config(conf_dir=conf)

    assert cfg.output.root == tmp_path / "out"
    assert cfg.sources["local"].variables == ("u", "v", "speed")
    assert cfg.sources["local"].temporal_sampling == "24h"


def test_flat_variable_list_is_selected_variables(tmp_path):
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


def test_selected_variables_are_validated_against_available_variables(tmp_path):
    conf = tmp_path / "conf"
    conf.mkdir()
    (conf / "default.yaml").write_text(
        textwrap.dedent(
            """
            sources:
              waves:
                kind: cmems
                available_variables: [VHM0, VTPK, VSDX, VSDY]
            """
        ),
        encoding="utf-8",
    )

    cfg = get_config(
        conf_dir=conf,
        overrides={"sources": {"waves": {"variables": ["VHM0", "VSDX"]}}},
    )
    assert cfg.sources["waves"].variables == ("VHM0", "VSDX")


def test_unknown_selected_variable_raises(tmp_path):
    conf = tmp_path / "conf"
    conf.mkdir()
    (conf / "default.yaml").write_text(
        textwrap.dedent(
            """
            sources:
              waves:
                kind: cmems
                available_variables: [VHM0]
            """
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="unknown variables"):
        get_config(conf_dir=conf, overrides={"sources": {"waves": {"variables": ["VHM0", "imaginary"]}}})
