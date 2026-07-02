"""Tests for the YAML configuration mechanism.

collekt ships the config *mechanism* and a minimal base file; source catalogs,
presets, and the variable-group vocabulary are supplied by the consuming brick,
so these tests exercise the mechanism with temporary configuration directories.
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


def test_default_config_loads_minimal_base():
    cfg = get_config()
    assert isinstance(cfg, Config)
    assert cfg.source_catalogs == ()
    assert cfg.sources == {}
    assert cfg.cache.reuse_existing is True
    assert cfg.output.manifest_name == "manifest.json"


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
    assert cfg.sources["local"].temporal.native_sampling == "24h"


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
                temporal:
                  native_sampling: 1 hour
                  supported_sampling: [1h, 3 hours, 6, 24h]
                  sampling_mode: instantaneous
            """
        ),
        encoding="utf-8",
    )

    cfg = get_config(conf_dir=conf)

    assert cfg.sources["local"].temporal.native_sampling == "1h"
    assert cfg.sources["local"].temporal.supported_sampling == ("1h", "3h", "6h", "24h")
    assert cfg.sources["local"].temporal.sampling_mode == "instantaneous"


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
