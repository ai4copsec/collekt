"""Tests for the read-only catalog view (`available_datasets`, `describe_datasets`)."""

import textwrap
from datetime import date

import collekt
from collekt.core.catalog import available_datasets, describe_datasets


def test_available_datasets_includes_bundled_entries_with_uniform_schema():
    catalog = available_datasets()

    assert "cmems_glorys_my" in catalog
    meta = catalog["cmems_glorys_my"]
    assert meta.keys() == {
        "provider",
        "path",
        "dataset_id",
        "variables",
        "has_depth",
        "temporal_sampling",
        "coverage",
        "url",
        "doi",
    }
    assert meta["provider"] == "cmems"
    assert meta["dataset_id"] == "cmems_mod_glo_phy_my_0.083deg_P1D-m"
    assert {"uo", "vo", "thetao"} <= set(meta["variables"])
    assert meta["has_depth"] is True
    assert meta["coverage"]["start"] == date(1993, 1, 1)


def test_available_datasets_defaults_to_whole_globe_when_coverage_is_unset():
    catalog = available_datasets()

    meta = catalog["skytruth"]
    coverage = meta["coverage"]
    assert coverage["west"] == -180.0
    assert coverage["east"] == 180.0
    assert coverage["south"] == -90.0
    assert coverage["north"] == 90.0
    assert coverage["start"] is None
    assert coverage["end"] is None


def test_available_datasets_is_exposed_on_the_public_api():
    assert collekt.available_datasets is available_datasets


def test_available_datasets_conf_dir_overlay_adds_downstream_entries(tmp_path):
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

    catalog = available_datasets(conf_dir=conf)

    assert catalog["my_currents"]["dataset_id"] == "MY-OCEAN-CURRENTS"
    assert "cmems_glorys_my" in catalog  # bundled catalog still present


def test_describe_datasets_groups_by_provider_and_lists_keys():
    text = describe_datasets()

    assert "cmems" in text
    assert "cmems_glorys_my" in text
    assert "skytruth" in text


def test_describe_datasets_is_exposed_on_the_public_api():
    assert collekt.describe_datasets is describe_datasets
