"""Tests for collekt.reference.geonorge_maritime — pure-function only.
"""
import pytest

from collekt.reference.geonorge_maritime import _pick, _safe_name


def test_safe_name_norwegian_chars():
    assert _safe_name("NorgesØkonomiskeSone") == "norgesoekonomiskesone"
    assert _safe_name("TilstøtendeSone") == "tilstoetendesone"
    assert _safe_name("Hele landet") == "hele_landet"
    assert _safe_name("Fiskerisone") == "fiskerisone"


def test_pick_matches_case_insensitively():
    items = [{"name": "GML 3.2.1"}, {"name": "SOSI 4.0"}, {"name": "PostGIS"}]
    assert _pick(items, "gml")["name"] == "GML 3.2.1"
    assert _pick(items, "SOSI")["name"] == "SOSI 4.0"


def test_pick_raises_with_available_list_on_miss():
    items = [{"name": "GML"}, {"name": "SOSI"}]
    with pytest.raises(KeyError, match=r"Available"):
        _pick(items, "nonexistent")