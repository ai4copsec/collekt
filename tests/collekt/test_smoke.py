"""Smoke tests: the package imports and exposes a version."""

import collekt


def test_version_is_dotted_string():
    assert isinstance(collekt.__version__, str)
    assert collekt.__version__.count(".") == 2
