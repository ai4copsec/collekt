import collekt.core.config


def test_config_module_imports():
    assert collekt.core.config.DATE_FORMAT == "%Y-%m-%d"
