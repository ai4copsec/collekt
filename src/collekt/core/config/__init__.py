"""Configuration loading for collekt."""

from collekt.core.config.loader import default_conf_dir, load_config
from collekt.core.config.schema import (
    CacheConfig,
    Config,
    CredentialsConfig,
    OutputConfig,
    SourceConfig,
    SourceVariableOverrides,
    TemporalConfig,
    apply_source_variable_overrides,
    get_config,
    parse_config,
)

# Legacy logging constants used by the current CLI (`collekt.cli.main`). The CLI
# moves to rich-based reporting in a later phase; kept here so imports of
# `collekt.core.config` keep working during the transition.
DATE_FORMAT = "%Y-%m-%d"
LOG_FORMAT = "[{asctime}][{levelname:^8s}] {name}: {message}"
LOG_STYLE = "{"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

__all__ = [
    "CacheConfig",
    "Config",
    "CredentialsConfig",
    "OutputConfig",
    "SourceConfig",
    "SourceVariableOverrides",
    "TemporalConfig",
    "apply_source_variable_overrides",
    "default_conf_dir",
    "get_config",
    "load_config",
    "parse_config",
    "DATE_FORMAT",
    "LOG_FORMAT",
    "LOG_STYLE",
    "LOG_DATE_FORMAT",
]
