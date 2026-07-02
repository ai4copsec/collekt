"""Credential resolution for credential-gated sources.

Secrets are read from the environment (or a local ``.env`` file), never from the
YAML configuration or the manifest.
"""

from __future__ import annotations

import uuid

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Credentials(BaseSettings):
    """User/password credentials resolved from the environment.

    Values are read from ``COLLEKT_``-prefixed environment variables or a local
    ``.env`` file.
    """

    user: str
    password: str
    csrf_token: str = Field(default=str(uuid.uuid4()))

    model_config = SettingsConfigDict(
        env_file=".env",
        env_nested_delimiter="__",
        env_prefix="COLLEKT_",
        extra="ignore",
    )
