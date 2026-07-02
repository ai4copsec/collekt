import uuid

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

DATE_FORMAT = "%Y-%m-%d"

LOG_FORMAT = "[{asctime}][{levelname:^8s}] {name}: {message}"
LOG_STYLE = "{"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class Credentials(BaseSettings):
    user: str
    password: str

    csrf_token: str = Field(default=str(uuid.uuid4()))

    model_config = SettingsConfigDict(env_file=".env", env_nested_delimiter="__", env_prefix="COLLEKT_", extra="ignore")
