from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SIAMQUANTUM_", env_file=".env", extra="ignore")

    env: str = "development"
    database_url: str = "sqlite:///data/processed/siamquantum_atlas.db"
    anthropic_api_key: str = Field(default="")
    claude_model: str = "claude-sonnet-4-6"
    youtube_api_key: str = Field(default="")
    gdelt_base_url: str = "https://api.gdeltproject.org/api/v2/doc/doc"
    viewer_port: int = 8765


settings = Settings()
