from __future__ import annotations

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SIAMQUANTUM_",
        env_file=(".env", ".env.local"),
        extra="ignore",
    )

    env: str = "development"
    database_url: str = "sqlite:///data/processed/siamquantum_atlas.db"
    anthropic_api_key: str = Field(default="")
    claude_model: str = "claude-sonnet-4-6"
    youtube_api_key: str = Field(default="")
    gdelt_base_url: str = "https://api.gdeltproject.org/api/v2/doc/doc"
    viewer_port: int = 8765
    google_cse_key: str = Field(default="")
    google_cse_cx_academic: str = Field(default="")
    google_cse_cx_media: str = Field(default="")
    deployment_mode: str = "local"
    database_read_only: bool = False
    relevance_recheck_days: int = 30
    relevance_audit_batch_size: int = 40
    # Supabase env vars are intentionally unprefixed to match the user's deployment setup.
    supabase_url: str = Field(default="", validation_alias=AliasChoices("SUPABASE_URL"))
    supabase_publishable_key: str = Field(
        default="",
        validation_alias=AliasChoices("SUPABASE_PUBLISHABLE_KEY"),
    )
    supabase_secret_key: str = Field(
        default="",
        validation_alias=AliasChoices("SUPABASE_SECRET_KEY"),
    )


settings = Settings()
