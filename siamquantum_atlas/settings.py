from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="SIAMQUANTUM_", extra="ignore")

    env: str = "development"
    database_url: str = "sqlite:///data/processed/siamquantum_atlas.db"
    anthropic_api_key: str | None = None
    claude_model: str = "claude-3-5-sonnet-latest"
    youtube_api_key: str | None = None
    gdelt_base_url: str = "https://api.gdeltproject.org/api/v2/doc/doc"
    viewer_port: int = 8765
    project_root: Path = Field(default_factory=lambda: Path(__file__).resolve().parent.parent)

    @property
    def data_dir(self) -> Path:
        return self.project_root / "data"

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def processed_dir(self) -> Path:
        return self.data_dir / "processed"

    @property
    def exports_dir(self) -> Path:
        return self.data_dir / "exports"

    @property
    def samples_dir(self) -> Path:
        return self.data_dir / "samples"

    @property
    def viewer_dir(self) -> Path:
        return self.project_root / "viewer"


settings = Settings()
