from __future__ import annotations

import logging
from typing import Any, TypeVar

from pydantic import BaseModel

from siamquantum_atlas.settings import settings

logger = logging.getLogger(__name__)
T = TypeVar("T", bound=BaseModel)

try:
    from anthropic import Anthropic
except Exception:  # pragma: no cover
    Anthropic = None  # type: ignore[assignment]


class AnthropicStructuredClient:
    def __init__(self) -> None:
        self.api_key = settings.anthropic_api_key
        self.client = Anthropic(api_key=self.api_key) if self.api_key and Anthropic else None
        self.cache_dir = settings.processed_dir / "llm_cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def generate_structured(self, *, prompt_name: str, cache_key: str, schema: type[T], fallback_payload: dict[str, Any]) -> T:
        cache_path = self.cache_dir / f"{cache_key}.json"
        if cache_path.exists():
            return schema.model_validate_json(cache_path.read_text(encoding="utf-8"))
        if self.client is not None:
            logger.info("Anthropic key detected for %s; using validated fallback until live tool-schema call is configured.", prompt_name)
        model = schema.model_validate(fallback_payload)
        cache_path.write_text(model.model_dump_json(indent=2), encoding="utf-8")
        return model
