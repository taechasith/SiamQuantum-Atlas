from __future__ import annotations

import json
from pathlib import Path

import yaml

from siamquantum.services import claude
from siamquantum.services import seeds


def test_fetch_seeds_direct_only_skips_http_and_keeps_direct_text(tmp_path: Path, monkeypatch) -> None:
    seed_file = tmp_path / "seed_urls.yaml"
    seed_file.write_text(
        yaml.safe_dump(
            {
                "seeds": [
                    {
                        "url": "https://example.com/direct",
                        "title_hint": "Direct quantum seed",
                        "published_year": 2024,
                        "rationale": "Direct rationale",
                        "direct": True,
                    },
                    {
                        "url": "https://example.com/http",
                        "title_hint": "HTTP quantum seed",
                        "published_year": 2024,
                    },
                ]
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(seeds, "_SEED_FILE", seed_file)

    result = seeds.fetch_seeds(direct_only=True)

    assert result.ok is True
    data = result.data or []
    assert len(data) == 1
    assert data[0]["url"] == "https://example.com/direct"
    assert "Direct quantum seed" in (data[0]["raw_text"] or "")


def test_analyze_url_prompt_is_grounded_and_normalizes_model_output(monkeypatch) -> None:
    seen: dict[str, str] = {}

    def fake_call(system: str, user: str) -> str:
        seen["system"] = system
        seen["user"] = user
        return json.dumps(
            {
                "title": " Thai quantum source ",
                "description": "A source about Thai quantum education.",
                "primary_category": "Invented Category",
                "content_type": "made_up",
                "tags": ["Quantum", "Quantum", "Thai education"],
                "estimated_reach": "planetary",
                "quantum_domain": "magic_quantum",
                "thai_relevance": True,
                "evidence_quotes": ["Thai quantum education"],
                "analysis_notes": "Grounded in a short excerpt.",
                "data_quality": "high",
                "confidence": 1.7,
                "needs_review": False,
            }
        )

    monkeypatch.setattr(claude, "_call", fake_call)

    result = claude.analyze_url(
        "https://example.com/thai-quantum",
        "Title: Thai quantum source\n\nThai quantum education source text.",
    )

    assert "Do not invent authors" in seen["system"]
    assert "Never create a new category name" in seen["system"]
    assert "evidence_quotes" in seen["system"]
    assert "Thai quantum education source text" in seen["user"]
    assert result["title"] == "Thai quantum source"
    assert result["primary_category"] == "News Article"
    assert result["content_type"] == "news"
    assert result["estimated_reach"] == "low"
    assert result["quantum_domain"] == "not_applicable"
    assert result["tags"] == ["Quantum", "Thai education"]
    assert result["confidence"] == 1.0
    assert result["data_quality"] == "high"
    assert result["needs_review"] is False
