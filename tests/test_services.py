from __future__ import annotations

from pathlib import Path

import yaml

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
