from siamquantum_atlas.llm.extractors import ClaudeExtractionService


def test_claude_fallback_returns_valid_schema() -> None:
    result = ClaudeExtractionService().extract("คิวบิตคืออะไร", "บทความอธิบายคอมพิวเตอร์ควอนตัมสำหรับคนทั่วไป", "article")
    assert result.main_topic
    assert 0.0 <= result.frame_result.normalization_score <= 1.0
    assert result.triplets
