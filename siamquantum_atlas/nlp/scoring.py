from __future__ import annotations


def clamp(value: float) -> float:
    return max(0.0, min(1.0, value))


def compute_scores(text: str, frames: dict[str, object]) -> tuple[dict[str, float], float, float]:
    lowered = text.lower()
    subscores = {
        "accessibility": 0.8 if any(term in lowered for term in ["ง่าย", "เบื้องต้น", "คนทั่วไป"]) else 0.5,
        "explanatory_clarity": 0.8 if any(term in lowered for term in ["อธิบาย", "explain", "ชัดเจน"]) else 0.5,
        "scientific_integrity": 0.85 if "skepticism_debunking" in frames["frame_labels"] or "rigorous_science" in frames["frame_labels"] else 0.55,
        "real_world_relevance": 0.8 if any(term in lowered for term in ["ไทย", "การแพทย์", "โลจิสติกส์", "การเงิน"]) else 0.4,
        "hype_pressure": 0.7 if any(term in lowered for term in ["อนาคต", "แข่งขัน", "ลงทุน"]) else 0.25,
        "spirituality_claim_strength": 0.85 if "spirituality_healing" in frames["frame_labels"] else 0.1,
        "uncertainty_confusion": 0.7 if any(term in lowered for term in ["คลุมเครือ", "งง"]) else 0.2,
    }
    normalization_score = clamp(
        0.2 * subscores["accessibility"]
        + 0.2 * subscores["explanatory_clarity"]
        + 0.25 * subscores["scientific_integrity"]
        + 0.15 * subscores["real_world_relevance"]
        - 0.1 * subscores["hype_pressure"]
        - 0.05 * subscores["spirituality_claim_strength"]
        - 0.05 * subscores["uncertainty_confusion"]
        + 0.2
    )
    distortion_risk = clamp(
        0.35 * subscores["hype_pressure"]
        + 0.35 * subscores["spirituality_claim_strength"]
        + 0.2 * subscores["uncertainty_confusion"]
        - 0.1 * subscores["scientific_integrity"]
        + 0.1
    )
    return subscores, normalization_score, distortion_risk
