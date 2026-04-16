from __future__ import annotations

from siamquantum_atlas.nlp.entities import extract_entities


def infer_frames(text: str) -> dict[str, object]:
    lowered = text.lower()
    frame_labels: list[str] = []
    gratifications: list[str] = []
    parasocial_signal = None
    if any(term in lowered for term in ["อธิบาย", "explainer", "นักวิจัย", "หลักสูตร"]):
        frame_labels.append("educational_explainer")
        gratifications.append("information_seeking")
    if any(term in lowered for term in ["อนาคต", "future", "แข่งขัน", "ลงทุน"]):
        frame_labels.append("national_competitiveness")
        gratifications.append("inspiration_future_imagination")
    if any(term in lowered for term in ["ฮีล", "healing", "จิตวิญญาณ", "spiritual"]):
        frame_labels.append("spirituality_healing")
        gratifications.append("identity_status")
    if any(term in lowered for term in ["ไซไฟ", "บันเทิง", "series", "film"]):
        frame_labels.append("entertainment_scifi")
        gratifications.append("entertainment")
    if any(term in lowered for term in ["เตือน", "ตั้งคำถาม", "debunk", "skeptic"]):
        frame_labels.append("skepticism_debunking")
        gratifications.append("curiosity_novelty")
    if not frame_labels:
        frame_labels.append("rigorous_science")
        gratifications.append("information_seeking")
    if any(term in lowered for term in ["ผู้ดำเนินรายการ", "host", "creator", "influencer"]):
        parasocial_signal = "creator"
    return {
        "frame_labels": frame_labels,
        "uses_and_gratifications": sorted(set(gratifications)),
        "parasocial_signal": parasocial_signal,
        "entities": extract_entities(text),
    }
