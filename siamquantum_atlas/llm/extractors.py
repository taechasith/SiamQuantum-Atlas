from __future__ import annotations

from siamquantum_atlas.llm.anthropic_client import AnthropicStructuredClient
from siamquantum_atlas.llm.schemas import ExtractionResult, FramingResult, TopicScore, TripletRecord
from siamquantum_atlas.nlp.frames import infer_frames
from siamquantum_atlas.nlp.scoring import compute_scores
from siamquantum_atlas.utils.hashes import stable_hash


class ClaudeExtractionService:
    def __init__(self) -> None:
        self.client = AnthropicStructuredClient()

    def extract(self, title: str, body: str, media_type: str) -> ExtractionResult:
        frames = infer_frames(f"{title} {body}")
        subscores, normalization_score, distortion_risk = compute_scores(f"{title} {body}", frames)
        topic = self._infer_topic(f"{title} {body}")
        fallback = {
            "title_th": title,
            "title_en": title,
            "summary_th": body[:280],
            "summary_en": body[:280],
            "main_topic": topic,
            "secondary_topics": [TopicScore(label=topic, weight=0.9, confidence=0.7).model_dump()],
            "media_type": media_type,
            "communicative_intent": "educate" if "อธิบาย" in body or "explain" in body.lower() else "inform",
            "audience_level": "general",
            "frame_result": FramingResult(
                frame_labels=frames["frame_labels"],
                uses_and_gratifications=frames["uses_and_gratifications"],
                parasocial_signal=frames["parasocial_signal"],
                normalization_subscores=subscores,
                normalization_score=normalization_score,
                distortion_risk=distortion_risk,
                confidence=0.65,
            ).model_dump(),
            "triplets": [TripletRecord(subject=topic, predicate="appears_in", object=media_type, confidence=0.6).model_dump()],
            "entities": frames["entities"],
        }
        return self.client.generate_structured(
            prompt_name="normalize_extract",
            cache_key=stable_hash(title + body),
            schema=ExtractionResult,
            fallback_payload=fallback,
        )

    def _infer_topic(self, text: str) -> str:
        lowered = text.lower()
        if "healing" in lowered or "ฮีล" in text or "จิตวิญญาณ" in text:
            return "quantum_spirituality"
        if "art" in lowered or "ศิลปะ" in text:
            return "quantum_art"
        if "ai" in lowered:
            return "quantum_ai"
        if "communication" in lowered or "การสื่อสาร" in text:
            return "quantum_communication"
        if "คอมพิวเตอร์ควอนตัม" in text or "computing" in lowered or "คิวบิต" in text:
            return "quantum_computing"
        return "quantum_science"
