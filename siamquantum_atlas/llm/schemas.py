from __future__ import annotations

from pydantic import BaseModel, Field


class TopicScore(BaseModel):
    label: str
    weight: float = Field(ge=0.0, le=1.0)
    confidence: float = Field(ge=0.0, le=1.0)


class TripletRecord(BaseModel):
    subject: str
    predicate: str
    object: str
    confidence: float = Field(ge=0.0, le=1.0)


class FramingResult(BaseModel):
    frame_labels: list[str]
    uses_and_gratifications: list[str]
    parasocial_signal: str | None = None
    normalization_subscores: dict[str, float]
    normalization_score: float = Field(ge=0.0, le=1.0)
    distortion_risk: float = Field(ge=0.0, le=1.0)
    confidence: float = Field(ge=0.0, le=1.0)


class ExtractionResult(BaseModel):
    title_th: str
    title_en: str
    summary_th: str
    summary_en: str
    main_topic: str
    secondary_topics: list[TopicScore]
    media_type: str
    communicative_intent: str
    audience_level: str
    frame_result: FramingResult
    triplets: list[TripletRecord]
    entities: list[str]
