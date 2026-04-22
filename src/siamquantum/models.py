from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl


# ---------------------------------------------------------------------------
# Shared config
# ---------------------------------------------------------------------------

class _Row(BaseModel):
    model_config = ConfigDict(from_attributes=True)


# ---------------------------------------------------------------------------
# Table mirrors (Row = read from DB)
# ---------------------------------------------------------------------------

class SourceRow(_Row):
    id: int
    platform: str
    url: str
    title: str | None
    raw_text: str | None
    published_year: int
    fetched_at: str
    view_count: int | None = None
    like_count: int | None = None
    comment_count: int | None = None
    channel_id: str | None = None
    channel_title: str | None = None
    channel_country: str | None = None
    channel_default_language: str | None = None


class GeoRow(_Row):
    source_id: int
    ip: str | None
    lat: float | None
    lng: float | None
    city: str | None
    region: str | None
    isp: str | None
    asn_org: str | None = None
    is_cdn_resolved: bool | None = None


class EntityRow(_Row):
    source_id: int
    content_type: str | None
    production_type: str | None
    area: str | None
    engagement_level: str | None


class TripletRow(_Row):
    id: int
    source_id: int
    subject: str
    relation: str
    object: str
    confidence: float = 1.0


class StatsCacheRow(_Row):
    key: str
    value: str  # JSON string
    computed_at: str


class CommunitySubmissionRow(_Row):
    id: int
    handle: str | None
    url: str
    status: str
    submitted_at: str


class DenStreamStateRow(_Row):
    id: int
    snapshot: bytes
    updated_at: str


# ---------------------------------------------------------------------------
# Create DTOs (write to DB)
# ---------------------------------------------------------------------------

class SourceCreate(BaseModel):
    platform: str
    url: str
    title: str | None = None
    raw_text: str | None = None
    published_year: int
    fetched_at: datetime = Field(default_factory=datetime.utcnow)
    view_count: int | None = None
    like_count: int | None = None
    comment_count: int | None = None
    channel_id: str | None = None
    channel_title: str | None = None
    channel_country: str | None = None
    channel_default_language: str | None = None


class GeoCreate(BaseModel):
    source_id: int
    ip: str | None = None
    lat: float | None = None
    lng: float | None = None
    city: str | None = None
    region: str | None = None
    isp: str | None = None
    asn_org: str | None = None
    is_cdn_resolved: bool | None = None


class EntityCreate(BaseModel):
    source_id: int
    content_type: str | None = None
    production_type: str | None = None
    area: str | None = None
    engagement_level: str | None = None


class TripletCreate(BaseModel):
    source_id: int
    subject: str
    relation: str
    object: str
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)


class CommunitySubmissionCreate(BaseModel):
    handle: str | None = None
    url: str
    submitted_at: datetime = Field(default_factory=datetime.utcnow)


# ---------------------------------------------------------------------------
# Service-layer types (used across pipeline)
# ---------------------------------------------------------------------------

class SourceRaw(BaseModel):
    """Raw record from GDELT or YouTube before DB insert."""
    platform: str
    url: str
    title: str | None = None
    raw_text: str | None = None
    published_year: int
    view_count: int | None = None
    like_count: int | None = None
    comment_count: int | None = None
    channel_id: str | None = None
    channel_title: str | None = None
    channel_country: str | None = None
    channel_default_language: str | None = None


class GeoResult(BaseModel):
    ip: str
    lat: float
    lng: float
    city: str | None = None
    region: str | None = None
    isp: str | None = None
    asn_org: str | None = None
    is_cdn_resolved: bool | None = None


class Triplet(BaseModel):
    subject: str
    relation: str
    object: str
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)


class EntityClassification(BaseModel):
    content_type: str
    production_type: str
    area: str
    engagement_level: str
    media_format: str | None = None
    media_format_detail: str | None = None
    user_intent: str | None = None
    thai_cultural_angle: str | None = None


class TaxonomyClassification(BaseModel):
    media_format: str
    media_format_detail: str | None = None
    user_intent: str
    thai_cultural_angle: str | None = None


class TTestResult(BaseModel):
    year_a: int
    year_b: int
    t: float
    df: float
    p_value: float
    significant: bool


class MacroCluster(BaseModel):
    center: list[float]
    size: int
    label: str | None = None


class ServiceResult(BaseModel):
    ok: bool
    data: Any | None = None
    error: str | None = None


class RelevanceVerdict(BaseModel):
    is_quantum_tech: bool
    is_thailand_related: bool
    quantum_domain: Literal[
        "quantum_computing",
        "quantum_communication",
        "quantum_sensing",
        "quantum_materials",
        "quantum_fundamentals",
        "quantum_education",
        "quantum_policy_industry",
        "not_applicable",
    ]
    rejection_reason: str | None = None
    confidence: float = Field(ge=0.0, le=1.0)
