from __future__ import annotations

from datetime import datetime
from typing import Any

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


class GeoRow(_Row):
    source_id: int
    ip: str | None
    lat: float | None
    lng: float | None
    city: str | None
    region: str | None
    isp: str | None


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


class GeoCreate(BaseModel):
    source_id: int
    ip: str | None = None
    lat: float | None = None
    lng: float | None = None
    city: str | None = None
    region: str | None = None
    isp: str | None = None


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


class GeoResult(BaseModel):
    ip: str
    lat: float
    lng: float
    city: str | None = None
    region: str | None = None
    isp: str | None = None


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
