from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from siamquantum_atlas.db.base import Base


class Source(Base):
    __tablename__ = "sources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    adapter: Mapped[str] = mapped_column(String(100))
    source_name: Mapped[str] = mapped_column(String(255))
    domain: Mapped[str | None] = mapped_column(String(255), nullable=True)
    country_focus: Mapped[str | None] = mapped_column(String(100), nullable=True)
    language_focus: Mapped[str | None] = mapped_column(String(50), nullable=True)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)

    media_items: Mapped[list["MediaItem"]] = relationship(back_populates="source")


class MediaItem(Base):
    __tablename__ = "media_items"
    __table_args__ = (UniqueConstraint("canonical_url", name="uq_media_items_canonical_url"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_id: Mapped[int | None] = mapped_column(ForeignKey("sources.id"), nullable=True)
    platform: Mapped[str] = mapped_column(String(100))
    media_type: Mapped[str] = mapped_column(String(100))
    title: Mapped[str] = mapped_column(Text)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    full_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    transcript: Mapped[str | None] = mapped_column(Text, nullable=True)
    url: Mapped[str] = mapped_column(Text)
    canonical_url: Mapped[str] = mapped_column(Text)
    published_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    collected_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    language_detected: Mapped[str | None] = mapped_column(String(32), nullable=True)
    thai_relevance_score: Mapped[float] = mapped_column(Float, default=0.0)
    content_hash: Mapped[str] = mapped_column(String(64))
    raw_payload_json: Mapped[dict] = mapped_column(JSON, default=dict)

    source: Mapped[Source | None] = relationship(back_populates="media_items")
    engagement_metrics: Mapped[list["EngagementMetric"]] = relationship(back_populates="media_item")
    embeddings: Mapped[list["Embedding"]] = relationship(back_populates="media_item")
    triplets: Mapped[list["Triplet"]] = relationship(back_populates="media_item")
    classifications: Mapped[list["ItemClassification"]] = relationship(back_populates="media_item")
    cluster_memberships: Mapped[list["ClusterMembership"]] = relationship(back_populates="media_item")


class EngagementMetric(Base):
    __tablename__ = "engagement_metrics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    media_item_id: Mapped[int] = mapped_column(ForeignKey("media_items.id"))
    observed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    views: Mapped[float | None] = mapped_column(Float, nullable=True)
    likes: Mapped[float | None] = mapped_column(Float, nullable=True)
    comments: Mapped[float | None] = mapped_column(Float, nullable=True)
    shares: Mapped[float | None] = mapped_column(Float, nullable=True)
    rank_proxy: Mapped[float | None] = mapped_column(Float, nullable=True)
    popularity_proxy: Mapped[float | None] = mapped_column(Float, nullable=True)
    engagement_type: Mapped[str] = mapped_column(String(64), default="mixed")
    confidence: Mapped[float] = mapped_column(Float, default=0.5)
    provenance_json: Mapped[dict] = mapped_column(JSON, default=dict)

    media_item: Mapped[MediaItem] = relationship(back_populates="engagement_metrics")


class Embedding(Base):
    __tablename__ = "embeddings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    media_item_id: Mapped[int] = mapped_column(ForeignKey("media_items.id"))
    model_name: Mapped[str] = mapped_column(String(255))
    vector: Mapped[list[float]] = mapped_column(JSON)
    input_scope: Mapped[str] = mapped_column(String(100))

    media_item: Mapped[MediaItem] = relationship(back_populates="embeddings")


class Triplet(Base):
    __tablename__ = "triplets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    media_item_id: Mapped[int] = mapped_column(ForeignKey("media_items.id"))
    subject: Mapped[str] = mapped_column(String(255))
    predicate: Mapped[str] = mapped_column(String(255))
    object: Mapped[str] = mapped_column(String(255))
    confidence: Mapped[float] = mapped_column(Float, default=0.5)
    evidence_json: Mapped[dict] = mapped_column(JSON, default=dict)

    media_item: Mapped[MediaItem] = relationship(back_populates="triplets")


class ItemClassification(Base):
    __tablename__ = "item_classifications"

    media_item_id: Mapped[int] = mapped_column(ForeignKey("media_items.id"), primary_key=True)
    main_topic: Mapped[str] = mapped_column(String(255))
    secondary_topics_json: Mapped[list[str]] = mapped_column(JSON, default=list)
    audience_level: Mapped[str] = mapped_column(String(100), default="general")
    communicative_intent: Mapped[str] = mapped_column(String(100), default="inform")
    parasocial_signal: Mapped[str | None] = mapped_column(String(100), nullable=True)
    normalization_score: Mapped[float] = mapped_column(Float, default=0.0)
    distortion_risk: Mapped[float] = mapped_column(Float, default=0.0)
    subscores_json: Mapped[dict] = mapped_column(JSON, default=dict)
    narrative_cluster_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    extraction_confidence: Mapped[float] = mapped_column(Float, default=0.5)

    media_item: Mapped[MediaItem] = relationship(back_populates="classifications")


class Cluster(Base):
    __tablename__ = "clusters"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    cluster_type: Mapped[str] = mapped_column(String(100))
    label: Mapped[str] = mapped_column(String(255))
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    method: Mapped[str] = mapped_column(String(100))
    params_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class ClusterMembership(Base):
    __tablename__ = "cluster_memberships"

    cluster_id: Mapped[int] = mapped_column(ForeignKey("clusters.id"), primary_key=True)
    media_item_id: Mapped[int] = mapped_column(ForeignKey("media_items.id"), primary_key=True)
    membership_strength: Mapped[float] = mapped_column(Float, default=1.0)
    confidence: Mapped[float] = mapped_column(Float, default=0.5)

    media_item: Mapped[MediaItem] = relationship(back_populates="cluster_memberships")


class GraphExport(Base):
    __tablename__ = "graph_exports"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    export_type: Mapped[str] = mapped_column(String(100))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    node_count: Mapped[int] = mapped_column(Integer)
    edge_count: Mapped[int] = mapped_column(Integer)
    file_path: Mapped[str] = mapped_column(Text)
    filters_json: Mapped[dict] = mapped_column(JSON, default=dict)


class GeoSignal(Base):
    __tablename__ = "geo_signals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    media_item_id: Mapped[int | None] = mapped_column(ForeignKey("media_items.id"), nullable=True)
    province_th: Mapped[str | None] = mapped_column(String(100), nullable=True)
    province_en: Mapped[str | None] = mapped_column(String(100), nullable=True)
    region: Mapped[str | None] = mapped_column(String(50), nullable=True)
    lat: Mapped[float | None] = mapped_column(Float, nullable=True)
    lng: Mapped[float | None] = mapped_column(Float, nullable=True)
    confidence: Mapped[float] = mapped_column(Float, default=0.0)
    method: Mapped[str] = mapped_column(String(64), default="no_signal")
    inferred_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class RealtimeRun(Base):
    __tablename__ = "realtime_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    collected_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    total_items: Mapped[int] = mapped_column(Integer)
    platform_counts_json: Mapped[dict] = mapped_column(JSON, default=dict)
    cluster_counts_json: Mapped[dict] = mapped_column(JSON, default=dict)
    report_paths_json: Mapped[dict] = mapped_column(JSON, default=dict)
