from __future__ import annotations

from collections import defaultdict
from pathlib import Path

from sqlalchemy import delete, select

from siamquantum_atlas.adapters.base import RawMediaRecord
from siamquantum_atlas.adapters.film_tv import FilmTVAdapter
from siamquantum_atlas.adapters.gdelt import GDELTAdapter
from siamquantum_atlas.adapters.podcasts import PodcastAdapter
from siamquantum_atlas.adapters.youtube import YouTubeAdapter
from siamquantum_atlas.clustering.graph_clusters import build_similarity_graph
from siamquantum_atlas.clustering.semantic import semantic_cluster
from siamquantum_atlas.db.models import Cluster, ClusterMembership, Embedding, EngagementMetric, GraphExport, ItemClassification, MediaItem, Source, Triplet
from siamquantum_atlas.db.session import get_session
from siamquantum_atlas.graph.builder import build_graph_payload
from siamquantum_atlas.ingestion.dedupe import content_fingerprint, dedupe_records
from siamquantum_atlas.ingestion.normalization import normalize_record_text
from siamquantum_atlas.llm.extractors import ClaudeExtractionService
from siamquantum_atlas.nlp.embeddings import embed_text
from siamquantum_atlas.settings import settings
from siamquantum_atlas.utils.files import ensure_dir
from siamquantum_atlas.utils.files import write_json
from siamquantum_atlas.utils.viewer_tools import copy_export_to_viewer


class PipelineRunner:
    def __init__(self) -> None:
        self.sample_path = settings.samples_dir / "thai_quantum_media.json"
        self.extractor = ClaudeExtractionService()

    def load_demo_records(self) -> list[RawMediaRecord]:
        records: list[RawMediaRecord] = []
        for adapter in [GDELTAdapter(), YouTubeAdapter(), PodcastAdapter(), FilmTVAdapter()]:
            records.extend(adapter.fetch(sample_path=self.sample_path))
        return dedupe_records(records)

    def run_demo(self) -> int:
        records = self.load_demo_records()
        self.persist_records(records)
        self.enrich_records()
        self.cluster_records()
        return len(records)

    def run_backfill(self, years: int) -> int:
        _ = years
        return self.run_demo()

    def run_refresh(self) -> int:
        return self.run_demo()

    def persist_records(self, records: list[RawMediaRecord]) -> None:
        with get_session() as session:
            source_cache: dict[str, Source] = {}
            for record in records:
                if record.adapter not in source_cache:
                    source = session.scalar(select(Source).where(Source.adapter == record.adapter))
                    if source is None:
                        source = Source(adapter=record.adapter, source_name=record.adapter, domain=record.domain, country_focus="TH", language_focus="th,en", metadata_json={"bootstrap": "sample"})
                        session.add(source)
                        session.flush()
                    source_cache[record.adapter] = source
                if session.scalar(select(MediaItem).where(MediaItem.canonical_url == record.canonical_url)):
                    continue
                normalized = normalize_record_text(record.title, record.description, record.full_text)
                item = MediaItem(
                    source_id=source_cache[record.adapter].id,
                    platform=record.platform,
                    media_type=record.media_type,
                    title=normalized["title"],
                    description=normalized["description"],
                    full_text=normalized["full_text"],
                    transcript=None,
                    url=record.url,
                    canonical_url=record.canonical_url,
                    published_at=record.published_at,
                    language_detected=normalized["language_detected"],
                    thai_relevance_score=1.0 if "th" in normalized["language_detected"] else 0.6,
                    content_hash=content_fingerprint(record),
                    raw_payload_json=record.raw_payload,
                )
                session.add(item)
                session.flush()
                session.add(
                    EngagementMetric(
                        media_item_id=item.id,
                        views=record.views,
                        likes=record.likes,
                        comments=record.comments,
                        shares=record.shares,
                        rank_proxy=record.rank_proxy,
                        popularity_proxy=record.popularity_proxy,
                        engagement_type="hard" if record.views is not None else "proxy",
                        confidence=0.85 if record.views is not None else 0.6,
                        provenance_json={"adapter": record.adapter},
                    )
                )
            session.commit()

    def enrich_records(self) -> None:
        with get_session() as session:
            items = session.scalars(select(MediaItem)).all()
            for item in items:
                if session.get(ItemClassification, item.id) is not None:
                    continue
                text = " ".join(filter(None, [item.title, item.description, item.full_text]))
                extraction = self.extractor.extract(item.title, text, item.media_type)
                session.add(Embedding(media_item_id=item.id, model_name="hashed-thai-demo", vector=embed_text(text), input_scope="title_description_full_text"))
                session.add(
                    ItemClassification(
                        media_item_id=item.id,
                        main_topic=extraction.main_topic,
                        secondary_topics_json=extraction.frame_result.frame_labels,
                        audience_level=extraction.audience_level,
                        communicative_intent=extraction.communicative_intent,
                        parasocial_signal=extraction.frame_result.parasocial_signal,
                        normalization_score=extraction.frame_result.normalization_score,
                        distortion_risk=extraction.frame_result.distortion_risk,
                        subscores_json=extraction.frame_result.normalization_subscores,
                        narrative_cluster_id=None,
                        extraction_confidence=extraction.frame_result.confidence,
                    )
                )
                for triplet in extraction.triplets:
                    session.add(Triplet(media_item_id=item.id, subject=triplet.subject, predicate=triplet.predicate, object=triplet.object, confidence=triplet.confidence, evidence_json={"method": "claude_or_fallback"}))
            session.commit()

    def cluster_records(self) -> None:
        with get_session() as session:
            session.execute(delete(ClusterMembership))
            session.execute(delete(Cluster))
            session.commit()
            items = session.scalars(select(MediaItem)).all()
            embeddings = {row.media_item_id: row.vector for row in session.scalars(select(Embedding)).all()}
            ordered_items = [item for item in items if item.id in embeddings]
            if not ordered_items:
                return
            vectors = [embeddings[item.id] for item in ordered_items]
            semantic = semantic_cluster(vectors, k=min(4, len(vectors)))
            communities = build_similarity_graph(vectors).communities
            groups: dict[int, list[MediaItem]] = defaultdict(list)
            for item, label in zip(ordered_items, semantic.labels, strict=False):
                groups[label].append(item)
            for label, members in groups.items():
                cluster = Cluster(cluster_type="semantic_topic", label=f"semantic_{label}", description=f"Semantic cluster {label}", method="kmeans", params_json={"label": label})
                session.add(cluster)
                session.flush()
                for member in members:
                    session.add(ClusterMembership(cluster_id=cluster.id, media_item_id=member.id, membership_strength=1.0, confidence=0.7))
                    classification = session.get(ItemClassification, member.id)
                    if classification:
                        classification.narrative_cluster_id = str(cluster.id)
            for index, community in enumerate(communities):
                cluster = Cluster(cluster_type="graph_community", label=f"community_{index}", description=f"Connected similarity community {index}", method="connected_components", params_json={"size": len(community)})
                session.add(cluster)
                session.flush()
                for member_index in community:
                    session.add(ClusterMembership(cluster_id=cluster.id, media_item_id=ordered_items[member_index].id, membership_strength=0.8, confidence=0.6))
            session.commit()

    def export_latest_graph(self) -> Path:
        with get_session() as session:
            items = session.scalars(select(MediaItem)).all()
            classifications = {row.media_item_id: row for row in session.scalars(select(ItemClassification)).all()}
            memberships = session.scalars(select(ClusterMembership)).all()
            clusters = {row.id: row for row in session.scalars(select(Cluster)).all()}
            for item in items:
                _ = item.engagement_metrics
            bundle = build_graph_payload(items, classifications, memberships, clusters)
            output_path = settings.exports_dir / "siamquantum_atlas_graph.json"
            ensure_dir(output_path.parent)
            graph_payload = {
                "name": "SiamQuantum Atlas",
                "layers": bundle.layers,
                "nodes": bundle.nodes,
                "links": bundle.edges,
                "metadata": {"export_format": "3d-force-graph", "generated_by": "python"},
            }
            write_json(output_path, graph_payload)
            session.add(GraphExport(export_type="3d_force_graph", node_count=len(bundle.nodes), edge_count=len(bundle.edges), file_path=str(output_path), filters_json={"latest": True}))
            session.commit()
            copy_export_to_viewer(output_path)
            return output_path

    def export_latest_arena(self) -> Path:
        return self.export_latest_graph()

    def generate_reports(self) -> tuple[Path, Path]:
        from siamquantum_atlas.reporting.csv_report import write_csv_report
        from siamquantum_atlas.reporting.markdown_report import write_markdown_report

        with get_session() as session:
            items = session.scalars(select(MediaItem)).all()
            classifications = {row.media_item_id: row for row in session.scalars(select(ItemClassification)).all()}
            md_path = settings.exports_dir / "report.md"
            csv_path = settings.exports_dir / "report.csv"
            ensure_dir(md_path.parent)
            write_markdown_report(items, classifications, md_path)
            write_csv_report(items, classifications, csv_path)
            return md_path, csv_path
