from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone

# Communication-value cluster labels
CLUSTER_BEGINNER_EDUCATION = "beginner_education"
CLUSTER_BREAKTHROUGH_NEWS = "breakthrough_news"
CLUSTER_STUDENT_INTEREST = "student_interest"
CLUSTER_CAREER_OPPORTUNITY = "career_opportunity"
CLUSTER_QUANTUM_COMPUTING = "quantum_computing"
CLUSTER_DAILY_LIFE = "daily_life_application"
CLUSTER_MISCONCEPTION = "misconception_confusion"
CLUSTER_HIGH_ENGAGEMENT = "high_engagement_hook"
CLUSTER_EMERGING = "emerging_topic"
CLUSTER_LOW_ENGAGEMENT = "low_engagement_topic"

ALL_CLUSTERS = [
    CLUSTER_BEGINNER_EDUCATION,
    CLUSTER_BREAKTHROUGH_NEWS,
    CLUSTER_STUDENT_INTEREST,
    CLUSTER_CAREER_OPPORTUNITY,
    CLUSTER_QUANTUM_COMPUTING,
    CLUSTER_DAILY_LIFE,
    CLUSTER_MISCONCEPTION,
    CLUSTER_HIGH_ENGAGEMENT,
    CLUSTER_EMERGING,
    CLUSTER_LOW_ENGAGEMENT,
]

CLUSTER_DESCRIPTIONS = {
    CLUSTER_BEGINNER_EDUCATION: "Introductory content explaining quantum concepts to general audiences",
    CLUSTER_BREAKTHROUGH_NEWS: "Announcements of research breakthroughs, discoveries, and milestones",
    CLUSTER_STUDENT_INTEREST: "Academic content targeting university students and researchers",
    CLUSTER_CAREER_OPPORTUNITY: "Scholarships, jobs, courses, and study-abroad opportunities",
    CLUSTER_QUANTUM_COMPUTING: "Technical computing content: qubits, gates, algorithms, hardware",
    CLUSTER_DAILY_LIFE: "Practical applications: quantum sensors, cryptography, medicine, finance",
    CLUSTER_MISCONCEPTION: "Pseudoscience, misuse of quantum terminology, or spirituality claims",
    CLUSTER_HIGH_ENGAGEMENT: "Top-quartile normalized engagement — viral or high-resonance content",
    CLUSTER_EMERGING: "Published within last 7 days — newly appearing topics",
    CLUSTER_LOW_ENGAGEMENT: "Bottom-quartile normalized engagement — poor resonance",
}

_SIGNALS: dict[str, list[str]] = {
    CLUSTER_MISCONCEPTION: [
        "ฮีล", "healing", "heal", "law of attraction", "จิตวิญญาณ", "spiritual",
        "พลังงานควอนตัม", "quantum energy", "quantum healing", "รักษา", "miracle",
        "vibration", "การสั่นสะเทือน", "consciousness", "จิตสำนึก", "chakra",
        "quantum touch", "เยียวยา", "abundance", "manifest", "ดึงดูด",
        "quantum mysticism", "metaphysics",
    ],
    CLUSTER_BREAKTHROUGH_NEWS: [
        "ค้นพบ", "discover", "breakthrough", "สำเร็จ", "ครั้งแรก", "first ever",
        "announce", "ประกาศ", "milestone", "achievement", "งานวิจัยใหม่", "new research",
        "published", "เผยแพร่", "record", "สถิติ", "world first", "ก้าวล้ำ",
        "pioneering", "นำร่อง", "historic",
    ],
    CLUSTER_CAREER_OPPORTUNITY: [
        "ทุนการศึกษา", "scholarship", "fellowship", "ทุนวิจัย", "research grant",
        "สมัครงาน", "job opening", "ตำแหน่ง", "position", "internship", "ฝึกงาน",
        "เรียนต่อ", "study abroad", "admission", "รับสมัคร", "hiring",
        "career", "อาชีพ", "เงินเดือน", "salary", "phd position",
    ],
    CLUSTER_STUDENT_INTEREST: [
        "นักศึกษา", "student", "university", "มหาวิทยาลัย", "วิทยานิพนธ์", "thesis",
        "ปริญญา", "degree", "research group", "กลุ่มวิจัย", "lab", "ห้องปฏิบัติการ",
        "professor", "อาจารย์", "coursework", "หลักสูตร", "curriculum",
        "จุฬา", "มหิดล", "มช", "kmitl", "kmutt", "kku",
        "undergraduate", "graduate", "postdoc",
    ],
    CLUSTER_QUANTUM_COMPUTING: [
        "qubit", "คิวบิต", "quantum gate", "quantum circuit", "quantum algorithm",
        "quantum error correction", "quantum supremacy", "quantum advantage",
        "ibm quantum", "google quantum", "ionq", "rigetti",
        "grover", "shor", "variational", "vqe", "qaoa",
        "entanglement", "superposition", "decoherence", "คอมพิวเตอร์ควอนตัม",
        "quantum computer", "quantum processor",
    ],
    CLUSTER_DAILY_LIFE: [
        "การแพทย์", "medical", "healthcare", "โรงพยาบาล", "hospital",
        "การเงิน", "finance", "fintech", "cryptocurrency", "quantum cryptography",
        "qkd", "การสื่อสารควอนตัม", "quantum communication", "quantum internet",
        "logistics", "โลจิสติกส์", "optimization", "การเพิ่มประสิทธิภาพ",
        "drug discovery", "material science", "battery", "ยา", "วัสดุ",
        "quantum sensor", "เซนเซอร์", "navigation", "gps", "climate",
        "everyday", "ชีวิตประจำวัน", "ใช้งาน", "practical",
    ],
    CLUSTER_BEGINNER_EDUCATION: [
        "คืออะไร", "what is", "อธิบาย", "explain", "พื้นฐาน", "basic",
        "introduction", "เบื้องต้น", "beginner", "ผู้เริ่มต้น", "มือใหม่",
        "ง่ายๆ", "simple", "เข้าใจง่าย", "easy to understand",
        "101", "guide", "คู่มือ", "for dummies", "crash course",
        "เรียนรู้", "learn", "สอน", "teach", "tutorial",
        "quantum for everyone", "ควอนตัมสำหรับทุกคน",
    ],
}

# Priority order: highest specificity first
_PRIORITY = [
    CLUSTER_MISCONCEPTION,
    CLUSTER_BREAKTHROUGH_NEWS,
    CLUSTER_CAREER_OPPORTUNITY,
    CLUSTER_STUDENT_INTEREST,
    CLUSTER_QUANTUM_COMPUTING,
    CLUSTER_DAILY_LIFE,
    CLUSTER_BEGINNER_EDUCATION,
]


@dataclass
class CommValueResult:
    cluster: str
    confidence: float
    signals_matched: list[str]
    is_emerging: bool
    days_old: float | None


def classify_comm_value(
    title: str,
    text: str,
    published_at: datetime | None = None,
    normalized_engagement: float | None = None,
    engagement_percentile: float | None = None,
) -> CommValueResult:
    combined = f"{title} {text}".lower()
    now = datetime.now(tz=timezone.utc)

    days_old: float | None = None
    is_emerging = False
    if published_at:
        pub = published_at if published_at.tzinfo else published_at.replace(tzinfo=timezone.utc)
        days_old = (now - pub).total_seconds() / 86400
        is_emerging = days_old <= 7

    best_cluster = CLUSTER_BEGINNER_EDUCATION
    best_score = 0
    best_signals: list[str] = []

    for cluster in _PRIORITY:
        signals = _SIGNALS.get(cluster, [])
        matched = [s for s in signals if s in combined]
        score = len(matched) / max(len(signals), 1) * 10 + len(matched) * 2
        if score > best_score:
            best_score = score
            best_cluster = cluster
            best_signals = matched

    # Override with engagement-based clusters if engagement_percentile available
    if engagement_percentile is not None:
        if engagement_percentile >= 0.75:
            best_cluster = CLUSTER_HIGH_ENGAGEMENT
            best_signals = ["engagement_p75+"]
        elif engagement_percentile <= 0.25 and best_score < 2:
            best_cluster = CLUSTER_LOW_ENGAGEMENT
            best_signals = ["engagement_p25-"]

    # Emerging overrides low-engagement fallback
    if is_emerging and best_cluster == CLUSTER_LOW_ENGAGEMENT:
        best_cluster = CLUSTER_EMERGING
        best_signals = [f"days_old={days_old:.1f}"]

    confidence = min(0.95, 0.40 + best_score * 0.05)
    if best_score == 0:
        confidence = 0.30

    return CommValueResult(
        cluster=best_cluster,
        confidence=confidence,
        signals_matched=best_signals[:8],
        is_emerging=is_emerging,
        days_old=days_old,
    )


def extract_keywords(text: str, top_n: int = 10) -> list[str]:
    """Simple keyword extractor — unigrams + bigrams, stops removed."""
    _STOPWORDS = {
        "the", "a", "an", "and", "or", "of", "to", "in", "is", "it", "for",
        "that", "this", "with", "on", "are", "be", "at", "by", "from",
        "ที่", "ใน", "และ", "ของ", "การ", "ใน", "เป็น", "มี", "ได้", "ว่า",
        "จาก", "ให้", "โดย", "เพื่อ", "แต่", "หรือ", "จะ", "ไม่", "ก็",
        "quantum", "ควอนตัม", "thailand", "ไทย", "thai",
    }

    tokens = re.findall(r"[a-zA-Z\u0E00-\u0E7F]{3,}", text.lower())
    filtered = [t for t in tokens if t not in _STOPWORDS]

    from collections import Counter
    unigrams = Counter(filtered)
    bigrams = Counter(
        f"{filtered[i]} {filtered[i+1]}"
        for i in range(len(filtered) - 1)
        if filtered[i] not in _STOPWORDS and filtered[i + 1] not in _STOPWORDS
    )

    combined: Counter = Counter()
    combined.update(unigrams)
    combined.update({k: v * 1.5 for k, v in bigrams.items()})  # bigrams weighted higher

    return [k for k, _ in combined.most_common(top_n)]
