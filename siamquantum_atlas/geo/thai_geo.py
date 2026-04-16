from __future__ import annotations

import re
from dataclasses import dataclass

# 77 Thai provinces: (thai_name, english_name, region, lat, lng)
_PROVINCES: list[tuple[str, str, str, float, float]] = [
    ("กรุงเทพมหานคร", "Bangkok", "Central", 13.7563, 100.5018),
    ("กระบี่", "Krabi", "South", 8.0863, 98.9063),
    ("กาญจนบุรี", "Kanchanaburi", "Central", 14.0023, 99.5328),
    ("กาฬสินธุ์", "Kalasin", "Northeast", 16.4314, 103.5058),
    ("กำแพงเพชร", "Kamphaeng Phet", "North", 16.4827, 99.5220),
    ("ขอนแก่น", "Khon Kaen", "Northeast", 16.4322, 102.8236),
    ("จันทบุรี", "Chanthaburi", "East", 12.6112, 102.1031),
    ("ฉะเชิงเทรา", "Chachoengsao", "East", 13.6904, 101.0779),
    ("ชลบุรี", "Chonburi", "East", 13.3611, 100.9847),
    ("ชัยนาท", "Chai Nat", "Central", 15.1853, 100.1249),
    ("ชัยภูมิ", "Chaiyaphum", "Northeast", 15.8068, 102.0318),
    ("ชุมพร", "Chumphon", "South", 10.4930, 99.1800),
    ("เชียงราย", "Chiang Rai", "North", 19.9105, 99.8406),
    ("เชียงใหม่", "Chiang Mai", "North", 18.7883, 98.9853),
    ("ตรัง", "Trang", "South", 7.5590, 99.6112),
    ("ตราด", "Trat", "East", 12.2427, 102.5176),
    ("ตาก", "Tak", "North", 16.8798, 99.1257),
    ("นครนายก", "Nakhon Nayok", "Central", 14.2057, 101.2132),
    ("นครปฐม", "Nakhon Pathom", "Central", 13.8199, 100.0442),
    ("นครพนม", "Nakhon Phanom", "Northeast", 17.3924, 104.7693),
    ("นครราชสีมา", "Nakhon Ratchasima", "Northeast", 14.9799, 102.0978),
    ("นครศรีธรรมราช", "Nakhon Si Thammarat", "South", 8.4325, 99.9599),
    ("นครสวรรค์", "Nakhon Sawan", "Central", 15.7030, 100.1373),
    ("นนทบุรี", "Nonthaburi", "Central", 13.8621, 100.5144),
    ("นราธิวาส", "Narathiwat", "South", 6.4255, 101.8253),
    ("น่าน", "Nan", "North", 18.7756, 100.7730),
    ("บึงกาฬ", "Bueng Kan", "Northeast", 18.3609, 103.6466),
    ("บุรีรัมย์", "Buriram", "Northeast", 14.9939, 103.1029),
    ("ปทุมธานี", "Pathum Thani", "Central", 14.0208, 100.5251),
    ("ประจวบคีรีขันธ์", "Prachuap Khiri Khan", "Central", 11.8126, 99.7967),
    ("ปราจีนบุรี", "Prachinburi", "East", 14.0519, 101.3657),
    ("ปัตตานี", "Pattani", "South", 6.8696, 101.2504),
    ("พระนครศรีอยุธยา", "Phra Nakhon Si Ayutthaya", "Central", 14.3692, 100.5877),
    ("พะเยา", "Phayao", "North", 19.1664, 99.9014),
    ("พระแสง", "Phangnga", "South", 8.4512, 98.5263),
    ("พัทลุง", "Phatthalung", "South", 7.6166, 100.0747),
    ("พิจิตร", "Phichit", "North", 16.4429, 100.3488),
    ("พิษณุโลก", "Phitsanulok", "North", 16.8211, 100.2659),
    ("เพชรบุรี", "Phetchaburi", "Central", 13.1119, 99.9390),
    ("เพชรบูรณ์", "Phetchabun", "North", 16.4189, 101.1591),
    ("แพร่", "Phrae", "North", 18.1445, 100.1402),
    ("ภูเก็ต", "Phuket", "South", 7.9519, 98.3381),
    ("มหาสารคาม", "Maha Sarakham", "Northeast", 16.0139, 103.1615),
    ("มุกดาหาร", "Mukdahan", "Northeast", 16.5425, 104.7233),
    ("แม่ฮ่องสอน", "Mae Hong Son", "North", 19.3020, 97.9654),
    ("ยโสธร", "Yasothon", "Northeast", 15.7928, 104.1452),
    ("ยะลา", "Yala", "South", 6.5407, 101.2804),
    ("ร้อยเอ็ด", "Roi Et", "Northeast", 16.0538, 103.6520),
    ("ระนอง", "Ranong", "South", 9.9529, 98.6085),
    ("ระยอง", "Rayong", "East", 12.6819, 101.2816),
    ("ราชบุรี", "Ratchaburi", "Central", 13.5282, 99.8134),
    ("ลพบุรี", "Lopburi", "Central", 14.7995, 100.6534),
    ("ลำปาง", "Lampang", "North", 18.2888, 99.4900),
    ("ลำพูน", "Lamphun", "North", 18.5744, 99.0087),
    ("เลย", "Loei", "Northeast", 17.4861, 101.7223),
    ("ศรีสะเกษ", "Si Sa Ket", "Northeast", 15.1186, 104.3220),
    ("สกลนคร", "Sakon Nakhon", "Northeast", 17.1664, 104.1486),
    ("สงขลา", "Songkhla", "South", 7.1756, 100.6142),
    ("สตูล", "Satun", "South", 6.6238, 100.0673),
    ("สมุทรปราการ", "Samut Prakan", "Central", 13.5991, 100.5998),
    ("สมุทรสงคราม", "Samut Songkhram", "Central", 13.4098, 100.0023),
    ("สมุทรสาคร", "Samut Sakhon", "Central", 13.5475, 100.2747),
    ("สระแก้ว", "Sa Kaeo", "East", 13.8240, 102.0648),
    ("สระบุรี", "Saraburi", "Central", 14.5289, 100.9107),
    ("สิงห์บุรี", "Sing Buri", "Central", 14.8936, 100.3979),
    ("สุโขทัย", "Sukhothai", "North", 17.0069, 99.8266),
    ("สุพรรณบุรี", "Suphan Buri", "Central", 14.4744, 100.1177),
    ("สุราษฎร์ธานี", "Surat Thani", "South", 9.1382, 99.3217),
    ("สุรินทร์", "Surin", "Northeast", 14.8820, 103.4937),
    ("หนองคาย", "Nong Khai", "Northeast", 17.8782, 102.7419),
    ("หนองบัวลำภู", "Nong Bua Lam Phu", "Northeast", 17.2022, 102.4260),
    ("อ่างทอง", "Ang Thong", "Central", 14.5896, 100.4550),
    ("อำนาจเจริญ", "Amnat Charoen", "Northeast", 15.8656, 104.6258),
    ("อุดรธานี", "Udon Thani", "Northeast", 17.4138, 102.7871),
    ("อุตรดิตถ์", "Uttaradit", "North", 17.6200, 100.0993),
    ("อุทัยธานี", "Uthai Thani", "Central", 15.3835, 100.0245),
    ("อุบลราชธานี", "Ubon Ratchathani", "Northeast", 15.2448, 104.8473),
    ("พังงา", "Phang Nga", "South", 8.4512, 98.5263),
]

# Build lookup dicts
_BY_THAI: dict[str, tuple] = {p[0]: p for p in _PROVINCES}
_BY_ENGLISH: dict[str, tuple] = {p[1].lower(): p for p in _PROVINCES}

# Aliases: shortened/common variants
_ALIASES: dict[str, str] = {
    "กทม": "กรุงเทพมหานคร",
    "กรุงเทพ": "กรุงเทพมหานคร",
    "bangkok": "กรุงเทพมหานคร",
    "bkk": "กรุงเทพมหานคร",
    "korat": "นครราชสีมา",
    "โคราช": "นครราชสีมา",
    "chiang mai": "เชียงใหม่",
    "chiangmai": "เชียงใหม่",
    "chiang rai": "เชียงราย",
    "chiangrai": "เชียงราย",
    "phuket": "ภูเก็ต",
    "ภูเก็ต": "ภูเก็ต",
    "hadyai": "สงขลา",
    "hat yai": "สงขลา",
    "hatyai": "สงขลา",
    "หาดใหญ่": "สงขลา",
    "pattaya": "ชลบุรี",
    "พัทยา": "ชลบุรี",
    "ayutthaya": "พระนครศรีอยุธยา",
    "อยุธยา": "พระนครศรีอยุธยา",
    "khon kaen": "ขอนแก่น",
    "khonkaen": "ขอนแก่น",
    "ubon": "อุบลราชธานี",
    "udon": "อุดรธานี",
    "chonburi": "ชลบุรี",
    "rayong": "ระยอง",
    "ชลบุรี": "ชลบุรี",
    "mahidol": "กรุงเทพมหานคร",
    "chulalongkorn": "กรุงเทพมหานคร",
    "จุฬา": "กรุงเทพมหานคร",
    "มหิดล": "กรุงเทพมหานคร",
    "kmitl": "กรุงเทพมหานคร",
    "kmutt": "กรุงเทพมหานคร",
    "ลาดกระบัง": "กรุงเทพมหานคร",
    "cmkl": "กรุงเทพมหานคร",
    "kasetsart": "กรุงเทพมหานคร",
    "เกษตรศาสตร์": "กรุงเทพมหานคร",
    "thammasat": "กรุงเทพมหานคร",
    "ธรรมศาสตร์": "กรุงเทพมหานคร",
    "chiang mai university": "เชียงใหม่",
    "มช": "เชียงใหม่",
    "มหาวิทยาลัยเชียงใหม่": "เชียงใหม่",
    "kku": "ขอนแก่น",
    "มขก": "ขอนแก่น",
    "มหาวิทยาลัยขอนแก่น": "ขอนแก่น",
    "prince of songkla": "สงขลา",
    "มอ": "สงขลา",
    "ม.อ": "สงขลา",
    "suranaree": "นครราชสีมา",
    "มทส": "นครราชสีมา",
}

# Region centroids (fallback when province not found but region is)
_REGION_CENTROIDS: dict[str, tuple[float, float]] = {
    "Central": (13.9, 100.5),
    "North": (18.5, 99.5),
    "Northeast": (16.0, 103.0),
    "South": (8.0, 99.5),
    "East": (13.0, 101.5),
}


@dataclass
class GeoInference:
    province_th: str | None
    province_en: str | None
    region: str | None
    lat: float | None
    lng: float | None
    confidence: float
    method: str  # explicit_thai | explicit_english | alias | domain | fallback_country


def infer_geo(text: str, domain: str | None = None, url: str | None = None) -> GeoInference:
    """Infer Thai province/region from text content and metadata.

    Uses only public location signals — never IP addresses.
    """
    text_lower = text.lower() if text else ""
    combined = f"{text_lower} {(domain or '')} {(url or '')}"

    # 1. Exact Thai province name match
    for thai_name, prov in _BY_THAI.items():
        if thai_name in text:
            return GeoInference(
                province_th=prov[0], province_en=prov[1], region=prov[2],
                lat=prov[3], lng=prov[4], confidence=0.85, method="explicit_thai",
            )

    # 2. Alias match (Thai/English shortcuts, university names)
    for alias, thai_name in _ALIASES.items():
        if alias.lower() in combined:
            prov = _BY_THAI[thai_name]
            return GeoInference(
                province_th=prov[0], province_en=prov[1], region=prov[2],
                lat=prov[3], lng=prov[4], confidence=0.75, method="alias",
            )

    # 3. English province name match
    for en_name_lower, prov in _BY_ENGLISH.items():
        if re.search(r"\b" + re.escape(en_name_lower) + r"\b", combined):
            return GeoInference(
                province_th=prov[0], province_en=prov[1], region=prov[2],
                lat=prov[3], lng=prov[4], confidence=0.70, method="explicit_english",
            )

    # 4. .th domain → country-level, no specific province
    if domain and domain.endswith(".th"):
        return GeoInference(
            province_th=None, province_en=None, region=None,
            lat=15.87, lng=100.99, confidence=0.55, method="domain",
        )

    # 5. "thailand" or "ไทย" in text → country-level
    if "thailand" in combined or "ไทย" in text:
        return GeoInference(
            province_th=None, province_en=None, region=None,
            lat=15.87, lng=100.99, confidence=0.40, method="fallback_country",
        )

    return GeoInference(
        province_th=None, province_en=None, region=None,
        lat=None, lng=None, confidence=0.0, method="no_signal",
    )


def build_province_hotspot_map(items: list[dict]) -> list[dict]:
    """Aggregate items by province for GEE-compatible FeatureCollection."""
    from collections import defaultdict
    buckets: dict[str, list[dict]] = defaultdict(list)
    country_level: list[dict] = []

    for item in items:
        geo = item.get("geo", {})
        prov_en = geo.get("province_en")
        if prov_en:
            buckets[prov_en].append(item)
        elif geo.get("confidence", 0) > 0:
            country_level.append(item)

    features = []
    all_counts = [len(v) for v in buckets.values()]
    max_count = max(all_counts) if all_counts else 1

    for rank, (prov_en, prov_items) in enumerate(
        sorted(buckets.items(), key=lambda kv: len(kv[1]), reverse=True), start=1
    ):
        prov_data = _BY_ENGLISH.get(prov_en.lower())
        if not prov_data:
            continue
        eng_scores = [i.get("normalized_engagement", 0) for i in prov_items if i.get("normalized_engagement") is not None]
        avg_eng = sum(eng_scores) / len(eng_scores) if eng_scores else None
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [prov_data[4], prov_data[3]]},
            "properties": {
                "province_th": prov_data[0],
                "province_en": prov_en,
                "region": prov_data[2],
                "item_count": len(prov_items),
                "hotspot_rank": rank,
                "avg_normalized_engagement": round(avg_eng, 2) if avg_eng is not None else None,
                "hotspot_intensity": round(len(prov_items) / max_count, 3),
                "top_clusters": _top_clusters(prov_items),
                "centroid_source": "province_centroid",
            },
        })

    return features


def _top_clusters(items: list[dict]) -> list[str]:
    from collections import Counter
    counts: Counter[str] = Counter()
    for item in items:
        c = item.get("comm_value_cluster")
        if c:
            counts[c] += 1
    return [k for k, _ in counts.most_common(3)]
