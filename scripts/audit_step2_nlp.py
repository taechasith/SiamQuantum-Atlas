"""Audit Phase 4 NLP on 10 selected sources with real cost tracking."""
from __future__ import annotations
import sys, json, time
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

import anthropic
from siamquantum.config import settings
from siamquantum.services.claude import _TRIPLET_SYSTEM, _ENTITY_SYSTEM, _parse_json
from siamquantum.models import EntityClassification, Triplet
from siamquantum.db.session import get_connection, db_path_from_url

SELECTED_IDS = [27, 16, 28, 12, 44, 175, 170, 207, 215, 201]

db_path = db_path_from_url(settings.database_url)
client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

total_input_tok = 0
total_output_tok = 0
parse_retries = 0
failures = 0
results = []

with get_connection(db_path) as conn:
    sources = {
        r["id"]: r for r in conn.execute(
            f"SELECT * FROM sources WHERE id IN ({','.join('?' for _ in SELECTED_IDS)})",
            SELECTED_IDS,
        ).fetchall()
    }

print("=== Running NLP on 10 sources ===\n")

for sid in SELECTED_IDS:
    src = sources[sid]
    text = (src["raw_text"] or src["title"] or "").strip()
    title = src["title"] or ""
    url = src["url"] or ""
    platform = src["platform"]

    print(f"--- id={sid} platform={platform} ---")
    print(f"    title: {repr(title[:80])}")

    # --- extract_triplets ---
    triplets: list[Triplet] = []
    for attempt in range(2):
        try:
            msg = client.messages.create(
                model=settings.claude_model,
                max_tokens=512,
                temperature=0,
                system=_TRIPLET_SYSTEM,
                messages=[{"role": "user", "content": f"Text:\n{text[:4000]}"}],
            )
            total_input_tok += msg.usage.input_tokens
            total_output_tok += msg.usage.output_tokens
            raw = msg.content[0].text  # type: ignore[union-attr]
            data = _parse_json(raw)
            triplets = [Triplet(**t) for t in data.get("triplets", [])]
            if attempt > 0:
                parse_retries += 1
            break
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            if attempt == 0:
                parse_retries += 1
                print(f"    PARSE RETRY (triplets): {exc}")
                continue
            print(f"    PARSE FAIL (triplets): {exc}")
            failures += 1

    # --- classify_entity ---
    entity: EntityClassification | None = None
    snippet = f"Title: {title}\nURL: {url}\n\nText:\n{text[:3000]}"
    for attempt in range(2):
        try:
            msg = client.messages.create(
                model=settings.claude_model,
                max_tokens=256,
                temperature=0,
                system=_ENTITY_SYSTEM,
                messages=[{"role": "user", "content": snippet}],
            )
            total_input_tok += msg.usage.input_tokens
            total_output_tok += msg.usage.output_tokens
            raw = msg.content[0].text  # type: ignore[union-attr]
            data = _parse_json(raw)
            entity = EntityClassification(**data)
            if attempt > 0:
                parse_retries += 1
            break
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            if attempt == 0:
                parse_retries += 1
                print(f"    PARSE RETRY (entity): {exc}")
                continue
            print(f"    PARSE FAIL (entity): {exc}")
            failures += 1

    results.append({
        "id": sid,
        "platform": platform,
        "title": title[:80],
        "triplets": [t.model_dump() for t in triplets],
        "entity": entity.model_dump() if entity else None,
    })

    print(f"    triplets={len(triplets)}  entity={entity.content_type + '/' + entity.production_type if entity else 'NONE'}")
    if triplets:
        print(f"    sample triplet: {triplets[0].subject!r} --[{triplets[0].relation}]--> {triplets[0].object!r}")
    print()
    time.sleep(0.5)

# --- Cost ---
PRICE_IN = 3.0 / 1_000_000
PRICE_OUT = 15.0 / 1_000_000
cost = total_input_tok * PRICE_IN + total_output_tok * PRICE_OUT

print("=== COST SUMMARY ===")
print(f"  input tokens:  {total_input_tok}")
print(f"  output tokens: {total_output_tok}")
print(f"  cost USD:      ${cost:.4f}")
print(f"  parse retries: {parse_retries}")
print(f"  failures:      {failures}")

# --- Distribution ---
print("\n=== CLASSIFICATION DISTRIBUTION ===")
from collections import Counter
ct = Counter(r["entity"]["content_type"] for r in results if r["entity"])
pt = Counter(r["entity"]["production_type"] for r in results if r["entity"])
el = Counter(r["entity"]["engagement_level"] for r in results if r["entity"])
print(f"  content_type:     {dict(ct)}")
print(f"  production_type:  {dict(pt)}")
print(f"  engagement_level: {dict(el)}")

# --- Triplet stats ---
counts = [len(r["triplets"]) for r in results]
print(f"\n=== TRIPLET STATS ===")
print(f"  mean={sum(counts)/len(counts):.1f}  min={min(counts)}  max={max(counts)}")

# --- Find a Thai-language source for sample ---
print("\n=== SAMPLE: Thai-language source triplets ===")
for r in results:
    if r["triplets"] and any(ord(c) > 0x0E00 for c in r["title"]):
        print(f"  source id={r['id']} title={repr(r['title'][:60])}")
        for t in r["triplets"][:3]:
            print(f"    {t['subject']!r} --[{t['relation']}]--> {t['object']!r} (conf={t['confidence']})")
        break

print("\n=== SAMPLE: Full classification ===")
for r in results:
    if r["entity"]:
        print(f"  source id={r['id']} title={repr(r['title'][:60])}")
        print(f"  {json.dumps(r['entity'], ensure_ascii=False, indent=4)}")
        break
