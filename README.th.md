# SiamQuantum Atlas

แพลตฟอร์มวิจัยสำหรับติดตามการมีส่วนร่วมของสาธารณะไทยต่อเนื้อหาเทคโนโลยีควอนตัมตั้งแต่ปี 2020 เป็นต้นไป

สแตกหลัก: Python 3.11+, SQLite, GDELT API v2, YouTube Data API v3, Claude API, FastAPI + uvicorn, Jinja2, Leaflet.js, Chart.js, 3d-force-graph

## เริ่มต้นใช้งาน

### 1. ตั้งค่าสภาพแวดล้อม

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
```

สำหรับ Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
```

คัดลอกไฟล์ตัวอย่างและตั้งค่า `.env`:

```bash
cp .env.example .env
```

ค่าที่ต้องตั้งอย่างน้อย:

- `SIAMQUANTUM_DATABASE_URL`
- `SIAMQUANTUM_ANTHROPIC_API_KEY`
- `SIAMQUANTUM_YOUTUBE_API_KEY`
- `SIAMQUANTUM_VIEWER_PORT`

### 2. ดาวน์โหลด GeoLite2

การทำ Geo backfill ต้องใช้ฐานข้อมูล MaxMind GeoLite2 City และ ASN

```bash
bash scripts/download_geoip.sh
```

หากไม่มีสคริปต์ช่วย:

1. สมัครบัญชี MaxMind และสร้าง license key
2. ดาวน์โหลดไฟล์ `.mmdb` ของ GeoLite2 City และ GeoLite2 ASN
3. วางไฟล์ไว้ใน `data/geoip/`

### 3. สร้างฐานข้อมูล

```bash
python -m siamquantum db init
```

หากต้องการล้างฐานข้อมูล:

```bash
python -m siamquantum db reset --confirm
```

## ขั้นตอนดึงข้อมูลย้อนหลัง

```bash
python -m siamquantum ingest seeds
python -m siamquantum ingest rss --feed all
python -m siamquantum ingest gdelt --year 2024 [--all-years]
python -m siamquantum ingest youtube --year 2024 [--all-years]
python -m siamquantum ingest geo --pending
```

## ขั้นตอน NLP และสถิติ

```bash
python -m siamquantum analyze nlp --year 2024
python -m siamquantum analyze stats
python -m siamquantum analyze taxonomy-stats
python -m siamquantum analyze graph-metrics
```

รัน flow แบบรวมครบทุกขั้นตอน:

```bash
python -m siamquantum analyze full
```

ตรวจสอบและปรับ relevance flags:

```bash
python -m siamquantum filter relevance
python -m siamquantum filter recheck-low-confidence
```

## เปิด viewer

```bash
python -m siamquantum serve
```

URL เริ่มต้น: `http://localhost:8765`

เปิดโหมด auto-reload สำหรับพัฒนา:

```bash
python -m siamquantum serve --reload
```

## ตั้งค่า Supabase Auth

ระบบรองรับ Supabase Auth สำหรับล็อกอิน, โปรไฟล์, และการส่งข้อมูลจากชุมชน หากไม่ตั้งค่า env vars ระบบจะใช้ local auth mode แทน (SQLite-backed)

ตั้งค่า env vars สำหรับ Supabase mode:

```text
SUPABASE_URL=...
SUPABASE_PUBLISHABLE_KEY=...
SUPABASE_SECRET_KEY=...
```

รัน SQL migration: `supabase/migrations/20260429_auth_profiles_submitted_data.sql`

ขั้นตอนใน Supabase dashboard:

1. รัน SQL migration ใน SQL editor
2. เปิด `Authentication → Providers → Google`
3. ใส่ Google OAuth client ID และ secret
4. เพิ่ม redirect URLs ที่ชี้กลับมาที่ `/profile`

## หน้าต่าง ๆ ของระบบ

| URL | คำอธิบาย |
|-----|----------|
| `/dashboard` | แดชบอร์ดแผนที่และภาพรวมแหล่งข้อมูล |
| `/network` | กราฟ 3 มิติของ entity/triplet |
| `/analytics` | กราฟรายปีและสถิติ engagement |
| `/database` | การ์ดข้อมูลพร้อมตัวกรองและส่งออก XLSX |
| `/submit-data` | ฟอร์มรับลิงก์จากชุมชน (ต้องล็อกอิน) |
| `/profile` | ล็อกอิน, สมัครสมาชิก, โปรไฟล์ผู้ใช้ |
| `/admin/submitted-data` | คิวรีวิวสำหรับ admin |

Redirect: `/community` → `/submit-data`, `/overview` → `/dashboard`

## Prefect Orchestration (ตัวเลือก)

ใช้ Prefect 3 สำหรับรัน pipeline แบบมี schedule ดูรายละเอียดใน `docs/prefect.md`

```bash
pip install prefect>=3
python -m siamquantum orchestration serve
```

## อ้างอิงคำสั่ง CLI

```text
python -m siamquantum db init
python -m siamquantum db reset --confirm
python -m siamquantum db audit [--fix]

python -m siamquantum ingest today
python -m siamquantum ingest seeds
python -m siamquantum ingest rss --feed all
python -m siamquantum ingest gdelt --year YYYY [--all-years]
python -m siamquantum ingest youtube --year YYYY [--all-years]
python -m siamquantum ingest geo --pending

python -m siamquantum analyze nlp --year YYYY
python -m siamquantum analyze stats
python -m siamquantum analyze taxonomy-stats
python -m siamquantum analyze graph-metrics
python -m siamquantum analyze full

python -m siamquantum filter relevance
python -m siamquantum filter recheck-low-confidence

python -m siamquantum serve [--port 8765] [--reload]

python -m siamquantum orchestration refresh
python -m siamquantum orchestration healthcheck
python -m siamquantum orchestration serve
python -m siamquantum orchestration deploy
python -m siamquantum orchestration worker
```

## Make targets

```bash
make install          # pip install -e ".[dev]"
make db               # db init
make ingest-historical  # gdelt + youtube 2020–2024 + geo
make serve            # python -m siamquantum serve
make test             # pytest
make lint             # ruff check + format check
```
