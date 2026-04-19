# SiamQuantum Atlas

แพลตฟอร์มวิจัยสำหรับติดตามการมีส่วนร่วมของสาธารณะไทยต่อเนื้อหาเทคโนโลยีควอนตัมตั้งแต่ปี 2020 เป็นต้นไป

สแตกหลัก: Python 3.11, SQLite, GDELT API v2, YouTube Data API v3, Claude API, FastAPI, openpyxl

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

ถ้ามีสคริปต์ช่วยในสภาพแวดล้อมของคุณ:

```bash
bash scripts/download_geoip.sh
```

ถ้าไม่มี ให้ทำดังนี้:

1. สมัครบัญชี MaxMind และสร้าง license key
2. ดาวน์โหลดไฟล์ `.mmdb` ของ GeoLite2 City และ GeoLite2 ASN
3. วางไฟล์ไว้ในตำแหน่งที่โปรเจ็กต์ใช้สำหรับงาน Geo/IP enrichment

### 3. สร้างฐานข้อมูล

```bash
python -m siamquantum db init
```

หากต้องการล้างฐานข้อมูลแบบทำลายข้อมูล:

```bash
python -m siamquantum db reset --confirm
```

## ขั้นตอนดึงข้อมูลย้อนหลัง

รันคำสั่ง ingest ตามปีที่ต้องการเก็บข้อมูล

```bash
python -m siamquantum ingest gdelt --year 2024
python -m siamquantum ingest youtube --year 2024
python -m siamquantum ingest geo --pending
```

หากต้องการดึงตั้งแต่ปี 2020 ถึงปีที่ระบุ:

```bash
python -m siamquantum ingest gdelt --year 2024 --all-years
python -m siamquantum ingest youtube --year 2024 --all-years
```

## ขั้นตอน NLP และสถิติ

ประมวลผล NLP สำหรับปีเดียว:

```bash
python -m siamquantum analyze nlp --year 2024
```

รันสถิติ:

```bash
python -m siamquantum analyze stats
```

รัน flow แบบรวมขั้นต่ำสำหรับปีที่มีอยู่จริงในฐานข้อมูล:

```bash
python -m siamquantum analyze full
```

## เปิด viewer

```bash
python -m siamquantum serve
```

URL เริ่มต้น:

- `http://localhost:8765`

เปิดโหมด auto-reload สำหรับพัฒนา:

```bash
python -m siamquantum serve --reload
```

กำหนดพอร์ตเอง:

```bash
python -m siamquantum serve --port 9000
```

## อ้างอิงคำสั่ง CLI

```text
python -m siamquantum db init
python -m siamquantum db reset --confirm

python -m siamquantum ingest gdelt --year YYYY [--all-years]
python -m siamquantum ingest youtube --year YYYY [--all-years]
python -m siamquantum ingest geo --pending

python -m siamquantum analyze nlp --year YYYY
python -m siamquantum analyze stats
python -m siamquantum analyze full

python -m siamquantum serve [--port 8765] [--reload]
```

## หน้าต่าง ๆ ของระบบ

- `/dashboard` - แดชบอร์ดแผนที่ประเทศไทย
- `/network` - กราฟ 3 มิติของ entity/triplet
- `/analytics` - กราฟรายปีและป้ายแสดงนัยสำคัญทางสถิติ
- `/database` - การ์ดข้อมูลพร้อมตัวกรองและส่งออก XLSX
- `/community` - ฟอร์มรับลิงก์จากชุมชน

## Make targets

```bash
make install
make db
make ingest-historical
make serve
make test
make lint
```
