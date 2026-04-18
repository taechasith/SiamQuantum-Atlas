# SiamQuantum Atlas

แพลตฟอร์มวิจัยติดตามระดับการมีส่วนร่วมของประชากรไทยต่อเนื้อหาเทคโนโลยีควอนตัม (2563–ปัจจุบัน)

**สแต็ก:** Python 3.11 · SQLite · GDELT API v2 · YouTube Data API v3 · Claude API · FastAPI (พอร์ต 8765)

## เริ่มต้นใช้งาน

```bash
# 1. ติดตั้ง dependencies
pip install -e ".[dev]"

# 2. ตั้งค่า environment
cp .env.example .env
# แก้ไข .env — ใส่ค่า SIAMQUANTUM_ANTHROPIC_API_KEY, SIAMQUANTUM_YOUTUBE_API_KEY, MAXMIND_LICENSE_KEY

# 3. ดาวน์โหลดฐานข้อมูล GeoLite2
bash scripts/download_geoip.sh

# 4. สร้างฐานข้อมูล
python -m siamquantum db init

# 5. ดึงข้อมูลย้อนหลัง (2563–2567)
make ingest-historical

# 6. ประมวลผล NLP + สถิติ
python -m siamquantum analyze full

# 7. เปิด viewer
python -m siamquantum serve
# → http://localhost:8765
```

## หน้าต่างระบบ
- `/dashboard` — แผนที่ประเทศไทยพร้อมหมุดแหล่งข้อมูล
- `/network` — กราฟโครงข่าย 3 มิติ
- `/analytics` — กราฟการมีส่วนร่วมรายปี + ป้ายนัยสำคัญทางสถิติ
- `/database` — การ์ดข้อมูลพร้อมกรอง + ส่งออก XLSX
- `/community` — ฟอร์มส่งแหล่งข้อมูลด้วยตนเอง
