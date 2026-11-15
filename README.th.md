# SiamQuantum Atlas

แพลตฟอร์มข้อมูลควอนตัมไทย: Vue 3 SPA + Hono API + SQLite + Caddy — ไม่ใช้ Python runtime แล้ว

## เริ่มใช้งาน

```bash
cp .env.example .env
docker compose up -d --build
```

เปิด `http://localhost:8765`

## ข้อมูล

API อ่านไฟล์ `data/processed/siamquantum_atlas.db` โดยตรง หากยังไม่มีไฟล์ ระบบจะสร้าง schema ว่างให้อัตโนมัติ

## Development

```bash
cd server && bun install && bun run dev
cd frontend && npm ci && npm run dev
```

## รูปโปรไฟล์ (R2)

ตั้งค่า R2 S3 API บนเครื่อง Server:

```env
R2_ACCOUNT_ID=...
R2_ACCESS_KEY_ID=...
R2_SECRET_ACCESS_KEY=...
R2_BUCKET=qatlas-storage
```

สร้าง R2 API Token สิทธิ์ **Object Read & Write** ของ bucket นี้
Frontend จะคุยกับ Hono API เท่านั้น ไม่ได้รับ credential โดยตรง

- `POST /api/profile/avatar` — อัปโหลดรูป (ต้อง login)
- `GET /api/avatar/:userId/:file` — เสิร์ฟรูปพร้อม cache

ทดสอบ credential:

```bash
cd server
bun run check:r2
```

## Routes

- `/dashboard` — Analytics หลัก
- `/atlas` — Globe / Network
- `/database` — คลังแหล่งข้อมูล
- `/submit-data` — ส่งงานวิจัย
- `/profile` — โปรไฟล์
- `/admin` — ผู้ดูแลระบบ
- `/about`

## ข้อควรระวัง

- ห้าม commit `.env`, database dump หรือ credential
- SQLite เป็นแหล่งข้อมูลหลัก (schema เดียวกับ D1)
- ไฟล์ฐานข้อมูลไม่ได้ bundle ไว้ใน repo
