# SiamQuantum Atlas

แพลตฟอร์มข้อมูลควอนตัมไทย: Vue 3 SPA + Hono API + PostgreSQL 16 + Caddy — ไม่ใช้ Python runtime แล้ว

## เริ่มใช้งาน

```bash
cp .env.example .env
docker compose up -d --build
```

เปิด `http://localhost:8765`

## นำเข้าข้อมูลเดิม

ถ้ามีไฟล์ SQLite เดิมในเครื่อง:

```bash
cd server
DATABASE_URL=postgres://siamquantum:changeme-dev-password@127.0.0.1:5432/siamquantum_atlas bun run import
```

## Development

```bash
cd server && bun install && bun run migrate && bun run dev
cd frontend && npm ci && npm run dev
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
- PostgreSQL เป็นแหล่งข้อมูลหลัก
- SQLite ใช้เพียงเป็นแหล่ง import ข้อมูลเดิม (ไม่ได้ bundle ไว้ใน repo)
