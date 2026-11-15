import { Hono } from 'hono'
import { getSql, type AppEnv } from '../db/client'
import { ok } from '../lib/http'

export const systemRoutes = new Hono<AppEnv>()

systemRoutes.get('/healthz', async (c) => {
  const sql = getSql(c)
  await sql`SELECT 1`
  return ok(c, { status: 'ok', runtime: 'bun', service: 'hono' })
})

systemRoutes.get('/pipeline/live', async (c) => {
  const sql = getSql(c)
  const limit = Math.min(20, Math.max(3, Number.parseInt(c.req.query('limit') ?? '8', 10) || 8))
  const rows = await sql`
    SELECT s.id, s.platform, s.url, s.title, s.published_year, s.fetched_at,
           CASE WHEN g.source_id IS NULL THEN 0 ELSE 1 END AS has_geo,
           CASE WHEN e.source_id IS NULL THEN 0 ELSE 1 END AS has_entity,
           (SELECT CAST(COUNT(*) AS INTEGER) FROM triplets t WHERE t.source_id = s.id) AS triplet_count,
           na.status AS nlp_status
    FROM sources s
    LEFT JOIN geo g ON g.source_id = s.id
    LEFT JOIN entities e ON e.source_id = s.id
    LEFT JOIN nlp_abstentions na ON na.source_id = s.id
    WHERE s.is_quantum_tech = 1 AND s.is_thailand_related = 1
    ORDER BY s.fetched_at DESC, s.id DESC
    LIMIT ${limit}
  `
  const [lastIngest] = await sql<{ updated_at: string | null }[]>`
    SELECT updated_at FROM pipeline_meta WHERE key = 'last_ingest_at' LIMIT 1
  `
  return ok(c, { items: rows, last_ingest_at: lastIngest?.updated_at ?? null })
})
