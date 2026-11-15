import { Hono } from 'hono'
import { sql, PUBLIC_SCOPE, relevanceMeta, scopeClause } from '../db/client'
import { fail, intParam, ok } from '../lib/http'

export const publicRoutes = new Hono()

type SourceRow = Record<string, unknown>
type GeoRow = Record<string, unknown>

const COUNTRY_CENTROIDS: Record<string, [number, number]> = {
  TH: [13.7563, 100.5018],
  US: [37.0902, -95.7129],
  CN: [35.8617, 104.1954],
  JP: [36.2048, 138.2529],
  GB: [55.3781, -3.436],
  SG: [1.3521, 103.8198],
  DE: [51.1657, 10.4515],
  KR: [35.9078, 127.7669],
  AU: [-25.2744, 133.7751],
  IN: [20.5937, 78.9629],
  FR: [46.2276, 2.2137],
  CA: [56.1304, -106.3468],
  NL: [52.1326, 5.2913],
  CH: [46.8182, 8.2275],
  SE: [60.1282, 18.6435],
  IL: [31.0461, 34.8516],
}

const DOMAIN_GROUPS: Record<string, string> = {
  quantum_computing: 'computing',
  quantum_fundamentals: 'fundamentals',
  quantum_education: 'education',
  quantum_policy_industry: 'policy',
  quantum_communication: 'qkd',
  quantum_materials: 'materials',
  quantum_sensing: 'sensing',
}

function normalizeConcept(value: string): string {
  return value.trim().toLowerCase().replace(/\s+/g, ' ')
}

publicRoutes.get('/stats/summary', async (c) => {
  try {
    const yearQuery = c.req.query('year')
    const year = yearQuery && /^\d{4}$/.test(yearQuery) ? Number(yearQuery) : null
    const platformQuery = c.req.query('platform')
    const platform = platformQuery && platformQuery !== 'all' ? platformQuery : null
    const domainQuery = c.req.query('domain')
    const domain = domainQuery && domainQuery !== 'all' ? domainQuery : null
    const searchQuery = c.req.query('q')?.trim() || null
    const searchPattern = searchQuery ? `%${searchQuery}%` : null
    const yearClause = year ? sql`s.published_year = ${year}` : sql`TRUE`
    const platformClause = platform ? sql`s.platform = ${platform}` : sql`TRUE`
    const domainClause = domain ? sql`s.quantum_domain = ${domain}` : sql`TRUE`
    const searchClause = searchPattern
      ? sql`(s.title ILIKE ${searchPattern} OR s.raw_text ILIKE ${searchPattern} OR s.url ILIKE ${searchPattern})`
      : sql`TRUE`
    const [stats] = await sql<
      {
        total_sources: number
        total_triplets: number
        min_year: number | null
        max_year: number | null
        real_geo: number
      }[]
    >`
      SELECT
        (SELECT COUNT(*)::int FROM sources s WHERE ${sql.unsafe(PUBLIC_SCOPE)} AND ${yearClause} AND ${platformClause} AND ${domainClause} AND ${searchClause}) AS total_sources,
        (SELECT COUNT(*)::int FROM triplets t JOIN sources s ON s.id = t.source_id WHERE ${sql.unsafe(PUBLIC_SCOPE)} AND ${yearClause} AND ${platformClause} AND ${domainClause} AND ${searchClause}) AS total_triplets,
        (SELECT MIN(published_year)::int FROM sources s WHERE ${sql.unsafe(PUBLIC_SCOPE)} AND ${yearClause} AND ${platformClause} AND ${domainClause} AND ${searchClause}) AS min_year,
        (SELECT MAX(published_year)::int FROM sources s WHERE ${sql.unsafe(PUBLIC_SCOPE)} AND ${yearClause} AND ${platformClause} AND ${domainClause} AND ${searchClause}) AS max_year,
        (
          SELECT COUNT(DISTINCT g.source_id)::int
          FROM geo g JOIN sources s ON g.source_id = s.id
          WHERE g.lat IS NOT NULL AND g.lng IS NOT NULL
            AND (g.is_cdn_resolved = 0 OR g.is_cdn_resolved IS NULL)
            AND ${sql.unsafe(PUBLIC_SCOPE)} AND ${yearClause} AND ${platformClause} AND ${domainClause} AND ${searchClause}
        ) AS real_geo
    `
    const platforms = await sql<{ platform: string; count: number }[]>`
      SELECT platform, COUNT(*)::int AS count
      FROM sources s
      WHERE ${sql.unsafe(PUBLIC_SCOPE)} AND ${yearClause} AND ${platformClause} AND ${domainClause} AND ${searchClause}
      GROUP BY platform
      ORDER BY count DESC
    `
    const approximate = await sql<{ count: number }[]>`
      SELECT COUNT(*)::int AS count
      FROM sources s
      WHERE s.channel_country IS NOT NULL
        AND ${sql.unsafe(PUBLIC_SCOPE)} AND ${yearClause} AND ${platformClause} AND ${domainClause} AND ${searchClause}
        AND s.id NOT IN (
          SELECT g2.source_id FROM geo g2
          WHERE g2.lat IS NOT NULL AND g2.lng IS NOT NULL
            AND (g2.is_cdn_resolved = 0 OR g2.is_cdn_resolved IS NULL)
        )
        AND s.channel_country = ANY(${sql.array(Object.keys(COUNTRY_CENTROIDS))})
    `
    const geoCount = (stats?.real_geo ?? 0) + (approximate[0]?.count ?? 0)
    return ok(c, {
      total_sources: stats?.total_sources ?? 0,
      total_triplets: stats?.total_triplets ?? 0,
      year_range: [stats?.min_year ?? 2016, stats?.max_year ?? 2026],
      geo_count: geoCount,
      geo_real_count: stats?.real_geo ?? 0,
      geo_approximate_count: approximate[0]?.count ?? 0,
      platforms,
    })
  } catch (error) {
    return fail(c, 500, 'summary_failed', error instanceof Error ? error.message : 'Failed to load summary')
  }
})

publicRoutes.get('/stats/yearly', async (c) => {
  try {
    const yearQuery = c.req.query('year')
    const year = yearQuery && /^\d{4}$/.test(yearQuery) ? Number(yearQuery) : null
    const platformQuery = c.req.query('platform')
    const platform = platformQuery && platformQuery !== 'all' ? platformQuery : null
    const domainQuery = c.req.query('domain')
    const domain = domainQuery && domainQuery !== 'all' ? domainQuery : null
    const searchQuery = c.req.query('q')?.trim() || null
    const searchPattern = searchQuery ? `%${searchQuery}%` : null
    const yearClause = year ? sql`s.published_year = ${year}` : sql`TRUE`
    const platformClause = platform ? sql`s.platform = ${platform}` : sql`TRUE`
    const domainClause = domain ? sql`s.quantum_domain = ${domain}` : sql`TRUE`
    const searchClause = searchPattern
      ? sql`(s.title ILIKE ${searchPattern} OR s.raw_text ILIKE ${searchPattern} OR s.url ILIKE ${searchPattern})`
      : sql`TRUE`
    const rows = await sql<{ published_year: number; platform: string; count: number }[]>`
      SELECT s.published_year, s.platform, COUNT(*)::int AS count
      FROM sources s
      WHERE ${sql.unsafe(PUBLIC_SCOPE)} AND ${platformClause} AND ${domainClause} AND ${searchClause}
      GROUP BY s.published_year, s.platform
      ORDER BY s.published_year
    `
    const years = [...new Set(rows.map((row) => String(row.published_year)))]
    const counts: Record<string, Record<string, number | string>> = {}
    for (const year of years) counts[year] = { total: 0 }
    for (const row of rows) {
      const bucket = counts[String(row.published_year)]
      if (!bucket) continue
      bucket[row.platform] = row.count
      bucket.total = Number(bucket.total) + row.count
    }
    return ok(c, {
      scope: 'thai_quantum_strict',
      years,
      counts,
    })
  } catch (error) {
    return fail(c, 500, 'yearly_stats_failed', error instanceof Error ? error.message : 'Failed to load yearly stats')
  }
})

publicRoutes.get('/corpus/coverage', async (c) => {
  try {
    const yearQuery = c.req.query('year')
    const year = yearQuery && /^\d{4}$/.test(yearQuery) ? Number(yearQuery) : null
    const platformQuery = c.req.query('platform')
    const platform = platformQuery && platformQuery !== 'all' ? platformQuery : null
    const domainQuery = c.req.query('domain')
    const domain = domainQuery && domainQuery !== 'all' ? domainQuery : null
    const searchQuery = c.req.query('q')?.trim() || null
    const searchPattern = searchQuery ? `%${searchQuery}%` : null
    const yearClause = year ? sql`s.published_year = ${year}` : sql`TRUE`
    const platformClause = platform ? sql`s.platform = ${platform}` : sql`TRUE`
    const domainClause = domain ? sql`s.quantum_domain = ${domain}` : sql`TRUE`
    const searchClause = searchPattern
      ? sql`(s.title ILIKE ${searchPattern} OR s.raw_text ILIKE ${searchPattern} OR s.url ILIKE ${searchPattern})`
      : sql`TRUE`
    const [totalRow] = await sql<{ total: number }[]>`
      SELECT COUNT(*)::int AS total FROM sources s WHERE ${sql.unsafe(PUBLIC_SCOPE)} AND ${yearClause} AND ${platformClause} AND ${domainClause} AND ${searchClause}
    `
    const byYear = await sql<{ published_year: number; platform: string; count: number }[]>`
      SELECT s.published_year, s.platform, COUNT(*)::int AS count
      FROM sources s
      WHERE ${sql.unsafe(PUBLIC_SCOPE)}
      GROUP BY s.published_year, s.platform
      ORDER BY s.published_year
    `
    const byDomain = await sql<{ domain: string | null; count: number }[]>`
      SELECT s.quantum_domain AS domain, COUNT(*)::int AS count
      FROM sources s
      WHERE s.quantum_domain IS NOT NULL AND ${sql.unsafe(PUBLIC_SCOPE)} AND ${yearClause} AND ${platformClause} AND ${domainClause} AND ${searchClause}
      GROUP BY s.quantum_domain
      ORDER BY count DESC
    `
    const yearsMap: Record<string, Record<string, number>> = {}
    for (const row of byYear) {
      const year = String(row.published_year)
      yearsMap[year] ??= {}
      yearsMap[year][row.platform] = row.count
    }
    return ok(c, {
      total: totalRow?.total ?? 0,
      by_year: yearsMap,
      by_domain: byDomain.map((row) => ({ domain: row.domain ?? 'unclassified', count: row.count })),
      years: Object.keys(yearsMap).sort(),
    })
  } catch (error) {
    return fail(c, 500, 'coverage_failed', error instanceof Error ? error.message : 'Failed to load coverage')
  }
})

publicRoutes.get('/sources', async (c) => {
  try {
    const page = intParam(c.req.query('page'), 1, 1, 100000)
    const pageSize = intParam(c.req.query('page_size'), 20, 1, 1000)
    const scope = c.req.query('include_filtered') === 'false' ? 'strict' : (c.req.query('relevance_scope') ?? 'strict')
    const conditions = [scopeClause(scope)]
    const params: any[] = []
    const add = (clause: (placeholder: string) => string, value: unknown) => {
      params.push(value)
      conditions.push(clause(`$${params.length}`))
    }

    const sourceId = c.req.query('source_id')
    if (sourceId) add((p) => `s.id = ${p}`, Number.parseInt(sourceId, 10))
    const year = c.req.query('year')
    if (year) add((p) => `s.published_year = ${p}`, Number.parseInt(year, 10))
    const platform = c.req.query('platform')
    if (platform) add((p) => `s.platform = ${p}`, platform)
    const domain = c.req.query('quantum_domain')
    if (domain) add((p) => `s.quantum_domain = ${p}`, domain)
    const search = c.req.query('search')?.trim()
    if (search) add((p) => `(s.title ILIKE ${p} OR s.raw_text ILIKE ${p} OR s.url ILIKE ${p})`, `%${search}%`)
    const contentType = c.req.query('content_type')
    if (contentType) add((p) => `e.content_type = ${p}`, contentType)

    const where = `WHERE ${conditions.join(' AND ')}`
    const offset = (page - 1) * pageSize
    const [totalRow] = await sql.unsafe<{ total: number }[]>(
      `SELECT COUNT(*)::int AS total FROM sources s LEFT JOIN entities e ON e.source_id = s.id ${where}`,
      params
    )
    const items = await sql.unsafe<SourceRow[]>(
      `SELECT s.*, e.content_type, e.production_type, e.area, e.engagement_level,
              e.media_format, e.user_intent,
              CASE WHEN EXISTS (
                SELECT 1 FROM geo g
                WHERE g.source_id = s.id AND g.lat IS NOT NULL
                  AND (g.is_cdn_resolved = 0 OR g.is_cdn_resolved IS NULL)
              ) THEN 1 ELSE 0 END AS has_real_geo
       FROM sources s
       LEFT JOIN entities e ON e.source_id = s.id
       ${where}
       ORDER BY s.published_year DESC, s.id DESC
       LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
      [...params, pageSize, offset]
    )
    return ok(
      c,
      {
        total: totalRow?.total ?? 0,
        page,
        page_size: pageSize,
        items,
      },
      { relevance: await relevanceMeta() }
    )
  } catch (error) {
    return fail(c, 500, 'sources_query_failed', error instanceof Error ? error.message : 'Failed to load sources')
  }
})

publicRoutes.get('/geo/list', async (c) => {
  try {
    const cdn = c.req.query('cdn') === 'true'
    const scope = c.req.query('scope') ?? 'strict'
    const clause = scopeClause(scope)
    const params: any[] = []
    void params

    const realRows = cdn
      ? await sql.unsafe<GeoRow[]>(`
          SELECT g.source_id, g.lat, g.lng, g.city, g.region, g.isp, g.asn_org, g.is_cdn_resolved,
                 s.platform, s.url, s.title, s.published_year, s.quantum_domain, s.fetched_at,
                 s.channel_title, s.channel_country, s.is_quantum_tech, s.is_thailand_related,
                 s.relevance_confidence, s.view_count, 0 AS is_approximate
          FROM geo g JOIN sources s ON g.source_id = s.id
          WHERE g.lat IS NOT NULL AND g.lng IS NOT NULL AND ${clause}
          ORDER BY s.published_year DESC, g.source_id DESC
          LIMIT 2000
        `)
      : await sql.unsafe<GeoRow[]>(`
          SELECT g.source_id, g.lat, g.lng, g.city, g.region, g.isp, g.asn_org, g.is_cdn_resolved,
                 s.platform, s.url, s.title, s.published_year, s.quantum_domain, s.fetched_at,
                 s.channel_title, s.channel_country, s.is_quantum_tech, s.is_thailand_related,
                 s.relevance_confidence, s.view_count, 0 AS is_approximate
          FROM geo g JOIN sources s ON g.source_id = s.id
          WHERE g.lat IS NOT NULL AND g.lng IS NOT NULL
            AND (g.is_cdn_resolved = 0 OR g.is_cdn_resolved IS NULL)
            AND ${clause}
          ORDER BY s.published_year DESC, g.source_id DESC
          LIMIT 2000
        `)

    const items: GeoRow[] = [...realRows]
    const realIds = new Set(realRows.map((row) => Number(row.source_id)))
    if (!cdn) {
      const candidates = await sql.unsafe<GeoRow[]>(`
        SELECT s.id AS source_id, s.channel_country, s.platform, s.url, s.title, s.published_year,
               s.quantum_domain, s.fetched_at, s.channel_title, s.is_quantum_tech,
               s.is_thailand_related, s.relevance_confidence, s.view_count
        FROM sources s
        WHERE s.channel_country IS NOT NULL AND ${clause}
          AND s.id NOT IN (
            SELECT g2.source_id FROM geo g2
            WHERE g2.lat IS NOT NULL AND g2.lng IS NOT NULL
              AND (g2.is_cdn_resolved = 0 OR g2.is_cdn_resolved IS NULL)
          )
        ORDER BY s.published_year DESC, s.id DESC
        LIMIT 2000
      `)
      for (const candidate of candidates) {
        const country = String(candidate.channel_country ?? '')
        const centroid = COUNTRY_CENTROIDS[country]
        const id = Number(candidate.source_id)
        if (!centroid || realIds.has(id)) continue
        items.push({
          ...candidate,
          lat: centroid[0],
          lng: centroid[1],
          city: null,
          region: null,
          isp: null,
          asn_org: null,
          is_cdn_resolved: null,
          is_approximate: 1,
        })
      }
    }
    return ok(
      c,
      items,
      {
        count: items.length,
        real_count: realIds.size,
        approximate_count: items.length - realIds.size,
        relevance: await relevanceMeta(),
      }
    )
  } catch (error) {
    return fail(c, 500, 'geo_list_failed', error instanceof Error ? error.message : 'Failed to load geo data')
  }
})

publicRoutes.get('/graph', async (c) => {
  try {
    const includeFiltered = c.req.query('include_filtered') !== 'false'
    const where = includeFiltered ? '1=1' : PUBLIC_SCOPE
    const rows = await sql.unsafe<
      { subject: string; relation: string; object: string; confidence: number; quantum_domain: string | null }[]
    >(`
      SELECT t.subject, t.relation, t.object, t.confidence, s.quantum_domain
      FROM triplets t
      JOIN sources s ON t.source_id = s.id
      WHERE ${where}
      ORDER BY t.id
      LIMIT 10000
    `)

    const labels = new Map<string, string>()
    const degree = new Map<string, number>()
    const domainCounts = new Map<string, Map<string, number>>()
    const edges = new Map<string, { relation: string; count: number }>()

    for (const row of rows) {
      const subject = (row.subject ?? '').trim()
      const object = (row.object ?? '').trim()
      const sourceKey = normalizeConcept(subject)
      const targetKey = normalizeConcept(object)
      if (sourceKey.length < 2 || targetKey.length < 2 || sourceKey === targetKey) continue
      if (!labels.has(sourceKey)) labels.set(sourceKey, subject)
      if (!labels.has(targetKey)) labels.set(targetKey, object)
      degree.set(sourceKey, (degree.get(sourceKey) ?? 0) + 1)
      degree.set(targetKey, (degree.get(targetKey) ?? 0) + 1)

      const group = row.quantum_domain ? DOMAIN_GROUPS[row.quantum_domain] : undefined
      if (group) {
        for (const key of [sourceKey, targetKey]) {
          const counts = domainCounts.get(key) ?? new Map<string, number>()
          counts.set(group, (counts.get(group) ?? 0) + 1)
          domainCounts.set(key, counts)
        }
      }
      const edgeKey = `${sourceKey}\u0000${targetKey}`
      const current = edges.get(edgeKey)
      if (current) current.count += 1
      else edges.set(edgeKey, { relation: row.relation ?? '', count: 1 })
    }

    const nodes = [...labels.entries()].map(([id, label]) => {
      const groups = domainCounts.get(id)
      const group = groups ? [...groups.entries()].sort((a, b) => b[1] - a[1])[0]?.[0] : undefined
      return { id, label, val: Math.max(1, degree.get(id) ?? 1), ...(group ? { group, category: group } : {}) }
    })
    const links = [...edges.entries()].map(([key, value]) => {
      const [source, target] = key.split('\u0000') as [string, string]
      return { source, target, label: value.relation, value: value.count }
    })
    const response = ok(c, { nodes, links }, { relevance: await relevanceMeta() })
    response.headers.set('Cache-Control', 'public, max-age=3600')
    return response
  } catch (error) {
    return fail(c, 500, 'graph_load_failed', error instanceof Error ? error.message : 'Failed to load graph')
  }
})
