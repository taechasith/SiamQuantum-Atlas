import postgres from 'postgres'

const connectionString =
  process.env.DATABASE_URL ??
  'postgres://siamquantum:changeme-dev-password@127.0.0.1:5432/siamquantum_atlas'

export const sql = postgres(connectionString, {
  max: Number(process.env.DATABASE_POOL_MAX ?? 10),
  idle_timeout: 20,
  connect_timeout: 10,
  onnotice: () => {},
  types: {
    bigint: {
      to: 20,
      from: [20],
      serialize: (value: string | number | bigint) => String(value),
      parse: (value: string) => Number(value),
    },
  },
})

export const PUBLIC_SCOPE = `s.is_quantum_tech = 1
  AND s.is_thailand_related = 1
  AND (s.relevance_confidence IS NULL OR s.relevance_confidence >= 0.65)`

export const SCOPE_BY_NAME: Record<string, string> = {
  strict: PUBLIC_SCOPE,
  quantum: 's.is_quantum_tech = 1',
  thailand: 's.is_thailand_related = 1',
  all: '1=1',
}

export function scopeClause(scope: string | undefined): string {
  return SCOPE_BY_NAME[scope ?? 'strict'] ?? PUBLIC_SCOPE
}

export async function relevanceMeta() {
  const [row] = await sql<{ total: number; checked: number }[]>`
    SELECT
      COUNT(*)::int AS total,
      COUNT(*) FILTER (WHERE relevance_checked_at IS NOT NULL)::int AS checked
    FROM sources
  `
  return {
    mode: 'local',
    checked_sources: row?.checked ?? 0,
    total_sources: row?.total ?? 0,
    note: 'Local Hono API against the imported SiamQuantum corpus.',
  }
}
