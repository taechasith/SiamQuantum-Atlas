import { Database } from 'bun:sqlite'
import { join } from 'node:path'
import postgres from 'postgres'

const sqlitePath = process.env.SQLITE_PATH ?? join(import.meta.dir, '..', '..', 'data', 'processed', 'siamquantum_atlas.db')
const databaseUrl = process.env.DATABASE_URL ?? 'postgres://siamquantum:changeme-dev-password@127.0.0.1:5432/siamquantum_atlas'
const sqlite = new Database(sqlitePath, { readonly: true })
const sql = postgres(databaseUrl, { max: 1, onnotice: () => {} })

const tableOrder = [
  'local_users',
  'sources',
  'geo',
  'entities',
  'triplets',
  'stats_cache',
  'community_submissions',
  'denstream_state',
  'nlp_abstentions',
  'pipeline_meta',
  'local_categories',
  'local_submitted_data',
  'admin_audit_log',
]

const jsonColumns = new Set(['value', 'metadata', 'details', 'analysis_result', 'source_meta'])
const byteColumns = new Set(['snapshot'])
const newUserColumns = ['organization', 'focus', 'citation']

function columnsFor(table: string): string[] {
  return (sqlite.query(`PRAGMA table_info(${table})`).all() as { name: string }[]).map((row) => row.name)
}

async function importTable(table: string): Promise<number> {
  const exists = sqlite.query("SELECT name FROM sqlite_master WHERE type='table' AND name = ?").get(table)
  if (!exists) {
    console.log(`skip ${table} (not in sqlite)`)
    return 0
  }

  const sourceColumns = columnsFor(table)
  const columns = table === 'local_users' ? [...sourceColumns, ...newUserColumns] : sourceColumns
  const rows = sqlite.query(`SELECT * FROM ${table}`).all() as Record<string, unknown>[]
  if (rows.length === 0) {
    console.log(`skip ${table} (empty)`)
    return 0
  }

  const placeholders = columns.map((_, index) => `$${index + 1}`).join(', ')
  const query = `INSERT INTO ${table} (${columns.join(', ')}) VALUES (${placeholders}) ON CONFLICT DO NOTHING`
  let imported = 0

  await sql.begin(async (tx) => {
    for (const raw of rows) {
      const values: any[] = columns.map((column) => {
        if (newUserColumns.includes(column)) return null
        let value = raw[column]
        if (value === undefined) value = null
        if (byteColumns.has(column) && value instanceof Uint8Array) return Buffer.from(value)
        if (jsonColumns.has(column) && value !== null && typeof value !== 'string') {
          return JSON.stringify(value)
        }
        if (typeof value === 'string') return value.replace(/\u0000/g, '')
        return value
      })
      await tx.unsafe(query, values)
      imported += 1
    }
  })

  console.log(`imported ${table}: ${imported}`)
  return imported
}

async function resetSequences() {
  const tables = [
    'sources',
    'triplets',
    'community_submissions',
    'local_categories',
    'local_submitted_data',
    'admin_audit_log',
  ]
  for (const table of tables) {
    await sql.unsafe(`
      SELECT setval(
        pg_get_serial_sequence('${table}', 'id'),
        COALESCE((SELECT MAX(id) FROM ${table}), 1),
        EXISTS (SELECT 1 FROM ${table})
      )
    `)
  }
}

async function main() {
  for (const table of tableOrder) {
    await importTable(table)
  }
  await resetSequences()
  const counts = await sql`
    SELECT
      (SELECT COUNT(*)::int FROM sources) AS sources,
      (SELECT COUNT(*)::int FROM triplets) AS triplets,
      (SELECT COUNT(*)::int FROM local_users) AS users,
      (SELECT COUNT(*)::int FROM geo) AS geo
  `
  console.log('postgres counts', counts[0])
}

main()
  .catch((error) => {
    console.error(error)
    process.exitCode = 1
  })
  .finally(async () => {
    sqlite.close()
    await sql.end()
  })
