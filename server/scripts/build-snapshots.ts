import { mkdirSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { app } from '../src/app'
import { setLocalSqlExecutor } from '../src/db/client'
import { createBunSqliteExecutor, ensureSqliteSchema, openSqlite } from '../src/db/sqlite'

const sqlitePath =
  process.env.SQLITE_PATH ??
  new URL('../../data/processed/siamquantum_atlas.db', import.meta.url).pathname
const outputDir = new URL('../../frontend/public/data', import.meta.url).pathname

const database = openSqlite(sqlitePath)
ensureSqliteSchema(database)
setLocalSqlExecutor(createBunSqliteExecutor(database))

async function fetchData(path: string): Promise<unknown> {
  const response = await app.fetch(new Request(`http://localhost${path}`))
  if (!response.ok) {
    throw new Error(`${path} failed: ${response.status} ${await response.text()}`)
  }
  const payload = (await response.json()) as { data?: unknown }
  return payload.data
}

async function writeSnapshot(name: string, path: string): Promise<void> {
  const data = await fetchData(path)
  const target = join(outputDir, name)
  writeFileSync(target, `${JSON.stringify({ ok: true, data, error: null }, null, 2)}\n`)
  console.log(`Wrote ${target}`)
}

mkdirSync(outputDir, { recursive: true })

try {
  await writeSnapshot('stats-summary.json', '/api/stats/summary')
  await writeSnapshot('stats-yearly.json', '/api/stats/yearly')
  await writeSnapshot('corpus-coverage.json', '/api/corpus/coverage')
  await writeSnapshot('sources.json', '/api/sources?page=1&page_size=1000')
  await writeSnapshot('geo-strict.json', '/api/geo/list?scope=strict')
  await writeSnapshot('geo-th.json', '/api/geo/list?scope=th')
  await writeSnapshot('geo-quantum.json', '/api/geo/list?scope=quantum')
  await writeSnapshot('geo-all.json', '/api/geo/list?scope=all')
  await writeSnapshot('graph.json', '/api/graph')
} finally {
  database.close()
}
