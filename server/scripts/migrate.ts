import { readdir } from 'node:fs/promises'
import { join } from 'node:path'
import postgres from 'postgres'

const url = process.env.DATABASE_URL ?? 'postgres://siamquantum:changeme-dev-password@127.0.0.1:5432/siamquantum_atlas'
const sql = postgres(url, { max: 1, onnotice: () => {} })

async function main() {
  const dir = join(import.meta.dir, '..', 'migrations')
  const files = (await readdir(dir)).filter((file) => file.endsWith('.sql')).sort()
  await sql`CREATE TABLE IF NOT EXISTS schema_migrations (filename TEXT PRIMARY KEY, applied_at TEXT NOT NULL DEFAULT (now()::text))`
  const appliedRows = await sql<{ filename: string }[]>`SELECT filename FROM schema_migrations`
  const applied = new Set(appliedRows.map((row) => row.filename))

  for (const file of files) {
    if (applied.has(file)) {
      console.log(`skip ${file}`)
      continue
    }
    const body = await Bun.file(join(dir, file)).text()
    console.log(`apply ${file}`)
    await sql.begin(async (tx) => {
      await tx.unsafe(body)
      await tx`INSERT INTO schema_migrations (filename) VALUES (${file})`
    })
  }
  await sql.end()
}

main().catch(async (error) => {
  console.error(error)
  await sql.end()
  process.exit(1)
})
