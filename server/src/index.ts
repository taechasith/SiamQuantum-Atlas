import { app } from './app'
import { setLocalSqlExecutor } from './db/client'
import { createBunSqliteExecutor, ensureSqliteSchema, openSqlite } from './db/sqlite'

const port = Number(process.env.PORT ?? 8766)
const sqlitePath =
  process.env.SQLITE_PATH ??
  new URL('../../data/processed/siamquantum_atlas.db', import.meta.url).pathname

const database = openSqlite(sqlitePath)
ensureSqliteSchema(database)
setLocalSqlExecutor(createBunSqliteExecutor(database))

const server = Bun.serve({
  port,
  fetch: app.fetch,
  idleTimeout: 30,
})

console.log(`SiamQuantum Hono API listening on http://127.0.0.1:${server.port}`)
console.log(`SQLite: ${sqlitePath}`)
