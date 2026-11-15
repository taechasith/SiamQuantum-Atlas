import { Database } from 'bun:sqlite'
import { mkdirSync, readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import type { Executor, Row } from './client'

export function createBunSqliteExecutor(db: Database): Executor {
  return async (query, params) => {
    const statement = db.prepare(query)
    const normalized = query.trimStart().toLowerCase()
    const returnsRows =
      normalized.startsWith('select') ||
      normalized.startsWith('with') ||
      normalized.startsWith('pragma') ||
      normalized.includes(' returning ')

    if (returnsRows) {
      return statement.all(...(params as never[])) as Row[]
    }

    statement.run(...(params as never[]))
    return []
  }
}

function hasColumn(db: Database, table: string, column: string): boolean {
  const columns = db.query(`PRAGMA table_info(${table})`).all() as { name: string }[]
  return columns.some((entry) => entry.name === column)
}

export function ensureSqliteSchema(db: Database): void {
  const hasSources = db
    .query("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'sources'")
    .get()

  if (!hasSources) {
    const schema = readFileSync(
      join(import.meta.dir, '..', '..', 'migrations', 'd1', '0001_schema.sql'),
      'utf8'
    )
    db.exec(schema)
  }

  const columns: Array<[string, string, string]> = [
    ['local_users', 'organization', 'TEXT'],
    ['local_users', 'focus', 'TEXT'],
    ['local_users', 'citation', 'TEXT'],
    ['local_submitted_data', 'province_code', 'TEXT'],
  ]

  for (const [table, column, type] of columns) {
    if (!hasColumn(db, table, column)) {
      db.run(`ALTER TABLE ${table} ADD COLUMN ${column} ${type}`)
    }
  }

  db.exec('PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL;')
}

export function openSqlite(path: string): Database {
  mkdirSync(dirname(path), { recursive: true })
  return new Database(path, { create: true })
}
