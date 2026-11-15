import type { Context } from 'hono'

export interface D1PreparedStatement {
  bind(...values: unknown[]): D1PreparedStatement
  all<T = Record<string, unknown>>(): Promise<{ results?: T[] }>
}

export interface D1Database {
  prepare(query: string): D1PreparedStatement
}

export interface R2BucketLike {
  put(
    key: string,
    value: Blob | ArrayBuffer | Uint8Array | ReadableStream,
    options?: { httpMetadata?: { contentType?: string } }
  ): Promise<unknown>
  get(key: string): Promise<{
    body: ReadableStream
    httpMetadata?: { contentType?: string }
  } | null>
  delete(key: string): Promise<void>
}

export interface AppBindings {
  DB?: D1Database
  STORAGE?: R2BucketLike
  SESSION_COOKIE_SECURE?: string
  NODE_ENV?: string
  R2_ACCOUNT_ID?: string
  R2_ACCESS_KEY_ID?: string
  R2_SECRET_ACCESS_KEY?: string
  R2_BUCKET?: string
}

export type AppEnv = {
  Bindings: AppBindings
  Variables: {
    sql: SqlClient
  }
}

export type AppContext = Context<AppEnv>
export type Row = Record<string, unknown>
export type Executor = (query: string, params: unknown[]) => Promise<Row[]>

export class SqlQuery<T = Row[]> implements PromiseLike<T> {
  constructor(
    readonly text: string,
    readonly params: unknown[],
    private readonly executor: Executor
  ) {}

  then<TResult1 = T, TResult2 = never>(
    onfulfilled?: ((value: T) => TResult1 | PromiseLike<TResult1>) | null,
    onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
  ): Promise<TResult1 | TResult2> {
    return this.executor(this.text, this.params).then(
      (rows) => (onfulfilled ? onfulfilled(rows as T) : (rows as unknown as TResult1)),
      onrejected
    )
  }

  catch<TResult = never>(
    onrejected?: ((reason: unknown) => TResult | PromiseLike<TResult>) | null
  ): Promise<T | TResult> {
    return this.executor(this.text, this.params).catch(onrejected) as Promise<T | TResult>
  }

  finally(onfinally?: (() => void) | null): Promise<T> {
    return this.executor(this.text, this.params).finally(onfinally ?? undefined) as Promise<T>
  }
}

export interface SqlClient {
  <T = Row[]>(strings: TemplateStringsArray, ...values: unknown[]): SqlQuery<T>
  unsafe<T = Row[]>(query: string, params?: unknown[]): SqlQuery<T>
}

function buildQuery(
  strings: TemplateStringsArray,
  values: unknown[],
  executor: Executor
): SqlQuery {
  let text = ''
  const params: unknown[] = []

  strings.forEach((part, index) => {
    text += part
    if (index >= values.length) return

    const value = values[index]
    if (value instanceof SqlQuery) {
      text += value.text
      params.push(...value.params)
      return
    }

    text += '?'
    params.push(value === undefined ? null : value)
  })

  return new SqlQuery(text, params, executor)
}

function normalizeUnsafe(query: string, params: unknown[] = []): { text: string; params: unknown[] } {
  const ordered: unknown[] = []
  const text = query.replace(/\$(\d+)/g, (_match, position: string) => {
    ordered.push(params[Number(position) - 1])
    return '?'
  })

  return {
    text,
    params: ordered.length ? ordered : params,
  }
}

export function createSql(executor: Executor): SqlClient {
  const sql = (<T = Row[]>(strings: TemplateStringsArray, ...values: unknown[]) =>
    buildQuery(strings, values, executor) as SqlQuery<T>) as SqlClient

  sql.unsafe = <T = Row[]>(query: string, params: unknown[] = []) => {
    const normalized = normalizeUnsafe(query, params)
    return new SqlQuery<T>(normalized.text, normalized.params, executor)
  }

  return sql
}

function createD1Executor(db: D1Database): Executor {
  return async (query, params) => {
    const statement = db.prepare(query).bind(...params)
    const result = await statement.all<Row>()
    return result.results ?? []
  }
}

let localExecutor: Executor | undefined

export function setLocalSqlExecutor(executor: Executor | undefined): void {
  localExecutor = executor
}

export function getSql(c: AppContext): SqlClient {
  const existing = c.get('sql') as SqlClient | undefined
  if (existing) return existing

  const env = c.env ?? {}
  const executor = env.DB ? createD1Executor(env.DB) : localExecutor

  if (!executor) {
    throw new Error('No database configured. Bind D1 as DB or configure the local SQLite executor.')
  }

  const client = createSql(executor)
  c.set('sql', client)
  return client
}

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

export async function relevanceMeta(sql: SqlClient) {
  const [row] = await sql<{ total: number; checked: number }[]>`
    SELECT
      CAST(COUNT(*) AS INTEGER) AS total,
      CAST(SUM(CASE WHEN relevance_checked_at IS NOT NULL THEN 1 ELSE 0 END) AS INTEGER) AS checked
    FROM sources
  `
  return {
    mode: 'local',
    checked_sources: row?.checked ?? 0,
    total_sources: row?.total ?? 0,
    note: 'SiamQuantum corpus served from SQLite or D1.',
  }
}
