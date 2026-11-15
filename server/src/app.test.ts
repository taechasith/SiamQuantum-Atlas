import { afterAll, beforeAll, describe, expect, it } from 'bun:test'
import { Database } from 'bun:sqlite'
import { app } from './app'
import { setLocalSqlExecutor } from './db/client'
import { createBunSqliteExecutor, ensureSqliteSchema } from './db/sqlite'

const database = new Database(':memory:')

beforeAll(() => {
  ensureSqliteSchema(database)
  setLocalSqlExecutor(createBunSqliteExecutor(database))
})

afterAll(() => {
  setLocalSqlExecutor(undefined)
  database.close()
})

describe('SiamQuantum Hono API', () => {
  it('responds to health checks', async () => {
    const response = await app.request('/healthz')
    expect(response.status).toBe(200)
    const body = (await response.json()) as any
    expect(body.ok).toBe(true)
    expect(body.data.runtime).toBe('bun')
  })

  it('returns null auth state for guests', async () => {
    const response = await app.request('/api/auth/me')
    expect(response.status).toBe(200)
    const body = (await response.json()) as any
    expect(body.ok).toBe(true)
    expect(body.data.user).toBeNull()
  })

  it('rejects invalid local credentials', async () => {
    const response = await app.request('/api/auth/local/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: 'nobody@example.com', password: 'wrong-password' }),
    })
    expect(response.status).toBe(401)
  })

  it('serves the core public corpus endpoints', async () => {
    for (const path of ['/api/stats/summary', '/api/stats/yearly', '/api/corpus/coverage', '/api/sources', '/api/geo/list', '/api/graph']) {
      const response = await app.request(path)
      expect(response.status).toBe(200)
      const body = (await response.json()) as any
      expect(body.ok).toBe(true)
    }
  })

  it('returns a JSON 404 for unknown API routes', async () => {
    const response = await app.request('/api/does-not-exist')
    expect(response.status).toBe(404)
    const body = (await response.json()) as any
    expect(body.ok).toBe(false)
    expect(body.error.code).toBe('not_found')
  })
})
