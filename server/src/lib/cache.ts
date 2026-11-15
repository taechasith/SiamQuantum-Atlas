import type { MiddlewareHandler } from 'hono'
import type { AppEnv } from '../db/client'

interface CacheStorageLike {
  match(request: unknown): Promise<Response | undefined>
  put(request: unknown, response: unknown): Promise<void>
}

type CacheGlobal = {
  caches?: {
    default?: CacheStorageLike
  }
}

function getCache(): CacheStorageLike | undefined {
  return (globalThis as CacheGlobal).caches?.default
}

function schedule(c: Parameters<MiddlewareHandler<AppEnv>>[0], promise: Promise<unknown>) {
  try {
    c.executionCtx.waitUntil(promise)
  } catch {
    void promise
  }
}

export function cacheResponse(ttl: number, staleWhileRevalidate = ttl): MiddlewareHandler<AppEnv> {
  return async (c, next) => {
    const cache = getCache()
    const cacheControl = `public, max-age=${ttl}, s-maxage=${ttl}, stale-while-revalidate=${staleWhileRevalidate}`

    if (!cache) {
      await next()
      c.header('Cache-Control', cacheControl)
      return
    }

    const key = new Request(c.req.url, { method: 'GET' })
    const hit = await cache.match(key)
    if (hit) {
      const headers = new Headers(hit.headers)
      headers.set('X-Cache', 'HIT')
      return new Response(await hit.arrayBuffer(), { status: hit.status, headers }) as unknown as Response
    }

    await next()

    if (!c.res.ok) return

    c.res.headers.set('Cache-Control', cacheControl)
    c.res.headers.set('X-Cache', 'MISS')

    schedule(c, cache.put(key, c.res.clone()).catch(() => undefined))
  }
}
