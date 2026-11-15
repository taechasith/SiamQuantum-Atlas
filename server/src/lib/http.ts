import type { Context } from 'hono'

export type ApiError = {
  code: string
  message: string
  details?: unknown
}

export function ok<T>(c: Context, data: T, extra: Record<string, unknown> = {}) {
  return c.json({ ok: true, data, error: null, ...extra })
}

export function fail(
  c: Context,
  status: 400 | 401 | 403 | 404 | 409 | 422 | 500 | 501 | 503,
  code: string,
  message: string,
  details?: unknown
) {
  return c.json(
    {
      ok: false,
      data: null,
      error: { code, message, ...(details === undefined ? {} : { details }) },
    },
    status
  )
}

export function intParam(value: string | undefined, fallback: number, min: number, max: number): number {
  const parsed = Number.parseInt(value ?? '', 10)
  if (!Number.isFinite(parsed)) return fallback
  return Math.min(max, Math.max(min, parsed))
}

export function nowIso(): string {
  return new Date().toISOString()
}

export function safeJsonParse<T>(value: unknown, fallback: T): T {
  if (typeof value !== 'string') return (value as T) ?? fallback
  try {
    return JSON.parse(value) as T
  } catch {
    return fallback
  }
}
