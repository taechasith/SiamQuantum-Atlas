import { createHash, pbkdf2Sync, randomBytes, timingSafeEqual } from 'node:crypto'
import { deleteCookie, getCookie, setCookie } from 'hono/cookie'
import { getSql, type AppContext } from '../db/client'
import { fail, nowIso } from './http'

export const SESSION_COOKIE = 'sq_local_session'
const SESSION_DAYS = 30
const PBKDF2_ITERATIONS = 120_000

export interface LocalUser {
  id: string
  email: string
  password_salt: string
  password_hash: string
  display_name: string | null
  avatar_url: string | null
  bio: string | null
  website_url: string | null
  organization: string | null
  focus: string | null
  citation: string | null
  role: string
  created_at: string
  updated_at: string
}

export function hashPassword(password: string, salt: string): string {
  return pbkdf2Sync(password, salt, PBKDF2_ITERATIONS, 32, 'sha256').toString('hex')
}

export function verifyPassword(password: string, user: LocalUser): boolean {
  const expected = hashPassword(password, user.password_salt)
  const a = new Uint8Array(Buffer.from(expected, 'utf8'))
  const b = new Uint8Array(Buffer.from(user.password_hash, 'utf8'))
  return a.length === b.length && timingSafeEqual(a, b)
}

export function newSalt(): string {
  return randomBytes(16).toString('hex')
}

export function newUserId(): string {
  return randomBytes(16).toString('hex')
}

export function newSessionToken(): string {
  return randomBytes(32).toString('base64url')
}

export function profilePayload(user: LocalUser) {
  return {
    id: user.id,
    email: user.email,
    display_name: user.display_name,
    avatar_url: user.avatar_url,
    bio: user.bio,
    website_url: user.website_url,
    organization: user.organization,
    focus: user.focus,
    citation: user.citation,
    role: user.role || 'user',
    created_at: user.created_at,
    updated_at: user.updated_at,
  }
}

export async function currentUser(c: AppContext): Promise<LocalUser | null> {
  const sql = getSql(c)
  const token = getCookie(c, SESSION_COOKIE)
  if (!token) return null
  const rows = await sql<LocalUser[]>`
    SELECT u.*
    FROM local_sessions s
    JOIN local_users u ON u.id = s.user_id
    WHERE s.token = ${token}
      AND (s.expires_at IS NULL OR s.expires_at > ${new Date().toISOString()})
    LIMIT 1
  `
  return rows[0] ?? null
}

export async function requireUser(c: AppContext): Promise<LocalUser | Response> {
  const user = await currentUser(c)
  if (!user) return fail(c, 401, 'auth_required', 'Login is required for this action.')
  return user
}

export async function requireAdmin(c: AppContext): Promise<LocalUser | Response> {
  const user = await currentUser(c)
  if (!user) return fail(c, 401, 'auth_required', 'Login is required for this action.')
  if (user.role !== 'admin') return fail(c, 403, 'admin_required', 'Admin access is required.')
  return user
}

export function setSessionCookie(c: AppContext, token: string): void {
  const secure =
    c.env?.SESSION_COOKIE_SECURE === 'true' ||
    c.env?.NODE_ENV === 'production' ||
    new URL(c.req.url).protocol === 'https:'
  setCookie(c, SESSION_COOKIE, token, {
    httpOnly: true,
    sameSite: 'Lax',
    secure,
    path: '/',
    maxAge: SESSION_DAYS * 24 * 60 * 60,
  })
}

export function clearSessionCookie(c: AppContext): void {
  deleteCookie(c, SESSION_COOKIE, { path: '/' })
}

export async function createSession(c: AppContext, userId: string): Promise<string> {
  const sql = getSql(c)
  const token = newSessionToken()
  const created = nowIso()
  const expires = new Date(Date.now() + SESSION_DAYS * 24 * 60 * 60 * 1000).toISOString()
  await sql`
    INSERT INTO local_sessions (token, user_id, created_at, expires_at)
    VALUES (${token}, ${userId}, ${created}, ${expires})
  `
  return token
}

export function legacySessionHash(token: string): string {
  return createHash('sha256').update(token).digest('hex')
}
