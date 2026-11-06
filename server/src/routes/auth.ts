import { Hono } from 'hono'
import { z } from 'zod'
import { sql } from '../db/client'
import {
  clearSessionCookie,
  createSession,
  currentUser,
  hashPassword,
  newSalt,
  newUserId,
  profilePayload,
  requireUser,
  setSessionCookie,
  verifyPassword,
  type LocalUser,
} from '../lib/auth'
import { fail, nowIso, ok } from '../lib/http'

export const authRoutes = new Hono()

const credentialsSchema = z.object({
  email: z.string().trim().email(),
  password: z.string().min(6),
})

const profilePatchSchema = z.object({
  display_name: z.string().trim().max(160).optional().nullable(),
  bio: z.string().trim().max(1000).optional().nullable(),
  website_url: z.string().trim().max(500).optional().nullable(),
  avatar_url: z.string().trim().max(1000).optional().nullable(),
  organization: z.string().trim().max(200).optional().nullable(),
  focus: z.string().trim().max(200).optional().nullable(),
  citation: z.string().trim().max(300).optional().nullable(),
})

function userResponse(user: LocalUser) {
  return {
    user: {
      id: user.id,
      email: user.email,
      created_at: user.created_at,
      display_name: user.display_name,
      avatar_url: user.avatar_url,
      user_metadata: {},
    },
    profile: profilePayload(user),
  }
}

authRoutes.get('/auth/config', (c) =>
  ok(c, {
    enabled: false,
    auth_mode: 'local',
    local_mode: true,
    google_oauth_available: false,
    supabase_url: '',
    supabase_publishable_key: '',
  })
)

authRoutes.post('/auth/local/register', async (c) => {
  try {
    const parsed = credentialsSchema.safeParse(await c.req.json())
    if (!parsed.success) {
      return fail(c, 422, 'validation_failed', 'A valid email and password of at least 6 characters are required.')
    }
    const email = parsed.data.email.toLowerCase()
    const [existing] = await sql<{ id: string }[]>`SELECT id FROM local_users WHERE email = ${email} LIMIT 1`
    if (existing) return fail(c, 409, 'email_exists', 'An account with this email already exists.')

    const [countRow] = await sql<{ count: number }[]>`SELECT COUNT(*)::int AS count FROM local_users`
    const role = (countRow?.count ?? 0) === 0 ? 'admin' : 'user'
    const id = newUserId()
    const salt = newSalt()
    const now = nowIso()
    const displayName = email.split('@')[0] ?? 'Researcher'
    const [user] = await sql<LocalUser[]>`
      INSERT INTO local_users
        (id, email, password_salt, password_hash, display_name, role, created_at, updated_at)
      VALUES
        (${id}, ${email}, ${salt}, ${hashPassword(parsed.data.password, salt)}, ${displayName}, ${role}, ${now}, ${now})
      RETURNING *
    `
    if (!user) return fail(c, 500, 'register_failed', 'Failed to create the account.')
    const token = await createSession(user.id)
    setSessionCookie(c, token)
    return c.json({ ok: true, data: userResponse(user), error: null }, 201)
  } catch (error) {
    return fail(c, 500, 'register_failed', error instanceof Error ? error.message : 'Failed to register')
  }
})

authRoutes.post('/auth/local/login', async (c) => {
  try {
    const parsed = credentialsSchema.safeParse(await c.req.json())
    if (!parsed.success) return fail(c, 401, 'auth_invalid', 'Invalid email or password.')
    const [user] = await sql<LocalUser[]>`
      SELECT * FROM local_users WHERE email = ${parsed.data.email.toLowerCase()} LIMIT 1
    `
    if (!user || !user.password_hash || !user.password_salt || !verifyPassword(parsed.data.password, user)) {
      return fail(c, 401, 'auth_invalid', 'Invalid email or password.')
    }
    const token = await createSession(user.id)
    setSessionCookie(c, token)
    return ok(c, userResponse(user))
  } catch (error) {
    return fail(c, 500, 'login_failed', error instanceof Error ? error.message : 'Failed to login')
  }
})

authRoutes.post('/auth/local/logout', async (c) => {
  const token = c.req.header('cookie')?.match(/sq_local_session=([^;]+)/)?.[1]
  if (token) await sql`DELETE FROM local_sessions WHERE token = ${decodeURIComponent(token)}`
  clearSessionCookie(c)
  return ok(c, { logged_out: true })
})

authRoutes.get('/auth/me', async (c) => {
  const user = await currentUser(c)
  if (!user) return ok(c, { user: null, profile: null })
  return ok(c, userResponse(user))
})

authRoutes.post('/auth/sync-profile', async (c) => {
  const user = await requireUser(c)
  if (user instanceof Response) return user
  return ok(c, { profile: profilePayload(user) })
})

authRoutes.get('/profile', async (c) => {
  const user = await requireUser(c)
  if (user instanceof Response) return user
  return ok(c, { profile: profilePayload(user) })
})

authRoutes.patch('/profile', async (c) => {
  const user = await requireUser(c)
  if (user instanceof Response) return user
  const parsed = profilePatchSchema.safeParse(await c.req.json().catch(() => ({})))
  if (!parsed.success) return fail(c, 422, 'profile_invalid', 'Invalid profile fields.')
  const patch = parsed.data
  const entries = Object.entries(patch).filter(([, value]) => value !== undefined)
  if (entries.length === 0) return fail(c, 422, 'profile_empty_update', 'No profile fields were provided.')

  const assignments = entries.map(([key], index) => `${key} = $${index + 1}`)
  const values = entries.map(([, value]) => (typeof value === 'string' ? value.trim() || null : value))
  const [updated] = await sql.unsafe<LocalUser[]>(
    `UPDATE local_users SET ${assignments.join(', ')}, updated_at = $${values.length + 1} WHERE id = $${values.length + 2} RETURNING *`,
    [...values, nowIso(), user.id]
  )
  if (!updated) return fail(c, 500, 'profile_update_failed', 'Failed to update profile.')
  return ok(c, { profile: profilePayload(updated) })
})
