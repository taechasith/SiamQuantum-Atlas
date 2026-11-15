import { Hono } from 'hono'
import { z } from 'zod'
import { getSql, type AppEnv, type SqlClient } from '../db/client'
import { profilePayload, requireAdmin, requireUser, type LocalUser } from '../lib/auth'
import { THAI_PROVINCE_SET } from '../lib/provinces'
import { fail, intParam, nowIso, ok, safeJsonParse } from '../lib/http'

export const submissionRoutes = new Hono<AppEnv>()

type SubmissionRow = {
  id: number
  user_id: string
  title: string
  description: string | null
  source_url: string | null
  category: string
  page_target: string | null
  status: string
  analysis_status: string
  analysis_result: string | null
  metadata: string | null
  province_code: string | null
  created_at: string
  updated_at: string
}

type AdminUserRow = Pick<
  LocalUser,
  'id' | 'email' | 'display_name' | 'role' | 'created_at' | 'organization' | 'focus'
>

const submitSchema = z.object({
  title: z.string().trim().min(1).max(500),
  url: z.string().trim().url(),
  category: z.string().trim().min(1).max(120),
  handle: z.string().trim().max(200).optional(),
  province_code: z
    .string()
    .trim()
    .max(120)
    .optional()
    .nullable()
    .refine((value) => !value || THAI_PROVINCE_SET.has(value), {
      message: 'Invalid province.',
    }),
})

const adminPatchSchema = z.object({
  status: z.enum(['pending', 'approved', 'rejected']).optional(),
  analysis_status: z.enum(['queued', 'processing', 'completed', 'failed']).optional(),
  analysis_result: z.unknown().optional(),
  province_code: z
    .string()
    .trim()
    .max(120)
    .optional()
    .nullable()
    .refine((value) => !value || THAI_PROVINCE_SET.has(value), {
      message: 'Invalid province.',
    }),
})

function submissionPayload(row: SubmissionRow) {
  const metadata = safeJsonParse<Record<string, unknown>>(row.metadata, {})
  const analysisResult = safeJsonParse<Record<string, unknown> | null>(row.analysis_result, null)
  return {
    id: row.id,
    user_id: row.user_id,
    title: row.title,
    description: row.description,
    source_url: row.source_url,
    category: row.category,
    page_target: row.page_target,
    status: row.status,
    analysis_status: row.analysis_status,
    analysis_result: analysisResult,
    metadata,
    province_code: row.province_code,
    submitted_by: typeof metadata.submitted_by === 'string' ? metadata.submitted_by : null,
    created_at: row.created_at,
    updated_at: row.updated_at,
  }
}

async function logAdminAction(
  sql: SqlClient,
  admin: LocalUser,
  action: string,
  targetType: string,
  targetId: string | null,
  details: Record<string, unknown> = {}
) {
  await sql`
    INSERT INTO admin_audit_log (admin_user_id, admin_email, action, target_type, target_id, details, created_at)
    VALUES (${admin.id}, ${admin.email}, ${action}, ${targetType}, ${targetId}, ${JSON.stringify(details)}, ${nowIso()})
  `
}

submissionRoutes.get('/submitted-data/mine', async (c) => {
  const sql = getSql(c)
  const user = await requireUser(c)
  if (user instanceof Response) return user
  const limit = intParam(c.req.query('limit'), 100, 1, 500)
  const rows = await sql<SubmissionRow[]>`
    SELECT * FROM local_submitted_data
    WHERE user_id = ${user.id}
    ORDER BY created_at DESC
    LIMIT ${limit}
  `
  return ok(c, { items: rows.map(submissionPayload) })
})

submissionRoutes.get('/submitted-data/public', async (c) => {
  const sql = getSql(c)
  const limit = intParam(c.req.query('limit'), 50, 1, 200)
  const rows = await sql<SubmissionRow[]>`
    SELECT * FROM local_submitted_data
    WHERE status = 'approved' AND analysis_status = 'completed'
    ORDER BY created_at DESC
    LIMIT ${limit}
  `
  return ok(c, { items: rows.map(submissionPayload) })
})

submissionRoutes.post('/submitted-data', async (c) => {
  const sql = getSql(c)
  const user = await requireUser(c)
  if (user instanceof Response) return user
  const parsed = submitSchema.safeParse(await c.req.json().catch(() => ({})))
  if (!parsed.success) return fail(c, 422, 'submission_invalid', 'Title, URL, and category are required.')
  const now = nowIso()
  const metadata = {
    submitted_by: user.display_name || user.email,
    handle: parsed.data.handle || null,
    platform: parsed.data.url.includes('youtube') ? 'youtube' : 'web',
  }
  const provinceCode = parsed.data.province_code?.trim() || null
  const [row] = await sql<SubmissionRow[]>`
    INSERT INTO local_submitted_data
      (user_id, title, source_url, category, page_target, status, analysis_status, metadata, province_code, created_at, updated_at)
    VALUES
      (${user.id}, ${parsed.data.title}, ${parsed.data.url}, ${parsed.data.category}, ${null},
       'pending', 'queued', ${JSON.stringify(metadata)}, ${provinceCode}, ${now}, ${now})
    RETURNING *
  `
  if (!row) return fail(c, 500, 'submission_failed', 'Failed to save the submission.')
  return c.json({ ok: true, data: submissionPayload(row), error: null }, 201)
})

submissionRoutes.post('/submitted-data/analyze-url', async (c) => {
  const sql = getSql(c)
  return fail(
    c,
    501,
    'analysis_not_implemented',
    'URL analysis is not enabled in the local Hono runtime yet. The submission can still be reviewed by an admin.'
  )
})

submissionRoutes.get('/categories', async (c) => {
  const sql = getSql(c)
  const rows = await sql<{ id: number; name: string; slug: string; description: string | null }[]>`
    SELECT id, name, slug, description FROM local_categories ORDER BY name
  `
  return ok(c, { items: rows })
})

submissionRoutes.post('/categories', async (c) => {
  const sql = getSql(c)
  const user = await requireUser(c)
  if (user instanceof Response) return user
  const body = await c.req.json().catch(() => ({}))
  const name = typeof body.name === 'string' ? body.name.trim() : ''
  if (!name) return fail(c, 422, 'category_invalid', 'Category name is required.')
  const slug = name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '') || `category-${Date.now()}`
  const [row] = await sql<{ id: number; name: string; slug: string; description: string | null }[]>`
    INSERT INTO local_categories (name, slug, description, created_by, created_at)
    VALUES (${name}, ${slug}, ${body.description ?? null}, ${user.id}, ${nowIso()})
    ON CONFLICT (name) DO NOTHING
    RETURNING id, name, slug, description
  `
  return c.json({ ok: true, data: row ?? null, error: null }, 201)
})

submissionRoutes.get('/admin/submitted-data', async (c) => {
  const sql = getSql(c)
  const admin = await requireAdmin(c)
  if (admin instanceof Response) return admin
  const status = c.req.query('status')
  const category = c.req.query('category')
  const limit = intParam(c.req.query('limit'), 200, 1, 500)
  const conditions: string[] = []
  const params: any[] = []
  if (status && status !== 'all') {
    params.push(status)
    conditions.push(`status = $${params.length}`)
  }
  if (category && category !== 'all') {
    params.push(category)
    conditions.push(`category = $${params.length}`)
  }
  const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : ''
  const rows = await sql.unsafe<SubmissionRow[]>(
    `SELECT * FROM local_submitted_data ${where} ORDER BY created_at DESC LIMIT $${params.length + 1}`,
    [...params, limit]
  )
  return ok(c, { items: rows.map(submissionPayload) })
})

submissionRoutes.patch('/admin/submitted-data/:id', async (c) => {
  const sql = getSql(c)
  const admin = await requireAdmin(c)
  if (admin instanceof Response) return admin
  const id = Number.parseInt(c.req.param('id'), 10)
  if (!Number.isFinite(id)) return fail(c, 422, 'submission_invalid', 'Invalid submission id.')
  const parsed = adminPatchSchema.safeParse(await c.req.json().catch(() => ({})))
  if (!parsed.success) return fail(c, 422, 'submission_patch_invalid', 'Invalid submission patch.')
  const entries = Object.entries(parsed.data).filter(([, value]) => value !== undefined)
  if (entries.length === 0) return fail(c, 422, 'submission_patch_empty', 'No fields were provided.')
  const assignments = entries.map(([key], index) => `${key} = $${index + 1}`)
  const values: any[] = entries.map(([key, value]) => {
    if (key === 'analysis_result' && value !== null) return JSON.stringify(value)
    if (key === 'province_code') return value === '' ? null : value
    return value
  })
  const [row] = await sql.unsafe<SubmissionRow[]>(
    `UPDATE local_submitted_data SET ${assignments.join(', ')}, updated_at = $${values.length + 1}
     WHERE id = $${values.length + 2} RETURNING *`,
    [...values, nowIso(), id]
  )
  if (!row) return fail(c, 404, 'submission_not_found', 'Submission not found.')
  await logAdminAction(sql, admin, 'update_submission', 'submission', String(id), parsed.data)
  return ok(c, submissionPayload(row))
})

submissionRoutes.get('/admin/users', async (c) => {
  const sql = getSql(c)
  const admin = await requireAdmin(c)
  if (admin instanceof Response) return admin
  const q = c.req.query('q')?.trim()
  const limit = intParam(c.req.query('limit'), 500, 1, 1000)
  const rows = q
    ? await sql<AdminUserRow[]>`
        SELECT id, email, display_name, role, created_at, organization, focus
        FROM local_users
        WHERE email LIKE ${`%${q}%`} OR display_name LIKE ${`%${q}%`}
        ORDER BY created_at DESC LIMIT ${limit}
      `
    : await sql<AdminUserRow[]>`
        SELECT id, email, display_name, role, created_at, organization, focus
        FROM local_users
        ORDER BY created_at DESC LIMIT ${limit}
      `
  return ok(c, { items: rows })
})

submissionRoutes.patch('/admin/users/:id', async (c) => {
  const sql = getSql(c)
  const admin = await requireAdmin(c)
  if (admin instanceof Response) return admin
  return fail(
    c,
    403,
    'role_managed_in_database',
    'Roles are provisioned directly in the database. Ask an operator to run the admin script.'
  )
})

submissionRoutes.get('/admin/audit-log', async (c) => {
  const sql = getSql(c)
  const admin = await requireAdmin(c)
  if (admin instanceof Response) return admin
  const limit = intParam(c.req.query('limit'), 100, 1, 500)
  const rows = await sql<
    {
      id: number
      admin_user_id: string
      admin_email: string | null
      action: string
      target_type: string
      target_id: string | null
      details: string | null
      created_at: string
    }[]
  >`SELECT * FROM admin_audit_log ORDER BY id DESC LIMIT ${limit}`
  return ok(c, {
    items: rows.map((row) => ({ ...row, details: safeJsonParse(row.details, {}) })),
  })
})
