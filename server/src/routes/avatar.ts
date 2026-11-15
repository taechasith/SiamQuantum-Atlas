import { Hono } from 'hono'
import { getSql, type AppEnv } from '../db/client'
import { profilePayload, requireUser, type LocalUser } from '../lib/auth'
import { fail, nowIso, ok } from '../lib/http'
import { getStorage } from '../lib/storage'

export const avatarRoutes = new Hono<AppEnv>()

const MAX_AVATAR_BYTES = 5 * 1024 * 1024
const AVATAR_TYPES: Record<string, string> = {
  'image/png': 'png',
  'image/jpeg': 'jpg',
  'image/webp': 'webp',
}

function oldAvatarKey(url: string | null, userId: string): string | null {
  if (!url) return null
  const prefix = `/api/avatar/${userId}/`
  if (!url.startsWith(prefix)) return null
  const file = url.slice(prefix.length)
  if (!/^[A-Za-z0-9._-]+$/.test(file)) return null
  return `avatars/${userId}/${file}`
}

avatarRoutes.post('/profile/avatar', async (c) => {
  const user = await requireUser(c)
  if (user instanceof Response) return user

  const storage = getStorage(c)
  if (!storage) {
    return fail(c, 503, 'storage_not_configured', 'R2 storage is not configured.')
  }

  const body = await c.req.parseBody()
  const file = body.avatar
  if (!(file instanceof File)) {
    return fail(c, 422, 'avatar_required', 'An image file is required.')
  }

  const extension = AVATAR_TYPES[file.type]
  if (!extension) {
    return fail(c, 422, 'avatar_type_invalid', 'Avatar must be a PNG, JPEG, or WebP image.')
  }
  if (file.size <= 0 || file.size > MAX_AVATAR_BYTES) {
    return fail(c, 422, 'avatar_size_invalid', 'Avatar must be between 1 byte and 5 MB.')
  }

  const key = `avatars/${user.id}/${crypto.randomUUID()}.${extension}`
  const bytes = new Uint8Array(await file.arrayBuffer())
  await storage.put(key, bytes, file.type)

  const previousKey = oldAvatarKey(user.avatar_url, user.id)
  if (previousKey) {
    await storage.delete(previousKey).catch(() => undefined)
  }

  const avatarUrl = `/api/avatar/${user.id}/${key.split('/').pop()}`
  const sql = getSql(c)
  const [updated] = await sql<LocalUser[]>`
    UPDATE local_users
    SET avatar_url = ${avatarUrl}, updated_at = ${nowIso()}
    WHERE id = ${user.id}
    RETURNING *
  `
  if (!updated) {
    return fail(c, 500, 'avatar_update_failed', 'Failed to update the profile picture.')
  }

  return ok(c, { profile: profilePayload(updated) })
})

avatarRoutes.get('/avatar/:userId/:file', async (c) => {
  const userId = c.req.param('userId')
  const file = c.req.param('file')
  if (!/^[a-f0-9]{32}$/i.test(userId) || !/^[A-Za-z0-9._-]+$/.test(file)) {
    return fail(c, 400, 'avatar_invalid', 'Invalid avatar path.')
  }

  const storage = getStorage(c)
  if (!storage) {
    return fail(c, 503, 'storage_not_configured', 'R2 storage is not configured.')
  }

  const object = await storage.get(`avatars/${userId}/${file}`)
  if (!object) {
    return fail(c, 404, 'avatar_not_found', 'Avatar not found.')
  }

  return new Response(object.body as never, {
    status: 200,
    headers: {
      'Content-Type': object.contentType,
      'Cache-Control': 'public, max-age=31536000, immutable',
    },
  })
})
