import { Hono } from 'hono'
import { cors } from 'hono/cors'
import { logger } from 'hono/logger'
import { secureHeaders } from 'hono/secure-headers'
import { publicRoutes } from './routes/public'
import { authRoutes } from './routes/auth'
import { submissionRoutes } from './routes/submissions'
import { exportRoutes } from './routes/export'
import { systemRoutes } from './routes/system'

export const app = new Hono()

app.use('*', logger())
app.use('*', secureHeaders())
app.use(
  '/api/*',
  cors({
    origin: [
      'http://localhost:5173',
      'http://127.0.0.1:5173',
      'http://localhost:8765',
      'http://127.0.0.1:8765',
      'http://localhost:8766',
      'http://127.0.0.1:8766',
    ],
    credentials: true,
    allowMethods: ['GET', 'POST', 'PATCH', 'DELETE', 'OPTIONS'],
    allowHeaders: ['Content-Type', 'Authorization'],
  })
)

app.get('/', (c) => c.text('SiamQuantum Atlas Hono API'))
app.get('/healthz', (c) => c.json({ ok: true, data: { status: 'ok', runtime: 'bun', service: 'hono' }, error: null }))

app.route('/api', publicRoutes)
app.route('/api', authRoutes)
app.route('/api', submissionRoutes)
app.route('/api', exportRoutes)
app.route('/api', systemRoutes)

app.notFound((c) =>
  c.json({ ok: false, data: null, error: { code: 'not_found', message: 'Route not found' } }, 404)
)

app.onError((error, c) => {
  console.error(error)
  return c.json(
    {
      ok: false,
      data: null,
      error: { code: 'internal_error', message: error.message || 'Internal server error' },
    },
    500
  )
})
