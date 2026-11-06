import { app } from './app'

const port = Number(process.env.PORT ?? 8766)

const server = Bun.serve({
  port,
  fetch: app.fetch,
  idleTimeout: 30,
})

console.log(`SiamQuantum Hono API listening on http://127.0.0.1:${server.port}`)
