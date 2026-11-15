import { app } from '../../server/src/app'

type PagesContext = {
  request: Request
  env: unknown
  waitUntil: (promise: Promise<unknown>) => void
}

export const onRequest = (context: PagesContext) =>
  app.fetch(context.request, context.env as never, context as never)
