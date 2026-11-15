# SiamQuantum Atlas — Hono/Bun API
FROM oven/bun:1-alpine

WORKDIR /app

COPY server/package.json server/bun.lock ./
RUN bun install --frozen-lockfile

COPY server/ ./

ENV NODE_ENV=production
EXPOSE 8766

HEALTHCHECK --interval=10s --timeout=5s --retries=5 \
  CMD wget -qO- http://127.0.0.1:8766/healthz >/dev/null || exit 1

CMD ["bun", "src/index.ts"]
