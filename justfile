set dotenv-load := true

default:
    @just --list

up:
    docker compose up -d --build

down:
    docker compose down

logs:
    docker compose logs -f hono

migrate:
    cd server && bun run migrate

import:
    cd server && bun run import

api-dev:
    cd server && bun run dev

front-dev:
    cd frontend && npm run dev

front-build:
    cd frontend && npm run build

typecheck:
    cd server && bun run typecheck

test:
    cd server && bun run typecheck
    cd frontend && npm run build
