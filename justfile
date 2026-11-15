set dotenv-load := true

default:
    @just --list

up:
    docker compose up -d --build

down:
    docker compose down

logs:
    docker compose logs -f hono

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
