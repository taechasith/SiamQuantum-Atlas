.PHONY: install dev migrate import frontend test typecheck

install:
	cd server && bun install
	cd frontend && npm ci

dev:
	cd server && bun run dev

migrate:
	cd server && bun run migrate

import:
	cd server && bun run import

frontend:
	cd frontend && npm run dev

typecheck:
	cd server && bun run typecheck

test: typecheck
	cd frontend && npm run build
