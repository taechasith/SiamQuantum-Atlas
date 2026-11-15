.PHONY: install dev frontend test typecheck

install:
	cd server && bun install
	cd frontend && npm ci

dev:
	cd server && bun run dev

frontend:
	cd frontend && npm run dev

typecheck:
	cd server && bun run typecheck

test: typecheck
	cd frontend && npm run build
