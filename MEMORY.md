## Phase 6a  second test gap
- TestClient (used by 17 gate tests) creates app instance directly, bypassing uvicorn.run() invocation.
- This means a broken `serve` command can coexist with 100% test pass rate.
- Lesson: any "serve"-style command must have at least one smoke test that actually invokes the CLI subprocess, hits a real HTTP request, and asserts response. TestClient is necessary but not sufficient.
