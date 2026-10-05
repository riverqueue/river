# Graceful shutdown example

This self-contained SQLite example starts one active handler and requests a
graceful stop while it is still running. River stops fetching, allows the
handler to finish, persists completion, and then resolves `RunHandle.stop`.

```sh
pnpm --filter riverqueue-example-graceful-shutdown run build
pnpm --filter riverqueue-example-graceful-shutdown run start
```

Production services should trigger the same stop call from their process
manager's shutdown signal and keep `run.completed` observed for fatal service
errors.
