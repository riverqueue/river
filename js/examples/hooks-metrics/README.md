# Hooks and metrics example

This SQLite example connects River's ordered hooks, Koa-style middleware, and
after-commit events to a tiny in-memory metrics collector. A production service
would replace the collector with its existing metrics or tracing SDK; River
does not require a particular observability dependency.

```sh
pnpm --filter riverqueue-example-hooks-metrics run build
pnpm --filter riverqueue-example-hooks-metrics run start
```

Hooks observe attempt-local work, while `job_completed` observes the committed
database transition. Do not count both as independent completed jobs.
