# Mixed-language producer example

On Node.js 24, [set up Temporal](../../README.md#temporal-on-nodejs-24) and
set `NODE_OPTIONS` to preload it before running the commands below. Node.js 26
provides Temporal natively.

This Postgres example inserts a versioned payload under the stable job kind
`mixed_language.generate_report`, then reads the durable row back without any
JavaScript-private metadata. A Go, Rust, or JavaScript worker can register that
same kind and decode the same JSON contract.

```sh
DATABASE_URL=postgres://localhost/river_dev \
  pnpm --filter riverqueue-example-mixed-language run build
DATABASE_URL=postgres://localhost/river_dev \
  pnpm --filter riverqueue-example-mixed-language run start
```

Language-level type names do not need to match. Persisted job kinds, payload
field names, JSON meanings, and migration compatibility do. Version payload
changes explicitly, and keep every language's schema accepting both the old
and new shapes during rolling deployments.
