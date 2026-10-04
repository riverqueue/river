# CPU worker-thread example

This self-contained SQLite example routes a CPU-heavy prime calculation through
the bounded `@riverqueue/worker-threads` executor. The main event loop remains
available for River's database, completion, cancellation, and shutdown work.

```sh
pnpm --filter riverqueue-example-worker-thread-cpu run build
pnpm --filter riverqueue-example-worker-thread-cpu run start
```

The example also runs directly from its TypeScript sources with Node's
built-in type stripping, without a build:

```sh
pnpm --filter riverqueue-example-worker-thread-cpu run start:source
```

The job definition lives in `jobs.ts`, so producers can import it without the
handler. `handler.ts` is the module that runs in worker threads; its export is
typed with the definition, and `index.ts` references it by URL with that
module's type, so a misspelled or mismatched export name fails to compile. The
definition's schema validates args in the main thread before an attempt
reaches a thread.

The handler URL uses the compiled `.js` name. When only `handler.ts` exists,
River's threads load the source instead, so the same URL works from `dist` and
from `src`. Relative imports use `.ts` extensions, which
`rewriteRelativeImportExtensions` turns into `.js` in the build, because Node's
type stripping runs the main module from source without a loader.
