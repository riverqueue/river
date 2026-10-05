# `@riverqueue/worker-threads`

This optional package runs CPU-heavy River handlers in a bounded pool of native
Node.js worker threads, so they cannot block the event loop that River uses for
claiming, completing, cancelling, and shutting down work. Ordinary I/O handlers
should stay in-process; promises already let them share the event loop
efficiently.

## Defining a thread handler

A thread handler is an ES module export, because functions cannot cross a
thread boundary. Keep the job definition in its own module so producers can
import it without the handler. In `jobs.ts`:

```ts file=jobs.ts
import { defineJob } from "riverqueue";
import { z } from "zod";

export const resizeImage = defineJob({
  kind: "resize_image",
  schema: z.object({
    source: z.string(),
    width: z.number().int().positive(),
  }),
});
```

Type the handler export with the definition so its args are typed. A
type-only import keeps the thread from loading anything it does not need. In
`image-handler.ts`:

```ts file=image-handler.ts
import type { WorkerThreadWorkHandler } from "@riverqueue/worker-threads";
import { complete } from "riverqueue";

import type { resizeImage } from "./jobs.js";

export const resizeImageHandler: WorkerThreadWorkHandler<
  typeof resizeImage
> = async ({ job, signal }) => {
  const bytes = await resize(job.args.source, job.args.width, signal);
  return complete({ output: { bytes } });
};

async function resize(
  source: string,
  width: number,
  signal: AbortSignal
): Promise<number> {
  signal.throwIfAborted();
  return source.length * width;
}
```

## Registering it

Reference the handler module by URL and export name. Annotating the URL with
the module's type lets TypeScript reject an export name that is missing or
that handles a different definition, without importing the handler's code into
the main thread:

```ts
import {
  WorkerThreads,
  type WorkerThreadModule,
} from "@riverqueue/worker-threads";
import { Workers } from "riverqueue";

import type * as imageHandlers from "./image-handler.js";
import { resizeImage } from "./jobs.js";

const imageModule: WorkerThreadModule<typeof imageHandlers> = new URL(
  "./image-handler.js",
  import.meta.url
);

await using executor = new WorkerThreads({ maxThreads: 4 });
const workers = new Workers().addExecutor(
  resizeImage,
  executor.handler(resizeImage, {
    exportName: "resizeImageHandler",
    module: imageModule,
  })
);
```

A plain `URL` also works, but then any export name compiles and a wrong one
fails only when an attempt runs. Pass `workers` to one or more clients as
usual.

## Arguments, results, and errors

River decodes and validates a job's args with its definition in the main
thread, exactly as for an in-process handler, and the thread receives the
decoded `job.args` alongside the persisted JSON in `job.rawArgs`. A job
inserted by another language with invalid args fails before it reaches a
thread.

Decoded args that are River JSON cross the boundary as text, which preserves
exact JSON numbers such as a Go-produced int64 ID. Other decoded args must
arrive unchanged through structured clone: `bigint`, `Date`, `Map`, `Set`,
`Uint8Array`, and plain objects and arrays of those. An attempt whose decoded
args contain anything else, such as a class instance or a function, fails with
a `ConfigurationError` instead of reaching the handler with the wrong type.

The handler's context has the job, execution metadata, logger, `signal`,
`recordOutput`, and `setMetadata`. It has no `client`, `completeTx`, or
`resumable`, which depend on the main thread. Outcomes, output, metadata, and
log attributes cross back as River JSON. Like output on the main thread,
`recordOutput` and each `setMetadata` value throw a `ValidationError` over
32 MiB. A log message is cut to 32 KiB, and log attributes whose JSON
exceeds 32 KiB are dropped with a note in the message. A thrown error keeps
its name, message, and stack, bounded to the same 32 KiB limits River applies
to persisted attempt errors, and fails the attempt with a
`WorkerThreadHandlerError`. An error whose properties can't be read still
fails only its attempt.

## Cancellation, timeouts, and crashes

When an attempt is cancelled, times out, or its client stops, the handler's
`signal` aborts. If the handler has not settled after the client's
`jobStuckThreshold` (10 seconds by default), River terminates its thread, so a CPU-bound handler
that never yields cannot hold River indefinitely. River records the attempt's
outcome only after the handler has settled or its thread has exited. If the
abort came from its client stopping, a handler terminated this way fails with a
`JobAbortedError`, so its attempt counts and the retry policy applies; after a
cancellation or a timeout, the attempt ends as if the handler had stopped
itself.

Attempts wait for a thread when all `maxThreads` are busy, and their job
timeout starts only once a thread takes them. A client can therefore allow
more workers than the executor has threads without timing out queued jobs.

A thread that crashes, whether from an uncaught error, an unhandled rejection,
`process.exit()`, or a resource limit, fails only the attempt it was running.
A crash while idle, typically from background work a handler left behind, only
discards the thread. Replacements start when later attempts need them, and
`diagnostics().crashedThreads` counts crashes. Limit each thread's V8 heap with
`resourceLimits`:

```ts
import { WorkerThreads } from "@riverqueue/worker-threads";

const limited = new WorkerThreads({
  maxThreads: 2,
  resourceLimits: { maxOldGenerationSizeMb: 256 },
});
await limited.close();
```

## Ownership and shutdown

The application owns the executor. Several clients may share one, and stopping
a client never closes it. Stop every client that uses it, then close it with
`await executor.close()` or let an `await using` scope do so. Closing fails any
attempts still queued or running. Idle threads do not keep the process alive,
so an executor that is never closed does not prevent exit.

## TypeScript sources in development

Node 26 strips TypeScript types natively, and River's threads load a module's
TypeScript source when the `.js` file that a handler URL or one of its relative
imports names does not exist but a `.ts` file beside it does. Keep referencing
handler modules by their compiled `.js` name, as above; the same URL then
works:

- from a build, where the `.js` files exist;
- from source with `node src/main.ts`, provided the main thread's own relative
  imports use `.ts` extensions, for example with TypeScript's
  `rewriteRelativeImportExtensions`, since Node resolves those itself; and
- in Vitest, whose test files run from source.

The fallback applies only after a resolution fails, so it never changes which
module a build loads. Loaders registered with `--import` also apply inside
River's threads, because Node passes the process's `execArgv` to worker
threads. Handler modules must use TypeScript syntax that Node can strip, which
excludes features such as enums and parameter properties.

## Security

Worker threads provide availability isolation, not a security sandbox. A
handler shares the process, its environment, and its file system access. Only
run trusted handler modules.

## Requirements

Node.js 26 or newer with native `Temporal`: `node -p "typeof Temporal"` must
print `object`. Official Node.js binaries include it; some builds compiled from
source, including some distribution and Homebrew packages, do not.

Install `riverqueue` at exactly this package's version. It is a peer dependency,
so npm rejects a mismatched pair instead of loading two copies.

TypeScript users need TypeScript 6.0 or newer and `@types/node`, with `"node"`
listed in `compilerOptions.types`. See [River's
requirements](https://github.com/riverqueue/river/tree/master/js#requirements) for
details.
