// Shared helpers for the packed-package suite. These files run with Node's
// built-in test runner inside a scratch consumer that installed River from
// `pnpm pack` tarballs, so they exercise exactly what npm would publish.

/* global AbortSignal */

/**
 * Start a client and wait until each listed job reaches a terminal state.
 * Resolves with the terminal events keyed by job ID.
 */
export async function workUntilFinalized(client, ids, timeoutMs = 10_000) {
  const pending = new Set(ids);
  const finalized = new Map();
  const subscription = client.subscribe({
    kinds: ["job_cancelled", "job_completed", "job_failed"],
    signal: AbortSignal.timeout(timeoutMs),
  });
  const run = await client.start();
  try {
    for await (const event of subscription) {
      if (event.kind === "job_failed" && event.job.state !== "discarded") {
        continue;
      }
      if (!pending.delete(event.job.id)) continue;
      finalized.set(event.job.id, event);
      if (pending.size === 0) break;
    }
  } finally {
    subscription.close();
    await run.stop({ mode: "graceful", timeout: { seconds: 5 } });
  }
  if (pending.size > 0) {
    throw new Error(`jobs did not finish: ${[...pending].join(", ")}`);
  }
  return finalized;
}
