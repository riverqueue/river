import type { WorkerThreadWorkContext } from "@riverqueue/worker-threads";

/** Deliberately uncooperative conformance handler used to prove hard aborts. */
export async function work(context: WorkerThreadWorkContext): Promise<void> {
  const behavior = (context.job.args as Record<string, unknown>).behavior;
  if (behavior !== "ignored_cancel") {
    throw new Error(
      `unexpected worker-thread conformance behavior ${JSON.stringify(behavior)}`
    );
  }
  await new Promise<never>(() => undefined);
}
