// A worker-thread handler module loaded by the packed SQLite runtime test.
import { complete, isExactJsonNumber } from "riverqueue";

export async function echoAccount({ job, signal }) {
  signal.throwIfAborted();
  if (!(job.createdAt instanceof Temporal.Instant)) {
    throw new TypeError("job timestamp does not use the worker's Temporal");
  }
  const { accountId } = job.args;
  if (!isExactJsonNumber(accountId)) {
    throw new TypeError("accountId did not cross the thread boundary exactly");
  }
  return complete({ output: { accountId, thread: true } });
}
