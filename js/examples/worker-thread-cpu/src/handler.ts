import type { WorkerThreadWorkHandler } from "@riverqueue/worker-threads";
import { complete } from "riverqueue";

import type { findPrime } from "./jobs.ts";

// Runs in a worker thread. `job.args` was already validated by `findPrime`'s
// schema in the main thread, so `ordinal` is a positive safe integer.
export const findPrimeHandler: WorkerThreadWorkHandler<typeof findPrime> = ({
  job,
  signal,
}) => complete({ output: { prime: nthPrime(job.args.ordinal, signal) } });

function nthPrime(ordinal: number, signal: AbortSignal): number {
  let found = 0;
  let candidate = 1;
  while (found < ordinal) {
    candidate++;
    if (isPrime(candidate)) found++;
    // Cooperate with cancellation; River terminates the thread otherwise.
    if (candidate % 10_000 === 0) signal.throwIfAborted();
  }
  return candidate;
}

function isPrime(value: number): boolean {
  for (let divisor = 2; divisor * divisor <= value; divisor++) {
    if (value % divisor === 0) return false;
  }
  return true;
}
