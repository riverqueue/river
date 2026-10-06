/**
 * Thread-side handlers for the worker-thread tests.
 *
 * Tests reference this module as `handlers.js`, which does not exist beside
 * the TypeScript source, so every test also exercises the thread's fallback
 * from a missing `.js` module to its `.ts` source.
 */
import { parentPort } from "node:worker_threads";

import {
  complete as completeOutcome,
  snooze as snoozeOutcome,
} from "riverqueue";

import type { WorkerThreadWorkHandler } from "../index.js";
import { describeValue } from "./format.js";
import type { richJob, testJob } from "./jobs.js";

type TestHandler = WorkerThreadWorkHandler<typeof testJob>;

export const allocateForever: TestHandler = ({ logger }) => {
  logger.info("started");
  const retained: number[][] = [];
  for (;;) retained.push(new Array<number>(100_000).fill(retained.length));
};

export const complete: TestHandler = ({ job }) =>
  completeOutcome({ output: { value: job.args.value ?? null } });

export const confirmNativeTemporal: TestHandler = ({ execution }) => {
  if (!(execution.startedAt instanceof Temporal.Instant)) {
    throw new TypeError("startedAt is not a native Temporal.Instant");
  }
  return completeOutcome({
    output: { startedAt: execution.startedAt.toString() },
  });
};

export const cooperate: TestHandler = ({ logger, signal }) => {
  logger.info("started");
  return new Promise((_resolve, reject) => {
    signal.addEventListener("abort", () => reject(signal.reason as Error), {
      once: true,
    });
  });
};

export const crashWhileRunning: TestHandler = ({ logger }) => {
  logger.info("started");
  setImmediate(() => {
    throw new Error("crashed in flight");
  });
  return new Promise(() => undefined);
};

export const describeRich: WorkerThreadWorkHandler<typeof richJob> = ({
  job,
}) =>
  completeOutcome({
    output: {
      at: describeValue(job.args.at),
      big: job.args.big.toString(),
      bytes: describeValue(job.args.bytes),
      lookup: describeValue(job.args.lookup),
      tags: describeValue(job.args.tags),
    },
  });

export const echoArgs: TestHandler = ({ job }) =>
  completeOutcome({ output: { args: { ...job.args }, rawArgs: job.rawArgs } });

export const echoExact: TestHandler = ({
  job,
  logger,
  recordOutput,
  setMetadata,
}) => {
  const id = job.args.id ?? null;
  logger.info({ id }, "exact");
  recordOutput({ id });
  setMetadata("id", id);
  return completeOutcome({ output: { id } });
};

export const exitBeforeNextTask: TestHandler = () => {
  // Replace the run listener so the next task kills this thread before it
  // can acknowledge the task. A setImmediate callback races with that message.
  parentPort?.removeAllListeners("message");
  parentPort?.once("message", () => process.exit(9));
  return completeOutcome();
};

export const exitWhenIdle: TestHandler = () => {
  setImmediate(() => process.exit(7));
  return completeOutcome();
};

export const fail: TestHandler = () => {
  throw new Error("handler failed");
};

export const failWithHugeError: TestHandler = () => {
  throw new Error("x".repeat(100_000));
};

export const failWithThrowingGetters: TestHandler = () => {
  const error = new Error("hidden");
  for (const key of ["message", "name", "stack"]) {
    Object.defineProperty(error, key, {
      get() {
        throw new Error(`${key} getter failed`);
      },
    });
  }
  throw error;
};

export const logHuge: TestHandler = ({ logger }) => {
  logger.info("x".repeat(100_000));
  logger.info({ blob: "y".repeat(100_000) }, "with attributes");
  return completeOutcome();
};

export const metadataThenComplete: TestHandler = ({ setMetadata }) => {
  setMetadata("thread", { forwarded: true });
  return completeOutcome();
};

/** Not a job handler; registering it must not type-check. */
export function nthSquare(ordinal: number): number {
  return ordinal * ordinal;
}

export const oversizedOutput: TestHandler = ({ recordOutput, setMetadata }) => {
  const huge = "z".repeat(32 * 1024 * 1024 + 1);
  try {
    setMetadata("huge", huge);
  } catch (error: unknown) {
    recordOutput({ metadataRejected: (error as Error).message });
  }
  recordOutput(huge);
  return completeOutcome();
};

export const outputThenFail: TestHandler = ({ recordOutput }) => {
  recordOutput({ beforeFailure: true });
  throw new Error("failed after output");
};

export const rejectWhenIdle: TestHandler = () => {
  setImmediate(() => {
    void Promise.reject(new Error("stray rejection"));
  });
  return completeOutcome();
};

export const sleep: TestHandler = ({ job, logger }) => {
  logger.info("started");
  return new Promise((resolve) =>
    setTimeout(() => resolve(undefined), job.args.milliseconds ?? 0)
  );
};

export const snooze: TestHandler = () => snoozeOutcome({ seconds: 30 });

export const spin: TestHandler = ({ logger }) => {
  logger.info("started");
  for (;;) {
    // Deliberately block this isolated thread to exercise forced termination.
  }
};

export const throwWhenIdle: TestHandler = () => {
  setImmediate(() => {
    throw new Error("stray timer");
  });
  return completeOutcome();
};

export const uncloneableOutcome: TestHandler = () =>
  ({ output: { value: () => undefined }, type: "complete" }) as never;
