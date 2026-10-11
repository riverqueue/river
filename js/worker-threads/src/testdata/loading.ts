import { parentPort } from "node:worker_threads";

// The test starts this module as task 1 in a fresh pool. Report that its
// import is pending and finish importing only once the abort message arrives.
await new Promise<void>((resolve) => {
  parentPort?.once("message", () => resolve());
  parentPort?.postMessage({
    level: "info",
    message: "started",
    taskId: 1,
    type: "log",
  });
});

export function complete(): never {
  throw new Error("aborted handler was invoked");
}
