/** Abort-aware waits used by the conformance adapters' scripted workers. */

/** Reject with `signal.reason` once the signal aborts; never resolves. */
export function abortPromise(signal: AbortSignal): Promise<never> {
  if (signal.aborted) return Promise.reject(signal.reason);
  return new Promise((_, reject) => {
    signal.addEventListener("abort", () => reject(signal.reason), {
      once: true,
    });
  });
}
