// Application-style setup for tests and packed consumers. Library imports never
// install a polyfill. Preload with --import so worker threads run this too.
if (globalThis.Temporal === undefined) {
  const { Temporal } = await import("@js-temporal/polyfill");
  Object.defineProperty(globalThis, "Temporal", {
    configurable: true,
    value: Temporal,
    writable: true,
  });
}
