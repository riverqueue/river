// Node 24 needs a polyfill; Node 26 keeps its native implementation. Install
// it before evaluating River modules so applications can use the same global
// Temporal constructors as River, including inside worker threads.
import { Temporal } from "@js-temporal/polyfill";

const nativeTemporal: unknown = Reflect.get(globalThis, "Temporal");
if (nativeTemporal === undefined) {
  Object.defineProperty(globalThis, "Temporal", {
    configurable: true,
    value: Temporal,
    writable: true,
  });
}
