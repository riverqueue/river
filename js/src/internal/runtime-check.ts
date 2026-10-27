import { assertRuntimeSupport } from "../runtime-support.js";

// Public entry points evaluate this before modules that use Temporal at import
// time, so missing runtime features produce a useful configuration error.
assertRuntimeSupport();
