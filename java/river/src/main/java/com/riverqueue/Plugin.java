package com.riverqueue;

/**
 * A shared type for hooks and middleware installed with {@link Client#withPlugins(Plugin...)}.
 * Implement any combination of the operation-specific {@link Hook} and {@link Middleware}
 * interfaces; they already extend this interface, so no additional methods are required.
 *
 * <p>Registration activates every hook and middleware interface the instance implements. Plugins
 * run in registration order, before separately registered hooks and middleware. The first
 * middleware is outermost. Instances may be invoked concurrently.
 */
public interface Plugin {}
