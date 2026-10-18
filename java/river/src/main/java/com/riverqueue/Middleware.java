package com.riverqueue;

/**
 * Middleware registered with {@link Client#withMiddleware(Middleware...)}. Implement {@link
 * JobInsertMiddleware}, {@link WorkerMiddleware}, or both. The first registered middleware is
 * outermost. Instances may be called concurrently. Use {@link Client#withPlugins(Plugin...)} to
 * also activate any hook interfaces implemented by the same instance.
 */
public interface Middleware extends Plugin {}
