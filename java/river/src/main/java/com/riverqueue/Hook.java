package com.riverqueue;

/**
 * A lifecycle hook registered with {@link Client#withHooks(Hook...)}. Implement one or more of
 * {@link HookInsertBegin}, {@link HookPeriodicJobsStart}, {@link HookWorkBegin}, and {@link
 * HookWorkEnd}. Hooks run in registration order and may be called concurrently. Use {@link
 * Client#withPlugins(Plugin...)} to also activate any middleware interfaces implemented by the same
 * instance.
 */
public interface Hook extends Plugin {}
