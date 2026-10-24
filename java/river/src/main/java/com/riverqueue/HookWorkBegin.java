package com.riverqueue;

/** A hook run inside worker middleware, before the handler. */
@FunctionalInterface
public interface HookWorkBegin extends Hook {
  /**
   * Runs with decoded arguments. Throwing fails the attempt without running the handler or work-end
   * hooks.
   */
  void workBegin(WorkContext<?> context) throws Exception;
}
