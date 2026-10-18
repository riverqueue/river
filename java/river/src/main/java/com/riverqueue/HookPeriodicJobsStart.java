package com.riverqueue;

/** A hook run when periodic job enqueuing starts on a newly elected leader. */
@FunctionalInterface
public interface HookPeriodicJobsStart extends Hook {
  /**
   * Runs once per leadership term after election commits. Throwing prevents maintenance from
   * starting and retries initialization on the next leadership check.
   */
  void start() throws Exception;
}
