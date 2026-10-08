package com.riverqueue;

import java.sql.Connection;
import java.util.concurrent.Callable;

/** Transaction-scoped insert hooks and attempt-scoped worker middleware. */
public interface Extension {
  default void beforeInsert(
      Connection connection, JobType<?> type, Object args, InsertOptions options)
      throws Exception {}

  default void beforeWork(WorkContext<?> context) throws Exception {}

  default void afterWork(WorkContext<?> context, Throwable failure) throws Exception {}

  default void afterClaim(Job<?> job) throws Exception {}

  default <T> T insert(Connection connection, Callable<T> next) throws Exception {
    return next.call();
  }

  default void periodicStarted() {}

  default void work(WorkContext<?> context, WorkContext.Step next) throws Exception {
    next.run();
  }
}
