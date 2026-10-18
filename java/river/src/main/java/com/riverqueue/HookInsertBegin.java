package com.riverqueue;

import java.sql.Connection;

/** A hook run for each job inside insertion middleware, before the database write. */
@FunctionalInterface
public interface HookInsertBegin extends Hook {
  /**
   * Runs in the insertion's transaction. Throwing fails and rolls back the insertion, including
   * application writes made using this connection. Options are the caller's overrides, before
   * merging with job-type defaults.
   */
  void insertBegin(Connection connection, JobType<?> type, Object args, InsertOptions options)
      throws Exception;
}
