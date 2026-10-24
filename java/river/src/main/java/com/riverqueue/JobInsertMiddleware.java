package com.riverqueue;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.Callable;

/** Middleware around an insertion, including its hooks and database writes. */
@FunctionalInterface
public interface JobInsertMiddleware extends Middleware {
  /**
   * Wraps a whole batch in its transaction; a single insertion is a one-element batch. Submissions
   * are an immutable list of the caller's requests, before merging job-type defaults. Call {@code
   * next.call()} exactly once and return its results in input order. Throwing, even after {@code
   * next.call()}, rolls back the insertion and application writes made using this connection.
   */
  List<Job.InsertResult> insertMany(
      Connection connection,
      List<Client.Submission<?>> submissions,
      Callable<List<Job.InsertResult>> next)
      throws Exception;
}
