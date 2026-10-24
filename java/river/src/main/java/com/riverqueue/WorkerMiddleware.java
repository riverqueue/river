package com.riverqueue;

/** Middleware around one job attempt, including its work hooks and handler. */
@FunctionalInterface
public interface WorkerMiddleware extends Middleware {
  /**
   * Runs with decoded arguments. Call {@code next.run()} once to run the inner middleware, hooks,
   * and handler. Throwing fails the attempt, even if the inner work succeeded.
   */
  void work(WorkContext<?> context, WorkContext.Step next) throws Exception;
}
