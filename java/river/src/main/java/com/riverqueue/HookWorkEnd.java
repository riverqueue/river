package com.riverqueue;

/** A hook run inside worker middleware after the handler returns or throws. */
@FunctionalInterface
public interface HookWorkEnd extends Hook {
  /**
   * Receives the handler's failure, or {@code null} on success. Return it unchanged to preserve the
   * outcome, another failure to replace it, or {@code null} to report success. Each hook receives
   * the previous hook's result, including control exceptions for cancellation and snoozing.
   * Throwing fails the attempt and skips remaining work-end hooks.
   */
  Throwable workEnd(WorkContext<?> context, Throwable failure) throws Exception;
}
