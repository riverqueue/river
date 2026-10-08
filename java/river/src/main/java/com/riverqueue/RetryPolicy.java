package com.riverqueue;

import java.time.Duration;
import java.util.Objects;
import java.util.random.RandomGenerator;

/**
 * Chooses a nonnegative retry delay from the failed attempt's snapshot, before its new error is
 * appended. Called only when another attempt is possible. Delays must fit in signed 64-bit
 * nanoseconds. A thrown failure, null, negative or overflowing delay, or unrepresentable retry time
 * is reported and replaced with the default policy's delay.
 */
@FunctionalInterface
public interface RetryPolicy {
  Duration delay(Job<?> job);

  static Default defaults() {
    return defaults(RandomGenerator.getDefault());
  }

  static Default defaults(RandomGenerator random) {
    return new Default(random);
  }

  /** River's quartic backoff and jitter, excluding snoozes from the failure count. */
  final class Default implements RetryPolicy {
    private final RandomGenerator random;

    private Default(RandomGenerator random) {
      this.random = Objects.requireNonNull(random, "random");
    }

    @Override
    public Duration delay(Job<?> job) {
      return delay(job.errors().size() + 1);
    }

    /** Computes the same backoff from an explicit failure count, including the current failure. */
    public synchronized Duration delay(int errorCount) {
      double seconds = Math.pow(Math.max(1, errorCount), 4);
      if (seconds * 1e9 >= Long.MAX_VALUE) return Duration.ofNanos(Long.MAX_VALUE);
      return Duration.ofNanos(
          (long) Math.min(Long.MAX_VALUE, seconds * 1e9 * (0.9 + random.nextDouble() * 0.2)));
    }
  }
}
