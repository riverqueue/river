package com.riverqueue;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Optional;

/** A schedule evaluated strictly after a reference time. */
@FunctionalInterface
public interface Schedule {
  Optional<OffsetDateTime> next(OffsetDateTime after);

  /** Parses River Go's five-field cron syntax, including descriptors and time zones. */
  static Schedule cron(String expression) {
    return new CronSchedule(expression);
  }

  static Schedule every(Duration interval) {
    if (interval.isNegative() || interval.isZero())
      throw new IllegalArgumentException("Interval must be positive");
    return after -> Optional.of(after.plus(interval));
  }
}
