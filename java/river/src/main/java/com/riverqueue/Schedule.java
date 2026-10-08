package com.riverqueue;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Optional;

/** A schedule evaluated strictly after a reference time. */
@FunctionalInterface
public interface Schedule {
  Optional<OffsetDateTime> next(OffsetDateTime after);

  /**
   * Evaluates after a named-zone reference and returns the result in that zone. Cron schedules use
   * its daylight-saving rules unless the expression specifies a different zone.
   */
  default Optional<ZonedDateTime> next(ZonedDateTime after) {
    return next(after.toOffsetDateTime()).map(next -> next.atZoneSameInstant(after.getZone()));
  }

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
