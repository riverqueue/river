package com.riverqueue;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.BitSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Pattern;

final class CronSchedule implements Schedule {
  private final Field days;
  private final Field hours;
  private final Duration interval;
  private final Field minutes;
  private final Field months;
  private final Field weekdays;
  private final ZoneId zone;

  CronSchedule(String expression) {
    expression = expression.strip();
    ZoneId explicitZone = null;
    if (expression.startsWith("CRON_TZ=") || expression.startsWith("TZ=")) {
      int separator = expression.indexOf(' ');
      if (separator < 0)
        throw new IllegalArgumentException("Missing cron expression after time zone");
      explicitZone = ZoneId.of(expression.substring(expression.indexOf('=') + 1, separator));
      expression = expression.substring(separator + 1).strip();
    }
    zone = explicitZone;
    if (expression.startsWith("@every ")) {
      interval = duration(expression.substring(7));
      days = hours = minutes = months = weekdays = null;
      return;
    }
    interval = null;
    expression =
        switch (expression) {
          case "@annually", "@yearly" -> "0 0 1 1 *";
          case "@monthly" -> "0 0 1 * *";
          case "@weekly" -> "0 0 * * 0";
          case "@daily", "@midnight" -> "0 0 * * *";
          case "@hourly" -> "0 * * * *";
          default -> expression;
        };
    String[] fields = expression.split("\\s+");
    if (fields.length != 5) throw new IllegalArgumentException("Cron requires five fields");
    minutes = field(fields[0], 0, 59, List.of());
    hours = field(fields[1], 0, 23, List.of());
    days = field(fields[2], 1, 31, List.of());
    months =
        field(
            fields[3],
            1,
            12,
            List.of(
                "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov",
                "dec"));
    weekdays = field(fields[4], 0, 6, List.of("sun", "mon", "tue", "wed", "thu", "fri", "sat"));
  }

  private boolean matchesDay(ZonedDateTime time) {
    boolean day = days.values.get(time.getDayOfMonth());
    boolean weekday = weekdays.values.get(time.getDayOfWeek().getValue() % 7);
    return days.wildcard || weekdays.wildcard ? day && weekday : day || weekday;
  }

  @Override
  public Optional<OffsetDateTime> next(OffsetDateTime after) {
    return next(after.toZonedDateTime()).map(ZonedDateTime::toOffsetDateTime);
  }

  @Override
  public Optional<ZonedDateTime> next(ZonedDateTime after) {
    if (interval != null) return Optional.of(after.truncatedTo(ChronoUnit.SECONDS).plus(interval));
    // Go uses the reference's zone for unqualified cron; Workers supplies the process zone.
    var current =
        after
            .withZoneSameInstant(zone == null ? after.getZone() : zone)
            .truncatedTo(ChronoUnit.SECONDS)
            .plusSeconds(1);
    int limit = current.getYear() + 5;
    boolean advanced = false;
    // Match Go's field order and wrap points. Rechecking every field after every
    // increment changes the result when a DST transition advances only part of an hour.
    search:
    while (current.getYear() <= limit) {
      while (!months.values.get(current.getMonthValue())) {
        if (!advanced) {
          current =
              resolve(current.toLocalDate().withDayOfMonth(1).atStartOfDay(), current.getZone());
          advanced = true;
        }
        // Normalize from the first day rather than clamping the day to the next month's length.
        // A missing midnight can have resolved the first day into the previous month.
        current =
            resolve(
                current
                    .toLocalDateTime()
                    .withDayOfMonth(1)
                    .plusMonths(1)
                    .plusDays(current.getDayOfMonth() - 1L),
                current.getZone());
        if (current.getMonthValue() == 1) continue search;
      }
      while (!matchesDay(current)) {
        if (!advanced) {
          current = resolve(current.toLocalDate().atStartOfDay(), current.getZone());
          advanced = true;
        }
        var previous = current;
        current = resolve(current.toLocalDateTime().plusDays(1), current.getZone());
        if (current.getHour() != 0)
          current =
              current.plusHours(
                  current.getHour() > 12 ? 24 - current.getHour() : -current.getHour());
        // Some zones skipped an entire date. Do not reproduce Go's infinite loop when its
        // date resolution lands back on the previous day (e.g. Apia on 2011-12-30).
        if (!current.isAfter(previous))
          current = previous.toLocalDate().plusDays(1).atStartOfDay(previous.getZone());
        if (current.getDayOfMonth() == 1) continue search;
      }
      while (!hours.values.get(current.getHour())) {
        if (!advanced) {
          current =
              resolve(current.toLocalDateTime().truncatedTo(ChronoUnit.HOURS), current.getZone());
          advanced = true;
        }
        current = current.plusHours(1);
        if (current.getHour() == 0) continue search;
      }
      while (!minutes.values.get(current.getMinute())) {
        if (!advanced) {
          current = current.truncatedTo(ChronoUnit.MINUTES);
          advanced = true;
        }
        current = current.plusMinutes(1);
        if (current.getMinute() == 0) continue search;
      }
      while (current.getSecond() != 0) {
        current = current.plusSeconds(1);
        advanced = true;
        if (current.getSecond() == 0) continue search;
      }
      return Optional.of(current.withZoneSameInstant(after.getZone()));
    }
    return Optional.empty();
  }

  private static Duration duration(String expression) {
    // Go accepts a sign on the whole duration, not on individual components.
    boolean negative = expression.startsWith("-");
    if (negative || expression.startsWith("+")) expression = expression.substring(1);
    if (expression.equals("0")) return Duration.ofSeconds(1);
    var pattern = Pattern.compile("((?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+))(ns|us|µs|μs|ms|s|m|h)");
    var matcher = pattern.matcher(expression);
    var nanos = BigDecimal.ZERO;
    int end = 0;
    while (matcher.find()) {
      if (matcher.start() != end) throw new IllegalArgumentException("Invalid cron duration");
      long scale =
          switch (matcher.group(2)) {
            case "ns" -> 1;
            case "us", "µs", "μs" -> 1000;
            case "ms" -> 1000000;
            case "s" -> 1000000000;
            case "m" -> 60000000000L;
            case "h" -> 3600000000000L;
            default -> throw new AssertionError();
          };
      nanos =
          nanos.add(
              new BigDecimal(matcher.group(1))
                  .multiply(BigDecimal.valueOf(scale))
                  .setScale(0, java.math.RoundingMode.DOWN));
      end = matcher.end();
    }
    if (end == 0 || end != expression.length())
      throw new IllegalArgumentException("Invalid cron duration");
    try {
      long value = (negative ? nanos.negate() : nanos).longValueExact();
      // Go's @every schedules drop fractional seconds and round delays below one second up.
      return Duration.ofSeconds(Math.max(1, value / 1000000000));
    } catch (ArithmeticException error) {
      throw new IllegalArgumentException(
          "Cron duration must fit in signed 64-bit nanoseconds", error);
    }
  }

  private static Field field(String value, int min, int max, List<String> names) {
    var bits = new BitSet();
    boolean wildcard = false;
    for (String term : value.toLowerCase(Locale.ROOT).split(",", -1)) {
      String[] stepped = term.split("/", -1);
      if (stepped.length > 2) throw new IllegalArgumentException("Invalid cron step");
      long step = stepped.length == 2 ? Long.parseLong(stepped[1]) : 1;
      if (step < 1) throw new IllegalArgumentException("Cron step must be positive");
      String[] range = stepped[0].split("-", -1);
      if (range.length > 2) throw new IllegalArgumentException("Invalid cron range");
      int start;
      int end;
      if (stepped[0].equals("*") || stepped[0].equals("?")) {
        start = min;
        end = max;
        wildcard |= step == 1;
      } else {
        start = number(range[0], names, min);
        end = range.length == 2 ? number(range[1], names, min) : stepped.length == 2 ? max : start;
      }
      if (start < min || end > max || start > end)
        throw new IllegalArgumentException("Cron value out of range");
      for (int v = start; v <= end; v += (int) Math.min(step, max + 1L)) bits.set(v);
    }
    return new Field(bits, wildcard);
  }

  private static int number(String value, List<String> names, int offset) {
    int index = names.indexOf(value);
    return index < 0 ? Integer.parseInt(value) : index + offset;
  }

  private static ZonedDateTime resolve(LocalDateTime local, ZoneId zone) {
    // Go's time.Date looks up the offset at the civil time interpreted as UTC, then corrects
    // it at the resulting instant. Java's default gap/overlap resolution makes different
    // choices, including retaining an offset from a later day when resetting to midnight.
    var rules = zone.getRules();
    var offset = rules.getOffset(local.toInstant(ZoneOffset.UTC));
    offset = rules.getOffset(local.toInstant(offset));
    return local.toInstant(offset).atZone(zone);
  }

  private record Field(BitSet values, boolean wildcard) {}
}
