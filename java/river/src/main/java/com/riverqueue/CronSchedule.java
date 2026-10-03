package com.riverqueue;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneId;
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

  @Override
  public Optional<OffsetDateTime> next(OffsetDateTime after) {
    if (interval != null) return Optional.of(after.truncatedTo(ChronoUnit.SECONDS).plus(interval));
    var current =
        after
            .atZoneSameInstant(zone == null ? after.getOffset() : zone)
            .truncatedTo(ChronoUnit.MINUTES)
            .plusMinutes(1);
    int limit = current.getYear() + 5;
    while (current.getYear() <= limit) {
      if (!months.values.get(current.getMonthValue())) {
        current = current.plusMonths(1).withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS);
        continue;
      }
      boolean day = days.values.get(current.getDayOfMonth());
      boolean weekday = weekdays.values.get(current.getDayOfWeek().getValue() % 7);
      if (!(days.wildcard || weekdays.wildcard ? day && weekday : day || weekday)) {
        current = current.plusDays(1).truncatedTo(ChronoUnit.DAYS);
        continue;
      }
      if (!hours.values.get(current.getHour())) {
        current = current.plusHours(1).truncatedTo(ChronoUnit.HOURS);
        continue;
      }
      if (!minutes.values.get(current.getMinute())) {
        current = current.plusMinutes(1);
        continue;
      }
      return Optional.of(current.toOffsetDateTime().withOffsetSameInstant(after.getOffset()));
    }
    return Optional.empty();
  }

  private static Duration duration(String expression) {
    var pattern =
        Pattern.compile("([+-]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+))(ns|us|µs|μs|ms|s|m|h)");
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
      nanos = nanos.add(new BigDecimal(matcher.group(1)).multiply(BigDecimal.valueOf(scale)));
      end = matcher.end();
    }
    if (end == 0 || end != expression.length())
      throw new IllegalArgumentException("Invalid cron duration");
    return Duration.ofSeconds(
        Math.max(1, nanos.divideToIntegralValue(BigDecimal.valueOf(1000000000)).longValueExact()));
  }

  private static Field field(String value, int min, int max, List<String> names) {
    var bits = new BitSet();
    boolean wildcard = false;
    for (String term : value.toLowerCase(Locale.ROOT).split(",", -1)) {
      String[] stepped = term.split("/", -1);
      if (stepped.length > 2) throw new IllegalArgumentException("Invalid cron step");
      int step = stepped.length == 2 ? Integer.parseInt(stepped[1]) : 1;
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
      for (int v = start; v <= end; v += step) bits.set(v);
    }
    return new Field(bits, wildcard);
  }

  private static int number(String value, List<String> names, int offset) {
    int index = names.indexOf(value);
    return index < 0 ? Integer.parseInt(value) : index + offset;
  }

  private record Field(BitSet values, boolean wildcard) {}
}
