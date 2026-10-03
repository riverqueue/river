package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class ScheduleTest {
  @ParameterizedTest
  @CsvSource({
    "0 0 1 11 *,2020-10-10T18:06:09Z,2020-11-01T04:00:00Z",
    "0 0 1 11 *,2026-10-30T04:34:24Z,2026-11-01T04:00:00Z",
    "0 0 1 * *,2026-10-31T14:00:00Z,2026-11-01T04:00:00Z",
    "0 0 * * sun,2026-10-31T14:00:00Z,2026-11-01T04:00:00Z"
  })
  void calendarAdvanceKeepsBothMidnightsDuringOverlap(
      String expression, String reference, String expected) {
    var from = OffsetDateTime.parse(reference);
    var first = OffsetDateTime.parse(expected);
    var zone = ZoneId.of("America/Havana");
    var explicit = Schedule.cron("CRON_TZ=" + zone + " " + expression);
    var local = Schedule.cron(expression);
    assertEquals(first, explicit.next(from).orElseThrow());
    assertEquals(first.plusHours(1), explicit.next(first).orElseThrow());
    assertEquals(
        first.atZoneSameInstant(zone), local.next(from.atZoneSameInstant(zone)).orElseThrow());
    assertEquals(
        first.plusHours(1).atZoneSameInstant(zone),
        local.next(first.atZoneSameInstant(zone)).orElseThrow());
  }

  @ParameterizedTest
  @CsvSource({
    "Australia/Lord_Howe,0 1 * * *,2026-10-03T14:30:00Z,2026-10-04T14:00:00Z",
    "Australia/Lord_Howe,0 0 * * sun,2026-04-04T13:00:00Z,2026-04-18T13:30:00Z",
    "Antarctica/Troll,0 2 * * *,2026-10-24T23:00:00Z,2026-10-25T02:00:00Z",
    "Antarctica/Troll,0 1 * * *,2026-10-25T00:00:00Z,2026-10-26T01:00:00Z",
    "America/Havana,0 0 1 11 *,2011-12-16T02:11:04Z,2013-11-01T04:00:00Z"
  })
  void dstFieldAdvancementMatchesGo(
      String zoneName, String expression, String reference, String expected) {
    var from = OffsetDateTime.parse(reference);
    var next = OffsetDateTime.parse(expected);
    var zone = ZoneId.of(zoneName);
    assertTimeoutPreemptively(
        Duration.ofSeconds(2),
        () -> {
          assertEquals(
              next, Schedule.cron("CRON_TZ=" + zone + " " + expression).next(from).orElseThrow());
          assertEquals(
              next.atZoneSameInstant(zone),
              Schedule.cron(expression).next(from.atZoneSameInstant(zone)).orElseThrow());
        });
  }

  @ParameterizedTest
  @CsvSource({
    "0,1",
    "+0,1",
    "-0,1",
    "-1h90m,1",
    "+1h30m,5400",
    "1.5s,1",
    "9223372036854775807ns,9223372036",
    "-9223372036854775808ns,1"
  })
  void everyDurationMatchesGo(String value, long seconds) {
    var from = OffsetDateTime.parse("2026-01-02T03:04:05.123456789Z");
    assertEquals(
        from.withNano(0).plusSeconds(seconds),
        Schedule.cron("@every " + value).next(from).orElseThrow());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"1h-30m", "1h+30m", "9223372036854775808ns", "-9223372036854775809ns", "2562048h"})
  void everyRejectsInvalidGoDurations(String value) {
    assertThrows(IllegalArgumentException.class, () -> Schedule.cron("@every " + value));
  }

  @ParameterizedTest
  @ValueSource(strings = {"CRON_TZ=", "TZ="})
  void explicitZoneOverridesTheReferenceZone(String prefix) {
    var schedule = Schedule.cron(prefix + "UTC 0 9 * * *");
    var from = ZonedDateTime.parse("2026-03-07T03:00:00-05:00[America/New_York]");
    var first = schedule.next(from).orElseThrow();
    assertEquals(ZonedDateTime.parse("2026-03-07T04:00:00-05:00[America/New_York]"), first);
    assertEquals(
        ZonedDateTime.parse("2026-03-08T05:00:00-04:00[America/New_York]"),
        schedule.next(first).orElseThrow());
  }

  @TestFactory
  @Tag("conformance")
  Stream<DynamicTest> goFixtures() throws Exception {
    var tests = new ArrayList<DynamicTest>();
    var fixture = Conformance.fixture("cron_schedules.json");
    for (String group : new String[] {"cron_cases", "cron_named_zone_cases"})
      for (var value : fixture.required(group)) {
        tests.add(
            DynamicTest.dynamicTest(
                value.required("name").asString(),
                () -> {
                  var schedule = Schedule.cron(value.required("expression").asString());
                  var from = OffsetDateTime.parse(value.required("from").asString());
                  if (value.required("next").isEmpty()) assertTrue(schedule.next(from).isEmpty());
                  for (var expected : value.required("next")) {
                    from = schedule.next(from).orElseThrow();
                    assertEquals(
                        OffsetDateTime.parse(expected.asString()).toInstant(), from.toInstant());
                    if (group.equals("cron_cases"))
                      assertEquals(
                          OffsetDateTime.parse(expected.asString()).getOffset(), from.getOffset());
                  }
                }));
      }
    for (var invalid : fixture.required("cron_invalid"))
      tests.add(
          DynamicTest.dynamicTest(
              "reject " + invalid.asString(),
              () -> assertThrows(RuntimeException.class, () -> Schedule.cron(invalid.asString()))));
    return tests.stream();
  }

  @ParameterizedTest
  @CsvSource({
    "0 2 * * *,2026-04-04T13:45:00Z,2026-04-05T15:30:00Z",
    "0 2 * * *,2026-04-04T14:15:00Z,2026-04-05T15:30:00Z",
    "0 2 * * *,2026-04-04T14:45:00Z,2026-04-05T15:30:00Z",
    "0 3 * * *,2026-04-04T14:15:00Z,2026-04-05T16:30:00Z",
    "0 2 * * *,2026-10-03T14:15:00Z,2026-10-04T15:00:00Z",
    "0 2 * * *,2026-10-03T14:45:00Z,2026-10-04T15:00:00Z"
  })
  void halfHourDstMatchesGo(String expression, String reference, String expected) {
    // robfig/cron skips these transition-day occurrences while stepping by whole hours.
    assertTimeoutPreemptively(
        Duration.ofSeconds(2),
        () -> {
          var from = OffsetDateTime.parse(reference);
          var zone = ZoneId.of("Australia/Lord_Howe");
          assertEquals(
              OffsetDateTime.parse(expected),
              Schedule.cron("CRON_TZ=" + zone + " " + expression).next(from).orElseThrow());
          assertEquals(
              OffsetDateTime.parse(expected).toInstant(),
              Schedule.cron(expression)
                  .next(from.atZoneSameInstant(zone))
                  .orElseThrow()
                  .toInstant());
        });
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void intervalsUseElapsedTimeAcrossDst(boolean cron) {
    var schedule = cron ? Schedule.cron("@every 24h") : Schedule.every(Duration.ofDays(1));
    var from = ZonedDateTime.parse("2026-03-07T09:00:00-05:00[America/New_York]");
    assertEquals(
        ZonedDateTime.parse("2026-03-08T10:00:00-04:00[America/New_York]"),
        schedule.next(from).orElseThrow());
  }

  @ParameterizedTest
  @ValueSource(longs = {2147483647L, 2147483648L, Long.MAX_VALUE})
  void largeStepsDoNotOverflow(long step) {
    var from = OffsetDateTime.parse("2026-01-02T03:04:05Z");
    assertEquals(
        OffsetDateTime.parse("2026-01-02T03:05:00Z"),
        Schedule.cron("5/" + step + " * * * *").next(from).orElseThrow());
  }

  @TestFactory
  @Tag("conformance")
  Stream<DynamicTest> namedReferenceZonesMatchGoFixtures() throws Exception {
    var tests = new ArrayList<DynamicTest>();
    for (var value : Conformance.fixture("cron_schedules.json").required("cron_named_zone_cases")) {
      String expression = value.required("expression").asString();
      int separator = expression.indexOf(' ');
      var zone = ZoneId.of(expression.substring(expression.indexOf('=') + 1, separator));
      tests.add(
          DynamicTest.dynamicTest(
              value.required("name").asString(),
              () -> {
                // Go treats an unqualified expression as local to the reference time's zone.
                var schedule = Schedule.cron(expression.substring(separator + 1));
                var from =
                    OffsetDateTime.parse(value.required("from").asString()).atZoneSameInstant(zone);
                for (var expected : value.required("next")) {
                  from = schedule.next(from).orElseThrow();
                  assertEquals(
                      OffsetDateTime.parse(expected.asString()).toInstant(), from.toInstant());
                  assertEquals(zone, from.getZone());
                }
              }));
    }
    return tests.stream();
  }

  @ParameterizedTest
  @CsvSource({"0 0 * * mon,2012-01-01T10:00:00Z", "0 0 30 12 *,2012-12-29T10:00:00Z"})
  void skippedDateDoesNotStall(String expression, String expected) {
    var from = OffsetDateTime.parse("2011-12-28T11:50:16Z");
    var zone = ZoneId.of("Pacific/Apia");
    var next = OffsetDateTime.parse(expected);
    assertTimeoutPreemptively(
        Duration.ofSeconds(2),
        () -> {
          assertEquals(
              next, Schedule.cron("CRON_TZ=" + zone + " " + expression).next(from).orElseThrow());
          assertEquals(
              next.atZoneSameInstant(zone),
              Schedule.cron(expression).next(from.atZoneSameInstant(zone)).orElseThrow());
        });
  }
}
