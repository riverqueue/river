package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

class ScheduleTest {
  @TestFactory
  Stream<DynamicTest> goGoldens() throws Exception {
    var tests = new ArrayList<DynamicTest>();
    try (var input = getClass().getResourceAsStream("/fixtures/maintenance_values.json")) {
      var fixture = Json.MAPPER.readTree(input);
      for (String group : new String[] {"cron_cases", "cron_named_zone_cases"})
        for (var value : fixture.path(group)) {
          tests.add(
              DynamicTest.dynamicTest(
                  value.path("name").asString(),
                  () -> {
                    var schedule = Schedule.cron(value.path("expression").asString());
                    var from = OffsetDateTime.parse(value.path("from").asString());
                    for (var expected : value.path("next")) {
                      from = schedule.next(from).orElseThrow();
                      assertEquals(
                          OffsetDateTime.parse(expected.asString()).toInstant(), from.toInstant());
                      if (group.equals("cron_cases"))
                        assertEquals(
                            OffsetDateTime.parse(expected.asString()).getOffset(),
                            from.getOffset());
                    }
                  }));
        }
      for (var invalid : fixture.path("cron_invalid"))
        tests.add(
            DynamicTest.dynamicTest(
                "reject " + invalid.asString(),
                () ->
                    assertThrows(RuntimeException.class, () -> Schedule.cron(invalid.asString()))));
    }
    return tests.stream();
  }
}
