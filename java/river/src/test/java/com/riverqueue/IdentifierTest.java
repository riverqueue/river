package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class IdentifierTest {
  private static final JobType<String> TYPE = JobType.of("identifier_test", String.class);
  private final Workers.Builder workers =
      new Client(Database.connect("jdbc:sqlite::memory:")).workers();

  @ParameterizedTest
  @ValueSource(
      strings = {
        "ab",
        "_0",
        "kind123",
        "with.dot",
        "with:colon",
        "with+plus",
        "with-hyphen",
        "with_underscore",
        "with[brackets]",
        "with<triangle_brackets>",
        "with/slash",
        "JobArgsReflectKind[github.com/riverqueue/river.JobArgs·12]"
      })
  void acceptsSharedKindAndPeriodicIdSyntax(String value) {
    assertDoesNotThrow(() -> JobType.of(value, String.class));
    assertDoesNotThrow(() -> periodic(value));
  }

  @Test
  void clientIdLengthCountsUtf8Bytes() {
    assertDoesNotThrow(() -> workers.id("x".repeat(100)));
    assertThrows(IllegalArgumentException.class, () -> workers.id("x".repeat(101)));
    assertDoesNotThrow(() -> workers.id("é".repeat(50)));
    assertThrows(IllegalArgumentException.class, () -> workers.id("é".repeat(51)));
    assertDoesNotThrow(() -> workers.id("😀".repeat(25)));
    assertThrows(IllegalArgumentException.class, () -> workers.id("😀".repeat(26)));
  }

  @Test
  void duplicatePeriodicIdsAreRejected() {
    periodic("daily-report");
    assertThrows(IllegalArgumentException.class, () -> periodic("daily-report"));
  }

  @Test
  void jobKindLengthCountsCharacters() {
    assertDoesNotThrow(() -> JobType.of("k".repeat(127), String.class));
    assertThrows(IllegalArgumentException.class, () -> JobType.of("k".repeat(128), String.class));
    assertDoesNotThrow(() -> JobType.of("k" + "·".repeat(126), String.class));
    assertThrows(
        IllegalArgumentException.class, () -> JobType.of("k" + "·".repeat(127), String.class));
  }

  @Test
  void periodicIdLengthCountsUtf8Bytes() {
    assertDoesNotThrow(() -> periodic("p".repeat(127)));
    assertThrows(IllegalArgumentException.class, () -> periodic("p".repeat(128)));
    assertDoesNotThrow(() -> periodic("p" + "·".repeat(63)));
    assertThrows(IllegalArgumentException.class, () -> periodic("pp" + "·".repeat(63)));
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(
      strings = {
        "a",
        "_",
        " ",
        "daily report",
        "daily,report",
        ":daily",
        "·daily",
        "daily!report",
        "daily\nreport",
        "daily-report\n",
        "daily\treport",
        "dáily",
        "éxample",
        "daily😀"
      })
  void rejectsInvalidKindAndPeriodicIdSyntax(String value) {
    if (value != null)
      assertThrows(IllegalArgumentException.class, () -> JobType.of(value, String.class));
    assertThrows(IllegalArgumentException.class, () -> periodic(value));
  }

  private void periodic(String id) {
    workers.periodic(
        id, Schedule.every(Duration.ofDays(1)), TYPE, "args", InsertOptions.defaults(), false);
  }
}
