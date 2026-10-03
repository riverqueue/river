package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.JsonNode;

class BulkDeleteTest {
  private static final JobType<String> TYPE = JobType.of("bulk_delete", String.class);
  @TempDir Path directory;
  @RegisterExtension final TestDatabase databases = new TestDatabase();

  private Client client() {
    return new Client(databases.open(directory.resolve("river.db")));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "ids",
        "kinds",
        "metadata",
        "priorities",
        "queues",
        "states",
        "tagsAll",
        "tagsAny"
      })
  void acceptsEachFilter(String filter) {
    var client = client();
    var matching =
        client
            .insert(
                TYPE,
                "matching",
                InsertOptions.builder()
                    .metadata(Map.of("tenant", "chosen"))
                    .pending(true)
                    .priority(1)
                    .queue("chosen")
                    .tags("chosen")
                    .build())
            .job();
    var other =
        client
            .insert(
                JobType.of("other", String.class),
                "other",
                InsertOptions.builder()
                    .metadata(Map.of("tenant", "other"))
                    .priority(4)
                    .tags("other")
                    .build())
            .job();
    var query = JobQuery.builder();
    switch (filter) {
      case "ids" -> query.ids(matching.id());
      case "kinds" -> query.kinds(TYPE.kind());
      case "metadata" -> query.metadata(Map.of("tenant", "chosen"));
      case "priorities" -> query.priorities(1);
      case "queues" -> query.queues("chosen");
      case "states" -> query.states(Job.State.PENDING);
      case "tagsAll" -> query.tagsAll("chosen");
      case "tagsAny" -> query.tagsAny("chosen");
      default -> throw new AssertionError(filter);
    }
    assertEquals(
        List.of(matching.id()),
        client.deleteMany(query.build(), false).stream().map(Job::id).toList());
    assertEquals(
        List.of(other.id()), client.list(JobQuery.all()).jobs().stream().map(Job::id).toList());
  }

  @Test
  void callerRollbackRestoresDeletedJobs() throws Exception {
    var client = client();
    var job = client.insert(TYPE, "rollback").job();
    try (var connection = client.database().connection()) {
      connection.setAutoCommit(false);
      assertEquals(
          1, client.deleteMany(connection, JobQuery.builder().ids(job.id()).build(), false).size());
      assertTrue(client.list(connection, JobQuery.all()).jobs().isEmpty());
      connection.rollback();
      assertEquals(job.id(), client.get(job.id()).id());
    }
  }

  @Test
  void hookFailureRollsBackWholeBatch() throws Exception {
    var base = client();
    var first = base.insert(TYPE, "first").job();
    var second = base.insert(TYPE, "second").job();
    var client =
        base.withPlugin(
            new Plugin() {
              @Override
              public void afterDelete(
                  Connection connection, Client.Driver driver, Job<JsonNode> job) {
                if (job.id() == second.id()) throw new IllegalStateException("reject deletion");
              }
            });
    try (var connection = client.database().connection()) {
      connection.setAutoCommit(false);
      assertThrows(
          IllegalStateException.class,
          () ->
              client.deleteMany(connection, JobQuery.builder().kinds(TYPE.kind()).build(), false));
      assertEquals(
          List.of(first.id(), second.id()),
          client.list(connection, JobQuery.all()).jobs().stream().map(Job::id).toList());
      connection.commit();
    }
    assertEquals(2, base.list(JobQuery.all()).jobs().size());
  }

  @ParameterizedTest
  @Tag("postgres")
  @ValueSource(strings = {"claim", "delete"})
  void ignoresJobsChangedAfterListing(String change) {
    var base = client();
    assumeTrue(base.database().dialect() == Database.Dialect.POSTGRES);
    var first = base.insert(TYPE, "first").job();
    var raced = base.insert(TYPE, "raced").job();
    var last = base.insert(TYPE, "last").job();
    var deleted = new ArrayList<Long>();
    var client =
        base.withPlugin(
            new Plugin() {
              @Override
              public void afterDelete(
                  Connection connection, Client.Driver driver, Job<JsonNode> job) {
                deleted.add(job.id());
                if (job.id() != first.id()) return;
                // The batch has listed all three rows. Commit a peer's change before it reaches the
                // second.
                if (change.equals("delete")) base.delete(raced.id());
                else
                  base.transaction(
                      peer -> {
                        try (var statement =
                            Sql.prepare(
                                peer,
                                "UPDATE "
                                    + base.database().prefix()
                                    + "river_job SET state='running', attempt=1, attempted_at=? WHERE id=?",
                                base.database().timestamp(Instant.now()),
                                raced.id())) {
                          assertEquals(1, statement.executeUpdate());
                        }
                        return null;
                      });
              }
            });
    assertEquals(
        List.of(first.id(), last.id()),
        client.deleteMany(JobQuery.builder().kinds(TYPE.kind()).build(), false).stream()
            .map(Job::id)
            .toList());
    assertEquals(List.of(first.id(), last.id()), deleted);
    if (change.equals("claim")) assertEquals(Job.State.RUNNING, base.get(raced.id()).state());
    else assertTrue(base.list(JobQuery.all()).jobs().isEmpty());
  }

  @ParameterizedTest
  @Tag("postgres")
  @ValueSource(
      strings = {
        "cursor",
        "kinds",
        "metadata",
        "priorities",
        "queues",
        "states",
        "tagsAll",
        "tagsAny"
      })
  void preservesJobsThatNoLongerMatchFilters(String change) {
    var base = client();
    assumeTrue(base.database().dialect() == Database.Dialect.POSTGRES);
    var options =
        InsertOptions.builder()
            .metadata(Map.of("tenant", "chosen"))
            .priority(1)
            .queue("chosen")
            .scheduledAt(Instant.parse("2026-01-02T00:00:00Z"))
            .tags("all", "any")
            .build();
    var boundary =
        base.insert(
                TYPE,
                "boundary",
                InsertOptions.builder().scheduledAt(Instant.parse("2026-01-01T00:00:00Z")).build())
            .job();
    var first = base.cancel(base.insert(TYPE, "first", options).job().id());
    var raced = base.cancel(base.insert(TYPE, "raced", options).job().id());
    var last = base.cancel(base.insert(TYPE, "last", options).job().id());
    var query =
        JobQuery.builder()
            .kinds(TYPE.kind())
            .metadata(Map.of("tenant", "chosen"))
            .order(JobQuery.Order.SCHEDULED_AT)
            .priorities(1)
            .queues("chosen")
            .states(Job.State.CANCELLED)
            .tagsAll("all")
            .tagsAny("any")
            .build();
    var deleted = new ArrayList<Long>();
    var client =
        base.withPlugin(
            new Plugin() {
              @Override
              public void afterDelete(
                  Connection connection, Client.Driver driver, Job<JsonNode> job) {
                deleted.add(job.id());
                if (job.id() != first.id()) return;
                // Commit a peer's update after listing, before deletion reaches the second row.
                if (change.equals("states")) base.retry(raced.id());
                else
                  base.transaction(
                      peer -> {
                        String assignment =
                            switch (change) {
                              case "cursor" -> "scheduled_at='2025-12-31T00:00:00Z'";
                              case "kinds" -> "kind='other'";
                              case "metadata" -> "metadata='{\"tenant\":\"other\"}'::jsonb";
                              case "priorities" -> "priority=4";
                              case "queues" -> "queue='other'";
                              case "tagsAll" -> "tags=ARRAY['any']";
                              case "tagsAny" -> "tags=ARRAY['all']";
                              default -> throw new AssertionError(change);
                            };
                        try (var statement =
                            Sql.prepare(
                                peer,
                                "UPDATE "
                                    + base.database().prefix()
                                    + "river_job SET "
                                    + assignment
                                    + " WHERE id=?",
                                raced.id())) {
                          assertEquals(1, statement.executeUpdate());
                        }
                        return null;
                      });
              }
            });
    assertEquals(
        List.of(first.id(), last.id()),
        client.deleteMany(query.after(query.cursor(boundary)), false).stream()
            .map(Job::id)
            .toList());
    assertEquals(List.of(first.id(), last.id()), deleted);
    assertEquals(
        change.equals("states") ? Job.State.AVAILABLE : Job.State.CANCELLED,
        base.get(raced.id()).state());
  }

  @Test
  void rejectsUnfilteredDeletionUnlessExplicit() {
    var client = client();
    var job = client.insert(TYPE, "kept").job();
    assertThrows(IllegalArgumentException.class, () -> client.deleteMany(JobQuery.all(), false));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            client.deleteMany(
                JobQuery.builder()
                    .metadata(Map.of())
                    .tagsAll()
                    .tagsAny()
                    .priorities()
                    .limit(1)
                    .descending(true)
                    .build(),
                false));
    assertEquals(job.id(), client.get(job.id()).id());
    assertEquals(1, client.deleteMany(JobQuery.all(), true).size());
  }

  @Test
  void skipsRunningJobsAndPreservesSingleDeleteRejection() {
    var client = client();
    var running = client.insert(TYPE, "running").job();
    var available = client.insert(TYPE, "available").job();
    client.transaction(
        connection -> {
          try (var statement =
              Sql.prepare(
                  connection,
                  "UPDATE "
                      + client.database().prefix()
                      + "river_job SET state='running', attempt=1, attempted_at=? WHERE id=?",
                  client.database().timestamp(Instant.now()),
                  running.id())) {
            statement.executeUpdate();
          }
          return null;
        });
    assertEquals(
        RiverException.Code.REJECTED,
        assertThrows(RiverException.class, () -> client.delete(running.id())).code());
    assertEquals(
        List.of(available.id()),
        client.deleteMany(JobQuery.builder().kinds(TYPE.kind()).limit(1).build(), false).stream()
            .map(Job::id)
            .toList());
    assertEquals(Job.State.RUNNING, client.get(running.id()).state());
  }
}
