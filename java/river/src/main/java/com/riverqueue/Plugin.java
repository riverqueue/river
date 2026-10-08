package com.riverqueue;

import java.sql.Connection;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import tools.jackson.databind.JsonNode;

/**
 * Internal transactional storage seam for River Pro. This interface is released in lockstep with
 * River Pro and is not a stable application extension API; applications should use {@link
 * Extension}.
 */
public interface Plugin {
  default void afterInsert(Connection connection, Client.Driver driver, Job<JsonNode> job)
      throws Exception {}

  default void afterDelete(Connection connection, Client.Driver driver, Job<JsonNode> job)
      throws Exception {}

  default void afterStateChange(Connection connection, Client.Driver driver, Job<JsonNode> job)
      throws Exception {}

  default void afterAttempt(Connection connection, Client.Driver driver, Job<JsonNode> original)
      throws Exception {}

  default void afterPeerClaim(
      Connection connection, Client.Driver driver, List<Client.Decoded> jobs) throws Exception {}

  default List<Client.Decoded> claim(
      Connection connection,
      Client.Driver driver,
      Claim claim,
      Client.Transaction<List<Client.Decoded>> next)
      throws Exception {
    return next.run(connection);
  }

  default boolean clean(Client river, Instant now) {
    return false;
  }

  default boolean clean(
      Client river,
      Instant now,
      java.time.Duration cancelled,
      java.time.Duration completed,
      java.time.Duration discarded) {
    return clean(river, now);
  }

  default JsonNode decode(Job<JsonNode> job) {
    return job.args();
  }

  default void maintain(Client river, Instant now) {}

  default Insert prepare(Connection connection, Client.Driver driver, Insert insert)
      throws Exception {
    return insert;
  }

  default void producer(Connection connection, Client.Driver driver, Producer producer)
      throws Exception {}

  default void leadershipStarted() {}

  default int workerLimit(String queue, int configured) {
    return configured;
  }

  default void starting(boolean leadership) {}

  default void starting(
      boolean leadership,
      java.time.Duration cancelled,
      java.time.Duration completed,
      java.time.Duration discarded) {
    starting(leadership);
  }

  default boolean rescue(Client river, Rescue rescue) {
    return false;
  }

  record Rescue(
      Instant now,
      Instant horizon,
      Set<Long> activeIds,
      Set<String> kinds,
      boolean timeoutDisabled,
      RetryPolicy retryPolicy) {}

  record Claim(
      String clientId,
      String queue,
      int limit,
      Instant now,
      Set<String> kinds,
      boolean fetchOnlyKnownKinds) {}

  record Insert(JobType<?> type, String args, InsertOptions options) {}

  record Producer(
      String clientId,
      String queue,
      int maxWorkers,
      List<Job<?>> active,
      boolean paused,
      Instant now) {}
}
