package com.riverqueue;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import tools.jackson.databind.JsonNode;

/**
 * Job operations obtained from {@link Client#jobs()}. Connection-first overloads use the caller's
 * transaction and never commit or close it; other overloads own a transaction. Insert jobs through
 * {@link Client#insert(JobType, Object)} or {@link Client#insertMany(List)}.
 */
public final class Jobs {
  private final Clock clock;
  private final Client river;

  Jobs(Client river, Clock clock) {
    this.clock = clock;
    this.river = river;
  }

  /** Cancels a job, signalling a running attempt to stop. */
  public Job<JsonNode> cancel(long id) {
    return river.transaction(c -> cancel(c, id));
  }

  /** Cancels a job, signalling a running attempt to stop. Uses the caller's transaction. */
  public Job<JsonNode> cancel(Connection connection, long id) {
    return river.transaction(
        connection,
        c -> {
          var before = locked(c, id);
          if (before.state().isFinalized()) return before;
          try (var statement =
                  Sql.prepare(
                      c,
                      Sql.query(river.database(), "cancel"),
                      river.database().timestamp(clock.instant()),
                      Json.encode(clock.instant()),
                      id);
              var rows = statement.executeQuery()) {
            if (!rows.next()) return get(c, id);
            var job = river.read(rows);
            river.pilot().afterStateChange(c, river.driver(), job);
            river.notify(c, Protocol.cancel(id, job.queue()));
            return job;
          }
        });
  }

  /** Completes a running attempt in the caller's transaction, preserving its argument type. */
  public <A> Job<A> complete(Connection connection, Job<A> job) {
    return complete(connection, job, java.util.Map.of());
  }

  /** Completes a running attempt and merges metadata in the caller's transaction. */
  public <A> Job<A> complete(Connection connection, Job<A> job, Object metadata) {
    Objects.requireNonNull(job, "job");
    if (job.state() != Job.State.RUNNING) throw new IllegalArgumentException("Job must be running");
    var updates = metadata == null ? Json.object() : Json.tree(metadata);
    if (!updates.isObject()) throw new IllegalArgumentException("Metadata must be a JSON object");
    return river.transaction(
        connection,
        c -> {
          try (var statement =
                  Sql.prepare(
                      c,
                      Sql.query(river.database(), "complete"),
                      "completed",
                      river.database().timestamp(clock.instant()),
                      null,
                      "{}",
                      Json.encode(updates),
                      "completed",
                      river.database().timestamp(clock.instant()),
                      river.database().timestamp(job.scheduledAt()),
                      job.attempt(),
                      null,
                      null,
                      job.id(),
                      job.attempt(),
                      river.database().timestamp(job.attemptedAt()));
              var rows = statement.executeQuery()) {
            if (!rows.next()) return get(c, job.id()).mapArgs(ignored -> job.args());
            var completed = river.read(rows);
            river.pilot().afterStateChange(c, river.driver(), completed);
            return completed.mapArgs(ignored -> job.args());
          }
        });
  }

  /** Deletes a job, rejecting running jobs. */
  public Job<JsonNode> delete(long id) {
    return river.transaction(c -> delete(c, id));
  }

  /** Deletes a job, rejecting running jobs. Uses the caller's transaction. */
  public Job<JsonNode> delete(Connection connection, long id) {
    return river.transaction(
        connection,
        c -> {
          var job = locked(c, id);
          if (job.state() == Job.State.RUNNING)
            throw new RiverException(RiverException.Code.REJECTED, "Cannot delete a running job");
          try (var statement = Sql.prepare(c, Sql.query(river.database(), "delete"), id);
              var rows = statement.executeQuery()) {
            rows.next();
            var deleted = river.read(rows);
            river.pilot().afterDelete(c, river.driver(), deleted);
            return deleted;
          }
        });
  }

  /**
   * Deletes a page of matching non-running jobs in query order. Running jobs do not count toward
   * the limit. Concurrent changes that stop selected jobs matching the filters or cursor can reduce
   * the returned batch size. Unfiltered deletion requires {@code all=true}.
   */
  public List<Job<JsonNode>> deleteMany(JobQuery query, boolean all) {
    return river.transaction(connection -> deleteMany(connection, query, all));
  }

  /** Deletes matching non-running jobs atomically within the caller's transaction. */
  public List<Job<JsonNode>> deleteMany(Connection connection, JobQuery query, boolean all) {
    if (!all && !query.hasFilters())
      throw new IllegalArgumentException("Bulk deletion requires filters or explicit all=true");
    return river.transaction(
        connection,
        c -> {
          // Reserve SQLite's writer before listing so another commit cannot invalidate the
          // snapshot.
          if (river.database().dialect() == Database.Dialect.SQLITE)
            try (var statement = Sql.prepare(c, Sql.query(river.database(), "writer_lock"))) {
              statement.executeUpdate();
            }
          var result = new ArrayList<Job<JsonNode>>();
          var jobs = list(c, query, true).jobs();
          var params = new ArrayList<Object>();
          String sql = query.deleteSql(river.database(), params);
          try (var statement = Sql.prepare(c, sql, params.toArray())) {
            for (var job : jobs) {
              statement.setLong(params.size() + 1, job.id());
              try (var rows = statement.executeQuery()) {
                // Recheck every filter atomically: a peer may have updated this job since listing.
                if (!rows.next()) continue;
                var deleted = river.read(rows);
                river.pilot().afterDelete(c, river.driver(), deleted);
                result.add(deleted);
              }
            }
          }
          return List.copyOf(result);
        });
  }

  /** Loads a job with its arguments as JSON. */
  public Job<JsonNode> get(long id) {
    return river.transaction(c -> get(c, id));
  }

  /** Loads and decodes a job, rejecting a kind that does not match the supplied type. */
  public <A> Job<A> get(long id, JobType<A> type) {
    return river.transaction(connection -> get(connection, id, type));
  }

  /** Loads and decodes a job using the caller's connection. */
  public <A> Job<A> get(Connection connection, long id, JobType<A> type) {
    var job = get(connection, id);
    if (!job.kind().equals(type.kind()))
      throw new IllegalArgumentException(
          "Expected job kind " + type.kind() + ", got " + job.kind());
    return river.typed(job, type);
  }

  /** Loads a job with its arguments as JSON. Uses the caller's transaction. */
  public Job<JsonNode> get(Connection connection, long id) {
    return river.transaction(
        connection,
        transaction -> {
          try (var statement = Sql.prepare(transaction, Sql.query(river.database(), "get"), id);
              var rows = statement.executeQuery()) {
            if (!rows.next())
              throw new RiverException(RiverException.Code.NOT_FOUND, "Job " + id + " not found");
            return river.read(rows);
          } catch (SQLException e) {
            throw Client.databaseError("Get job", e);
          }
        });
  }

  /** Lists a page of jobs matching the query. */
  public JobQuery.Page list(JobQuery query) {
    return river.transaction(c -> list(c, query));
  }

  /** Lists a page of jobs matching the query. Uses the caller's transaction. */
  public JobQuery.Page list(Connection connection, JobQuery query) {
    return list(connection, query, false);
  }

  private JobQuery.Page list(Connection connection, JobQuery query, boolean forDelete) {
    return river.transaction(
        connection,
        transaction -> {
          var params = new ArrayList<Object>();
          String sql = query.sql(river.database(), params, forDelete);
          try (var statement = Sql.prepare(transaction, sql, params.toArray());
              var rows = statement.executeQuery()) {
            var jobs = new ArrayList<Job<JsonNode>>();
            while (rows.next()) jobs.add(river.read(rows));
            return new JobQuery.Page(
                jobs.isEmpty() ? null : query.cursor(jobs.getLast()), List.copyOf(jobs));
          } catch (SQLException e) {
            throw Client.databaseError("List jobs", e);
          }
        });
  }

  private Job<JsonNode> locked(Connection connection, long id) throws SQLException {
    if (river.database().dialect() == Database.Dialect.SQLITE)
      try (var statement = Sql.prepare(connection, Sql.query(river.database(), "writer_lock"))) {
        statement.executeUpdate();
      }
    try (var statement = Sql.prepare(connection, Sql.query(river.database(), "lock_get"), id);
        var rows = statement.executeQuery()) {
      if (!rows.next())
        throw new RiverException(RiverException.Code.NOT_FOUND, "Job " + id + " not found");
      return river.read(rows);
    }
  }

  /** Makes a job available for another attempt. */
  public Job<JsonNode> retry(long id) {
    return river.transaction(c -> retry(c, id));
  }

  /** Makes a job available for another attempt. Uses the caller's transaction. */
  public Job<JsonNode> retry(Connection connection, long id) {
    return river.transaction(
        connection,
        c -> {
          var before = locked(c, id);
          try (var statement =
                  Sql.prepare(
                      c,
                      Sql.query(river.database(), "retry"),
                      river.database().timestamp(clock.instant()),
                      id,
                      river.database().timestamp(clock.instant()));
              var rows = statement.executeQuery()) {
            boolean updated = rows.next();
            var job = updated ? river.read(rows) : before;
            river.pilot().afterStateChange(c, river.driver(), job);
            if (updated) river.notifyInsert(c, job.queue());
            return job;
          }
        });
  }

  /**
   * Merges job metadata and sets recorded output immediately, returning the updated job. Unlike
   * {@link WorkContext#output(Object)}, this writes before the attempt finishes. Other metadata
   * keys are preserved. Throws {@link RiverException} with code {@code NOT_FOUND} for a missing
   * job.
   */
  public Job<JsonNode> update(long id, JobUpdateOptions options) {
    return river.transaction(connection -> update(connection, id, options));
  }

  /**
   * Applies job updates in a caller-owned transaction using a savepoint. The connection must have
   * auto-commit disabled; the caller owns commit or rollback. Empty options leave the job
   * unchanged. Throws {@link RiverException} with code {@code NOT_FOUND} for a missing job.
   */
  public Job<JsonNode> update(Connection connection, long id, JobUpdateOptions options) {
    var updates = Objects.requireNonNull(options, "options").metadataUpdates();
    if (updates.isEmpty()) return get(connection, id);
    return river.transaction(
        connection,
        c -> {
          try (var statement =
                  Sql.prepare(c, Sql.query(river.database(), "update"), Json.encode(updates), id);
              var rows = statement.executeQuery()) {
            if (!rows.next())
              throw new RiverException(RiverException.Code.NOT_FOUND, "Job " + id + " not found");
            return river.read(rows);
          }
        });
  }
}
