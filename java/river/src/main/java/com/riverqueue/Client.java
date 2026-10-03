package com.riverqueue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import tools.jackson.databind.JsonNode;

/**
 * Transactional job storage. Connection overloads use the caller's transaction and never commit or
 * close it; other overloads own a transaction. Close the {@link Workers} returned by {@link
 * Workers.Builder#start()} to stop execution. The client owns no resources to close.
 */
public final class Client {
  private final java.util.concurrent.CopyOnWriteArrayList<Runnable> insertListeners =
      new java.util.concurrent.CopyOnWriteArrayList<>();
  private final ThreadLocal<Commit> ownedCommit = new ThreadLocal<>();
  private final Plugin plugin;
  private final Clock clock;
  private final Database database;
  private final List<Extension> extensions;

  public Client(Database database) {
    this(database, Clock.systemUTC());
  }

  public Client(Database database, Clock clock) {
    this(database, clock, List.of(), new Plugin() {});
  }

  private Client(Database database, Clock clock, List<Extension> extensions, Plugin plugin) {
    this.plugin = plugin;
    this.database = Objects.requireNonNull(database);
    this.clock = Objects.requireNonNull(clock);
    this.extensions = List.copyOf(extensions);
  }

  public Database database() {
    return database;
  }

  public Queues queues() {
    return new Queues(this);
  }

  public Workers.Builder workers() {
    return new Workers.Builder(this);
  }

  public Client withExtension(Extension extension) {
    var all = new ArrayList<>(extensions);
    all.add(extension);
    return new Client(database, clock, all, plugin);
  }

  /** Internal integration point used by River Pro; not a stable application API. */
  public Client withPlugin(Plugin plugin) {
    return new Client(database, clock, extensions, Objects.requireNonNull(plugin));
  }

  Runnable onInsertCommit(Runnable listener) {
    insertListeners.add(listener);
    return () -> insertListeners.remove(listener);
  }

  private static final class Commit {
    final Connection connection;
    boolean inserted;

    Commit(Connection connection) {
      this.connection = connection;
    }
  }

  Plugin plugin() {
    return plugin;
  }

  /** Internal driver access for the matched River Pro release. */
  public Driver driver() {
    return new Driver();
  }

  public final class Driver {
    private Driver() {}

    /** Internal conformance reset; deletes all core River data. */
    public void reset() {
      Client.this.reset();
    }

    /** Internal retention operation used by the conformance adapter. */
    public int deleteFinalized(
        Connection connection,
        Instant before,
        int limit,
        List<String> excluded,
        List<String> included) {
      return Client.this.deleteFinalized(connection, before, limit, excluded, included);
    }

    public Object timestamp(Instant value) {
      return database.timestamp(value);
    }

    public String prefix() {
      return database.prefix();
    }

    public Job<JsonNode> read(ResultSet rows) throws SQLException {
      return Client.this.read(rows);
    }

    public Decoded readPartial(ResultSet rows) throws SQLException {
      return Client.this.readPartial(rows);
    }

    public String query(String name) {
      return Sql.query(database, name);
    }

    public java.sql.PreparedStatement prepare(Connection c, String sql, Object... params)
        throws SQLException {
      return Sql.prepare(c, sql, params);
    }

    public void notify(Connection c, String topic, String payload) throws SQLException {
      Client.this.notify(c, topic, payload);
    }

    public Database database() {
      return database;
    }

    /**
     * Reinsert a persisted job through insertion middleware, preserving its unique key and dates.
     */
    public Job.InsertResult<JsonNode> reinsert(Connection connection, Job<JsonNode> original) {
      var type = JobType.of(original.kind(), JsonNode.class);
      var options =
          new InsertOptions(
              original.maxAttempts(),
              original.metadata(),
              false,
              original.priority(),
              original.queue(),
              original.scheduledAt(),
              original.tags(),
              Unique.none());
      return atomic(
          connection,
          c ->
              insertMiddleware(
                  c,
                  0,
                  () -> {
                    for (var extension : extensions)
                      extension.beforeInsert(c, type, original.args(), options);
                    return insertOne(c, type, original.args(), options, original);
                  }));
    }
  }

  List<Extension> extensions() {
    return extensions;
  }

  public Job<JsonNode> cancel(long id) {
    return transaction(c -> cancel(c, id));
  }

  public Job<JsonNode> cancel(Connection connection, long id) {
    return atomic(
        connection,
        c -> {
          var before = locked(c, id);
          if (before.state().isFinalized()) return before;
          try (var statement =
                  Sql.prepare(
                      c,
                      Sql.query(database, "cancel"),
                      database.timestamp(clock.instant()),
                      Json.encode(clock.instant()),
                      id);
              var rows = statement.executeQuery()) {
            if (!rows.next()) return get(c, id);
            var job = read(rows);
            plugin.afterStateChange(c, driver(), job);
            notify(
                c,
                "river_control",
                Json.encode(
                    java.util.Map.of("action", "cancel", "job_id", id, "queue", job.queue())));
            return job;
          }
        });
  }

  public Job<JsonNode> delete(long id) {
    return transaction(c -> delete(c, id));
  }

  public Job<JsonNode> delete(Connection connection, long id) {
    return atomic(
        connection,
        c -> {
          var job = locked(c, id);
          if (job.state() == Job.State.RUNNING)
            throw new RiverException(RiverException.Code.REJECTED, "Cannot delete a running job");
          try (var statement = Sql.prepare(c, Sql.query(database, "delete"), id);
              var rows = statement.executeQuery()) {
            rows.next();
            var deleted = read(rows);
            plugin.afterDelete(c, driver(), deleted);
            return deleted;
          }
        });
  }

  private int deleteFinalized(
      Connection connection,
      Instant before,
      int limit,
      List<String> excluded,
      List<String> included) {
    if (limit < 1) throw new IllegalArgumentException("Cleaner limit must be positive");
    try (var statement =
        Sql.prepare(
            connection,
            Sql.query(database, "delete_finalized"),
            database.timestamp(before),
            Json.encode(excluded),
            included == null,
            Json.encode(included == null ? List.of() : included),
            limit)) {
      return statement.executeUpdate();
    } catch (SQLException e) {
      throw databaseError("Delete finalized jobs", e);
    }
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
    return atomic(
        connection,
        c -> {
          try (var statement =
                  Sql.prepare(
                      c,
                      Sql.query(database, "complete"),
                      "completed",
                      database.timestamp(clock.instant()),
                      "completed",
                      database.timestamp(clock.instant()),
                      database.timestamp(job.scheduledAt()),
                      job.attempt(),
                      Json.encode(updates),
                      null,
                      null,
                      job.id(),
                      job.attempt(),
                      database.timestamp(job.attemptedAt()));
              var rows = statement.executeQuery()) {
            if (!rows.next()) return get(c, job.id()).mapArgs(_ -> job.args());
            var completed = read(rows);
            plugin.afterStateChange(c, driver(), completed);
            return completed.mapArgs(_ -> job.args());
          }
        });
  }

  public List<Job<JsonNode>> deleteMany(JobQuery query, boolean all) {
    return transaction(connection -> deleteMany(connection, query, all));
  }

  public List<Job<JsonNode>> deleteMany(Connection connection, JobQuery query, boolean all) {
    if (!all
        && query.ids().isEmpty()
        && query.kinds().isEmpty()
        && query.queues().isEmpty()
        && query.states().isEmpty())
      throw new IllegalArgumentException("Bulk deletion requires filters or explicit all=true");
    return atomic(
        connection,
        c -> {
          var result = new ArrayList<Job<JsonNode>>();
          for (var job : list(c, query).jobs())
            if (job.state() != Job.State.RUNNING) result.add(delete(c, job.id()));
          return List.copyOf(result);
        });
  }

  public Job<JsonNode> get(long id) {
    return transaction(c -> get(c, id));
  }

  /** Loads and decodes a job, rejecting a kind that does not match the supplied type. */
  public <A> Job<A> get(long id, JobType<A> type) {
    return transaction(connection -> get(connection, id, type));
  }

  /** Loads and decodes a job using the caller's connection. */
  public <A> Job<A> get(Connection connection, long id, JobType<A> type) {
    var job = get(connection, id);
    if (!job.kind().equals(type.kind()))
      throw new IllegalArgumentException(
          "Expected job kind " + type.kind() + ", got " + job.kind());
    return typed(job, type);
  }

  public Job<JsonNode> get(Connection connection, long id) {
    return atomic(
        connection,
        transaction -> {
          try (var statement = Sql.prepare(transaction, Sql.query(database, "get"), id);
              var rows = statement.executeQuery()) {
            if (!rows.next())
              throw new RiverException(RiverException.Code.NOT_FOUND, "Job " + id + " not found");
            return read(rows);
          } catch (SQLException e) {
            throw databaseError("Get job", e);
          }
        });
  }

  public <A> Job.InsertResult<A> insert(JobType<A> type, A args) {
    return insert(type, args, InsertOptions.defaults());
  }

  public <A> Job.InsertResult<A> insert(JobType<A> type, A args, InsertOptions options) {
    return transaction(c -> insert(c, type, args, options));
  }

  public <A> Job.InsertResult<A> insert(Connection connection, JobType<A> type, A args) {
    return insert(connection, type, args, InsertOptions.defaults());
  }

  public <A> Job.InsertResult<A> insert(
      Connection connection, JobType<A> type, A args, InsertOptions options) {
    return atomic(
        connection,
        c -> {
          return insertMiddleware(
              c,
              0,
              () -> {
                for (var extension : extensions) extension.beforeInsert(c, type, args, options);
                return insertOne(c, type, args, options);
              });
        });
  }

  private <T> T insertMiddleware(
      Connection connection, int index, java.util.concurrent.Callable<T> next) throws Exception {
    return index == extensions.size()
        ? next.call()
        : extensions
            .get(index)
            .insert(connection, () -> insertMiddleware(connection, index + 1, next));
  }

  /** Inserts jobs of one kind atomically, inheriting the job type's defaults. */
  public <A> List<Job.InsertResult<A>> insertMany(JobType<A> type, List<A> args) {
    return insertMany(type, args, InsertOptions.defaults());
  }

  /** Inserts jobs of one kind with common options in a new transaction. */
  public <A> List<Job.InsertResult<A>> insertMany(
      JobType<A> type, List<A> args, InsertOptions options) {
    return transaction(c -> insertMany(c, type, args, options));
  }

  /** Inserts jobs of one kind atomically in the caller's transaction. */
  public <A> List<Job.InsertResult<A>> insertMany(
      Connection connection, JobType<A> type, List<A> args) {
    return insertMany(connection, type, args, InsertOptions.defaults());
  }

  /** Inserts jobs of one kind with common options in the caller's transaction. */
  public <A> List<Job.InsertResult<A>> insertMany(
      Connection connection, JobType<A> type, List<A> args, InsertOptions options) {
    return insertBatch(connection, args, (c, arg) -> insertWithHooks(c, type, arg, options));
  }

  /** Inserts mixed job kinds and per-job options atomically, in input order. */
  public List<Job.InsertResult<JsonNode>> insertMany(List<? extends Submission<?>> submissions) {
    return transaction(c -> insertMany(c, submissions));
  }

  /** Inserts mixed job kinds and per-job options inside the caller's transaction. */
  public List<Job.InsertResult<JsonNode>> insertMany(
      Connection connection, List<? extends Submission<?>> submissions) {
    return insertBatch(connection, submissions, (c, submission) -> insertSubmission(c, submission));
  }

  private <A> Job.InsertResult<A> insertWithHooks(
      Connection connection, JobType<A> type, A args, InsertOptions options) {
    try {
      for (var extension : extensions) extension.beforeInsert(connection, type, args, options);
    } catch (Exception error) {
      throw propagate(error);
    }
    return insertOne(connection, type, args, options);
  }

  private <A> Job.InsertResult<JsonNode> insertSubmission(
      Connection connection, Submission<A> value) {
    var result = insertWithHooks(connection, value.type(), value.args(), value.options());
    return new Job.InsertResult<>(
        result.job().mapArgs(Json::tree), result.uniqueSkippedAsDuplicate());
  }

  private <S, A> List<Job.InsertResult<A>> insertBatch(
      Connection connection,
      List<S> values,
      java.util.function.BiFunction<Connection, S, Job.InsertResult<A>> insert) {
    if (values.isEmpty()) throw new IllegalArgumentException("Cannot insert an empty batch");
    return atomic(
        connection,
        c ->
            insertMiddleware(
                c,
                0,
                () -> {
                  var results = new ArrayList<Job.InsertResult<A>>();
                  var keys = new java.util.HashSet<String>();
                  for (var value : values) {
                    var result = insert.apply(c, value);
                    if (result.job().uniqueKey() != null && !keys.add(result.job().uniqueKey()))
                      throw new RiverException(
                          RiverException.Code.REJECTED, "Batch repeats a unique key");
                    results.add(result);
                  }
                  return List.copyOf(results);
                }));
  }

  private <A> Job.InsertResult<A> insertOne(
      Connection connection, JobType<A> type, A args, InsertOptions options) {
    return insertOne(connection, type, args, options, null);
  }

  private <A> Job.InsertResult<A> insertOne(
      Connection connection,
      JobType<A> type,
      A args,
      InsertOptions options,
      Job<JsonNode> original) {
    var resolved = options.resolve(type.defaults());
    validate(resolved);
    String encodedArgs = Json.encode(args);
    try {
      var prepared =
          plugin.prepare(connection, driver(), new Plugin.Insert(type, encodedArgs, resolved));
      encodedArgs = prepared.args();
      resolved = prepared.options();
      validate(resolved);
    } catch (Exception error) {
      throw propagate(error);
    }
    Instant now = clock.instant();
    Instant scheduled = resolved.scheduledAt() == null ? now : resolved.scheduledAt();
    String state =
        original != null
            ? "available"
            : resolved.pending()
                ? "pending"
                : resolved.scheduledAt() != null ? "scheduled" : "available";
    String key =
        original != null
            ? original.uniqueKey()
            : resolved
                .unique()
                .key(
                    type.kind(),
                    encodedArgs,
                    type.uniqueFields(),
                    now,
                    resolved.queue(),
                    resolved.scheduledAt());
    int stateMask =
        original == null || original.uniqueStates() == null
            ? resolved.unique().stateMask()
            : original.uniqueStates().stream().mapToInt(Job.State::bit).reduce(0, (a, b) -> a | b);
    boolean postgres = database.dialect() == Database.Dialect.POSTGRES;
    try {
      if (!postgres && key != null && (stateMask & Job.State.of(state).bit()) != 0) {
        try (var statement = Sql.prepare(connection, Sql.query(database, "writer_lock"))) {
          statement.executeUpdate();
        }
        try (var statement =
                Sql.prepare(
                    connection, Sql.query(database, "unique_get"), HexFormat.of().parseHex(key));
            var rows = statement.executeQuery()) {
          if (rows.next()) return new Job.InsertResult<>(typed(read(rows), type), true);
        }
      }
      Object tags =
          postgres
              ? connection.createArrayOf("text", resolved.tags().toArray())
              : Json.encode(resolved.tags());
      Object mask =
          key == null
              ? null
              : postgres
                  ? String.format("%8s", Integer.toBinaryString(stateMask)).replace(' ', '0')
                  : stateMask;
      var metadata = resolved.metadata().deepCopy();
      boolean nonce = !postgres || !database.supportsNotifications(connection);
      if (nonce)
        ((tools.jackson.databind.node.ObjectNode) metadata)
            .put(
                "river:unique_nonce",
                java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 16));
      Object[] params = {
        encodedArgs,
        database.timestamp(original == null ? now : original.createdAt()),
        type.kind(),
        resolved.maxAttempts(),
        Json.encode(metadata),
        resolved.priority(),
        resolved.queue(),
        database.timestamp(scheduled),
        state,
        tags,
        key == null ? null : HexFormat.of().parseHex(key),
        mask
      };
      try (var statement = Sql.prepare(connection, Sql.query(database, "insert"), params);
          var rows = statement.executeQuery()) {
        if (!rows.next())
          throw new RiverException(RiverException.Code.REJECTED, "Insertion returned no row");
        var job = read(rows);
        boolean duplicate =
            nonce
                ? !job.metadata()
                    .path("river:unique_nonce")
                    .equals(metadata.path("river:unique_nonce"))
                : rows.getBoolean("duplicate");
        if (!duplicate) plugin.afterInsert(connection, driver(), job);
        if (!duplicate && job.state() == Job.State.AVAILABLE) notifyInsert(connection, job.queue());
        return new Job.InsertResult<>(typed(job, type), duplicate);
      } finally {
        if (tags instanceof java.sql.Array array) array.free();
      }
    } catch (Exception e) {
      throw propagate(e);
    }
  }

  public JobQuery.Page list(JobQuery query) {
    return transaction(c -> list(c, query));
  }

  public JobQuery.Page list(Connection connection, JobQuery query) {
    return atomic(
        connection,
        transaction -> {
          var params = new ArrayList<Object>();
          String sql = query.sql(database, params);
          try (var statement = Sql.prepare(transaction, sql, params.toArray());
              var rows = statement.executeQuery()) {
            var jobs = new ArrayList<Job<JsonNode>>();
            while (rows.next()) jobs.add(read(rows));
            return new JobQuery.Page(
                jobs.isEmpty() ? null : query.cursor(jobs.getLast()), List.copyOf(jobs));
          } catch (SQLException e) {
            throw databaseError("List jobs", e);
          }
        });
  }

  private Job<JsonNode> locked(Connection connection, long id) throws SQLException {
    if (database.dialect() == Database.Dialect.SQLITE)
      try (var statement = Sql.prepare(connection, Sql.query(database, "writer_lock"))) {
        statement.executeUpdate();
      }
    try (var statement = Sql.prepare(connection, Sql.query(database, "lock_get"), id);
        var rows = statement.executeQuery()) {
      if (!rows.next())
        throw new RiverException(RiverException.Code.NOT_FOUND, "Job " + id + " not found");
      return read(rows);
    }
  }

  /** Requests that the current leader resign after this transaction commits. */
  public void requestResign(Connection connection) {
    atomic(
        connection,
        c -> {
          notify(
              c,
              "river_leadership",
              Json.encode(java.util.Map.of("action", "request_resign", "leader_id", "")));
          return null;
        });
  }

  /** Requests a new leadership election. */
  public void requestResign() {
    transaction(
        c -> {
          requestResign(c);
          return null;
        });
  }

  void notify(Connection connection, String topic, String payload) throws SQLException {
    var commit = ownedCommit.get();
    if (commit != null && commit.connection == connection && topic.equals("river_insert"))
      commit.inserted = true;
    if (!database.supportsNotifications(connection)) return;
    try (var statement = Sql.prepare(connection, Sql.query(database, "notify"), topic, payload)) {
      statement.execute();
    }
  }

  void notifyInsert(Connection connection, String queue) throws SQLException {
    notify(connection, "river_insert", Json.encode(java.util.Map.of("queue", queue)));
  }

  public Job<JsonNode> output(long id, Object output) {
    return transaction(connection -> output(connection, id, output));
  }

  public Job<JsonNode> output(Connection connection, long id, Object output) {
    return atomic(
        connection,
        c -> {
          try (var statement =
                  Sql.prepare(c, Sql.query(database, "output"), Json.encode(output), id);
              var rows = statement.executeQuery()) {
            if (!rows.next())
              throw new RiverException(RiverException.Code.NOT_FOUND, "Job " + id + " not found");
            return read(rows);
          }
        });
  }

  public Job<JsonNode> retry(long id) {
    return transaction(c -> retry(c, id));
  }

  public Job<JsonNode> retry(Connection connection, long id) {
    return atomic(
        connection,
        c -> {
          var before = locked(c, id);
          try (var statement =
                  Sql.prepare(
                      c,
                      Sql.query(database, "retry"),
                      database.timestamp(clock.instant()),
                      id,
                      database.timestamp(clock.instant()));
              var rows = statement.executeQuery()) {
            var job = rows.next() ? read(rows) : before;
            plugin.afterStateChange(c, driver(), job);
            return job;
          }
        });
  }

  private void reset() {
    transaction(
        c -> {
          for (var name : List.of("jobs", "queues", "leader", "notifications"))
            try (var statement = Sql.prepare(c, Sql.query(database, "reset_" + name))) {
              statement.executeUpdate();
            }
          return null;
        });
  }

  /** Runs a callback in a new transaction, committing on success and rolling back on failure. */
  public <T> T transaction(Transaction<T> action) {
    try (var connection = database.connection()) {
      connection.setAutoCommit(false);
      var previous = ownedCommit.get();
      var commit = new Commit(connection);
      ownedCommit.set(commit);
      try {
        T result = action.run(connection);
        connection.commit();
        // JDBC batches incoming notifications. Wake this client's workers immediately after a
        // successful commit; cross-process and caller-owned transactions still use the database.
        if (commit.inserted) insertListeners.forEach(Runnable::run);
        return result;
      } catch (Throwable e) {
        try {
          connection.rollback();
        } catch (SQLException rollback) {
          e.addSuppressed(rollback);
        }
        throw propagate(e);
      } finally {
        if (previous == null) ownedCommit.remove();
        else ownedCommit.set(previous);
      }
    } catch (SQLException e) {
      throw databaseError("River transaction", e);
    }
  }

  /**
   * Runs an atomic operation inside an existing transaction using a savepoint. The connection must
   * have auto-commit disabled. Failure rolls back only this operation; the caller remains
   * responsible for committing, rolling back, and closing the connection.
   */
  public <T> T transaction(Connection connection, Transaction<T> action) {
    return atomic(connection, action);
  }

  private <T> T atomic(Connection connection, Transaction<T> action) {
    try {
      if (connection.getAutoCommit())
        throw new IllegalArgumentException("Caller connection must have autoCommit disabled");
      var savepoint = connection.setSavepoint();
      try {
        T result = action.run(connection);
        connection.releaseSavepoint(savepoint);
        return result;
      } catch (Throwable e) {
        try {
          connection.rollback(savepoint);
          connection.releaseSavepoint(savepoint);
        } catch (SQLException rollback) {
          e.addSuppressed(rollback);
        }
        throw propagate(e);
      }
    } catch (SQLException e) {
      throw databaseError("River operation savepoint", e);
    }
  }

  private static RuntimeException propagate(Throwable e) {
    if (e instanceof Error fatal) throw fatal;
    return e instanceof RuntimeException runtime
        ? runtime
        : e instanceof SQLException sql
            ? databaseError("Database operation", sql)
            : new RiverException(RiverException.Code.REJECTED, "Transaction callback failed", e);
  }

  static RiverException databaseError(String action, SQLException error) {
    return new RiverException(
        RiverException.Code.DATABASE, action + ": " + error.getMessage(), error);
  }

  <A> Job<A> typed(Job<JsonNode> job, JobType<A> type) {
    return job.mapArgs(_ -> Json.decode(plugin.decode(job), type.argsType()));
  }

  Job<JsonNode> read(ResultSet rows) throws SQLException {
    var decoded = readPartial(rows);
    if (decoded.failure() != null) throw decoded.failure();
    return decoded.job();
  }

  public record Decoded(Job<JsonNode> job, RuntimeException failure) {}

  Decoded readPartial(ResultSet rows) throws SQLException {
    var failures = new ArrayList<String>();
    var columns = new java.util.HashMap<String, JsonNode>();
    for (String column : List.of("args", "attempted_by", "errors", "metadata", "tags")) {
      String raw;
      if (rows.getObject(column) instanceof java.sql.Array array) {
        try {
          var values = (Object[]) array.getArray();
          var json = Json.MAPPER.createArrayNode();
          for (var value : values)
            json.add(column.equals("errors") ? Json.parse(value.toString()) : Json.tree(value));
          raw = Json.encode(json);
        } finally {
          array.free();
        }
      } else raw = rows.getString(column);
      try {
        columns.put(
            column,
            Json.parse(
                raw == null
                    ? (column.equals("metadata") || column.equals("args") ? "{}" : "[]")
                    : raw));
      } catch (RuntimeException error) {
        failures.add(column + ": " + error.getMessage());
        columns.put(column, Json.object());
      }
    }
    var errors = new ArrayList<Job.AttemptError>();
    for (var error : columns.get("errors")) {
      Instant at = Instant.parse("0001-01-01T00:00:00Z");
      if (error.path("at").isString())
        try {
          at = Instant.parse(error.path("at").asString());
        } catch (java.time.DateTimeException ignored) {
        }
      String message = error.isObject() ? errorText(error.path("error")) : errorText(error);
      errors.add(
          new Job.AttemptError(
              at, error.path("attempt").asInt(0), message, errorText(error.path("trace"))));
    }
    String bits = rows.getString("unique_states");
    int mask =
        bits == null
            ? 0
            : Integer.parseInt(bits, database.dialect() == Database.Dialect.POSTGRES ? 2 : 10);
    byte[] key = rows.getBytes("unique_key");
    var job =
        new Job<>(
            rows.getLong("id"),
            columns.get("args"),
            rows.getInt("attempt"),
            Database.instant(rows.getString("attempted_at")),
            strings(columns.get("attempted_by")),
            Database.instant(rows.getString("created_at")),
            errors,
            Database.instant(rows.getString("finalized_at")),
            rows.getString("kind"),
            rows.getInt("max_attempts"),
            columns.get("metadata"),
            rows.getInt("priority"),
            rows.getString("queue"),
            Database.instant(rows.getString("scheduled_at")),
            Job.State.of(rows.getString("state")),
            strings(columns.get("tags")),
            key == null ? null : HexFormat.of().formatHex(key),
            bits == null
                ? null
                : Arrays.stream(Job.State.values())
                    .filter(state -> (mask & state.bit()) != 0)
                    .toList());
    return new Decoded(
        job,
        failures.isEmpty()
            ? null
            : new IllegalArgumentException(
                "job row couldn't be decoded: " + String.join("; ", failures)));
  }

  private static String errorText(JsonNode value) {
    return value.isMissingNode() || value.isNull()
        ? ""
        : value.isString() ? value.asString() : Json.encode(value);
  }

  private static List<String> strings(JsonNode values) {
    var result = new ArrayList<String>();
    for (var value : values) result.add(value.asString());
    return result;
  }

  static void validate(InsertOptions options) {
    if (options.maxAttempts() < 1 || options.maxAttempts() > 32767)
      throw new IllegalArgumentException("maxAttempts must be between 1 and 32767");
    if (options.priority() < 1 || options.priority() > 4)
      throw new IllegalArgumentException("priority must be between 1 and 4");
    if (options.queue().length() > 64 || !options.queue().matches("[a-z0-9]+([_|-]?[a-z0-9]+)*"))
      throw new IllegalArgumentException("Invalid queue name");
    if (!options.metadata().isObject())
      throw new IllegalArgumentException("metadata must be a JSON object");
    for (var tag : options.tags())
      if (tag.length() > 255 || !tag.matches("[a-zA-Z0-9_][a-zA-Z0-9_-]+[a-zA-Z0-9_]"))
        throw new IllegalArgumentException("Invalid tag: " + tag);
  }

  /** A job and its individual options in an atomic batch. */
  public record Submission<A>(JobType<A> type, A args, InsertOptions options) {
    public Submission {
      Objects.requireNonNull(type, "type");
      Objects.requireNonNull(args, "args");
      Objects.requireNonNull(options, "options");
    }
  }

  /** A callback run in the same JDBC transaction as the application's writes. */
  @FunctionalInterface
  public interface Transaction<T> {
    T run(Connection connection) throws Exception;
  }
}
