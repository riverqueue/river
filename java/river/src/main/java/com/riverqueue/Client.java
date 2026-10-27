package com.riverqueue;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Stream;
import tools.jackson.databind.JsonNode;

/**
 * Transactional job storage. Connection overloads use the caller's transaction and never commit or
 * close it; other overloads own a transaction. Configure workers with {@link #builder(Database)},
 * then call {@link #start()}. Close the returned {@link RunHandle} to stop execution. The
 * application owns its connection pool; inserting jobs does not require a running client.
 */
public final class Client {
  /** Default queue name, used when neither insert options nor job-type defaults specify a queue. */
  public static final String QUEUE_DEFAULT = "default";

  private final Builder config;
  private final String id;
  private final List<Consumer<Event>> subscribers = new CopyOnWriteArrayList<>();
  private final java.util.concurrent.CopyOnWriteArrayList<Runnable> insertListeners =
      new java.util.concurrent.CopyOnWriteArrayList<>();
  private final ThreadLocal<Commit> ownedCommit = new ThreadLocal<>();
  private final Pilot pilot;
  private final Clock clock;
  private final Database database;
  private final List<Hook> allHooks;
  private final List<Hook> hooks;
  private final List<JobInsertMiddleware> insertMiddleware;
  private final List<Middleware> middleware;
  private final List<Plugin> plugins;
  private final List<WorkerMiddleware> workerMiddleware;

  private final ReentrantLock lifecycleLock = new ReentrantLock();
  private RunHandle activeRun;

  /** Creates an insert-only client using the system clock. */
  public Client(Database database) {
    this(database, Clock.systemUTC());
  }

  /** Creates an insert-only client using the supplied clock for storage operations. */
  public Client(Database database, Clock clock) {
    this(
        database,
        clock,
        List.of(),
        List.of(),
        List.of(),
        new Pilot() {},
        new Builder(database, clock));
  }

  private Client(
      Database database,
      Clock clock,
      List<Hook> hooks,
      List<Middleware> middleware,
      List<Plugin> plugins,
      Pilot pilot,
      Builder config) {
    this.config = new Builder(config);
    this.id = config.id == null ? "java-" + UUID.randomUUID() : config.id;
    this.pilot = pilot;
    this.database = Objects.requireNonNull(database);
    this.clock = Objects.requireNonNull(clock);
    this.hooks = List.copyOf(hooks);
    this.middleware = List.copyOf(middleware);
    this.plugins = List.copyOf(plugins);
    this.config.hooks = this.hooks;
    this.config.middleware = this.middleware;
    this.config.plugins = this.plugins;
    this.config.pilot = pilot;
    this.allHooks =
        Stream.concat(this.plugins.stream(), this.hooks.stream())
            .filter(Hook.class::isInstance)
            .map(Hook.class::cast)
            .toList();
    var allMiddleware = Stream.concat(this.plugins.stream(), this.middleware.stream()).toList();
    this.insertMiddleware =
        allMiddleware.stream()
            .filter(JobInsertMiddleware.class::isInstance)
            .map(JobInsertMiddleware.class::cast)
            .toList();
    this.workerMiddleware =
        allMiddleware.stream()
            .filter(WorkerMiddleware.class::isInstance)
            .map(WorkerMiddleware.class::cast)
            .toList();
  }

  /** Creates a builder for a client using an application-owned database. */
  public static Builder builder(Database database) {
    return new Builder(database, Clock.systemUTC());
  }

  public Database database() {
    return database;
  }

  /** Returns this client's ID, shared by its successive runs. */
  public String id() {
    return id;
  }

  public Queues queues() {
    return new Queues(this);
  }

  /**
   * Starts this client's configured runtime. Only one run may be active, including while a timed
   * out stop is still draining. Once a stop call succeeds, the client can be started again. Each
   * run starts with the configured queues and periodic jobs. At least one queue and handler are
   * required; insertion does not require starting the client.
   */
  public RunHandle start() {
    lifecycleLock.lock();
    try {
      if (activeRun != null) throw new IllegalStateException("Client already running or stopping");
      var run = new RunHandle(this, config);
      activeRun = run;
      try {
        pilot.starting(
            config.leadership,
            config.cancelledRetention,
            config.completedRetention,
            config.discardedRetention);
        run.launch();
        return run;
      } catch (RuntimeException | Error error) {
        try {
          run.abortStart();
        } catch (RuntimeException | Error cleanupError) {
          error.addSuppressed(cleanupError);
        }
        throw error;
      }
    } finally {
      lifecycleLock.unlock();
    }
  }

  void stopped(RunHandle run) {
    lifecycleLock.lock();
    try {
      if (activeRun == run) activeRun = null;
    } finally {
      lifecycleLock.unlock();
    }
  }

  /**
   * Subscribes before or during execution. Subscriptions belong to this client and survive restarts
   * until closed. Callbacks run synchronously and should return promptly.
   */
  public Subscription subscribe(Consumer<Event> subscriber, EventKind... kinds) {
    Objects.requireNonNull(subscriber, "subscriber");
    Set<EventKind> selected = Set.copyOf(List.of(kinds));
    Consumer<Event> filtered =
        event -> {
          if (selected.isEmpty() || selected.contains(event.kind())) subscriber.accept(event);
        };
    subscribers.add(filtered);
    return () -> subscribers.remove(filtered);
  }

  List<Consumer<Event>> subscribers() {
    return subscribers;
  }

  /** Copies configuration into a builder for an independent, unstarted client. */
  public Builder toBuilder() {
    return new Builder(config);
  }

  /**
   * Returns a new client with hooks appended in registration order, after plugins. Only hook
   * interfaces are activated; use {@link #withPlugins(Plugin...)} to also activate middleware.
   */
  public Client withHooks(Hook... hooks) {
    var all = new ArrayList<>(this.hooks);
    all.addAll(List.of(hooks));
    return new Client(database, clock, all, middleware, plugins, pilot, config);
  }

  /**
   * Returns a new client with middleware appended after plugins; the first middleware is outermost.
   * Only middleware interfaces are activated; use {@link #withPlugins(Plugin...)} to also activate
   * hooks.
   */
  public Client withMiddleware(Middleware... middleware) {
    var all = new ArrayList<>(this.middleware);
    all.addAll(List.of(middleware));
    return new Client(database, clock, hooks, all, plugins, pilot, config);
  }

  /** Internal integration point used by River Pro; not a stable application API. */
  public Client withPilot(Pilot pilot) {
    return new Client(
        database, clock, hooks, middleware, plugins, Objects.requireNonNull(pilot), config);
  }

  /**
   * Returns a new client with plugins appended in registration order. Each plugin activates all its
   * hook and middleware interfaces. Plugins run before separately configured hooks and middleware,
   * regardless of which registration method was called first. Repeated registrations are retained,
   * including when the same instance is registered through multiple methods.
   */
  public Client withPlugins(Plugin... plugins) {
    var all = new ArrayList<>(this.plugins);
    all.addAll(List.of(plugins));
    return new Client(database, clock, hooks, middleware, all, pilot, config);
  }

  Runnable onInsertCommit(Runnable listener) {
    insertListeners.add(listener);
    return () -> insertListeners.remove(listener);
  }

  /** Mutable client configuration. Building a client snapshots its settings and handlers. */
  public static final class Builder {
    boolean cancelOnError;
    Duration cancelledRetention = Duration.ofDays(1);
    Clock clock;
    Duration completedRetention = Duration.ofDays(1);
    final Database database;
    Duration discardedRetention = Duration.ofDays(7);
    Consumer<Throwable> errorHandler =
        error ->
            System.getLogger("com.riverqueue")
                .log(System.Logger.Level.WARNING, "River runtime failure", error);
    boolean fetchOnlyKnownKinds;
    final Map<String, Workers.Registration<?>> handlers = new java.util.LinkedHashMap<>();
    List<Hook> hooks = List.of();
    String id;
    Duration jobTimeout = Duration.ofMinutes(1);
    boolean jobTimeoutConfigured;
    boolean leadership = true;
    Duration maintenanceInterval = Duration.ofSeconds(5);
    List<Middleware> middleware = List.of();
    final List<Consumer<String>> observers = new ArrayList<>();
    final List<PeriodicJob<?>> periodic = new ArrayList<>();
    Pilot pilot = new Pilot() {};
    List<Plugin> plugins = List.of();
    Duration pollInterval = Duration.ofSeconds(1);
    boolean pollOnly;
    final Map<String, Integer> queues = new java.util.LinkedHashMap<>();
    Duration reindexInterval;
    List<String> reindexNames =
        List.of(
            "river_job_args_index",
            "river_job_kind",
            "river_job_metadata_index",
            "river_job_pkey",
            "river_job_prioritized_fetching_index",
            "river_job_state_and_finalized_at_index",
            "river_job_unique_idx");
    Duration rescueAfter;
    boolean rescueAfterConfigured;
    RetryPolicy retryPolicy = RetryPolicy.defaults();
    Duration serviceInterval = Duration.ofSeconds(1);
    Duration stopTimeout = Duration.ofSeconds(30);
    Duration stuckThreshold = Duration.ofSeconds(5);

    private Builder(Database database, Clock clock) {
      this.database = Objects.requireNonNull(database, "database");
      this.clock = Objects.requireNonNull(clock, "clock");
    }

    private Builder(Builder source) {
      this(source.database, source.clock);
      hooks = source.hooks;
      middleware = source.middleware;
      plugins = source.plugins;
      pilot = source.pilot;
      cancelledRetention = source.cancelledRetention;
      completedRetention = source.completedRetention;
      discardedRetention = source.discardedRetention;
      reindexInterval = source.reindexInterval;
      reindexNames = source.reindexNames;
      cancelOnError = source.cancelOnError;
      fetchOnlyKnownKinds = source.fetchOnlyKnownKinds;
      errorHandler = source.errorHandler;
      handlers.putAll(source.handlers);
      id = source.id;
      jobTimeout = source.jobTimeout;
      jobTimeoutConfigured = source.jobTimeoutConfigured;
      leadership = source.leadership;
      maintenanceInterval = source.maintenanceInterval;
      observers.addAll(source.observers);
      periodic.addAll(source.periodic);
      pollInterval = source.pollInterval;
      pollOnly = source.pollOnly;
      queues.putAll(source.queues);
      retryPolicy = source.retryPolicy;
      rescueAfter = source.rescueAfter;
      rescueAfterConfigured = source.rescueAfterConfigured;
      stopTimeout = source.stopTimeout;
      serviceInterval = source.serviceInterval;
      stuckThreshold = source.stuckThreshold;
    }

    /** Builds a client without starting background work. */
    public Client build() {
      if (!leadership && !periodic.isEmpty())
        throw new IllegalArgumentException("Periodic jobs require leader election");
      var config = new Builder(this);
      if (!config.rescueAfterConfigured)
        config.rescueAfter =
            positive(
                !jobTimeoutConfigured || jobTimeout.isNegative()
                    ? Duration.ofHours(1)
                    : jobTimeout.plus(Duration.ofHours(1)),
                "rescueAfter");
      if (config.rescueAfter.compareTo(jobTimeout) < 0)
        throw new IllegalArgumentException("rescueAfter must not be less than jobTimeout");
      return new Client(database, clock, hooks, middleware, plugins, pilot, config);
    }

    public Builder cancelOnError(boolean value) {
      cancelOnError = value;
      return this;
    }

    /** Uses the given clock for client operations. */
    public Builder clock(Clock value) {
      clock = Objects.requireNonNull(value, "clock");
      return this;
    }

    static Duration duration(Duration value, String name) {
      Objects.requireNonNull(value, name);
      try {
        value.toNanos();
      } catch (ArithmeticException error) {
        throw new IllegalArgumentException(name + " must fit in signed 64-bit nanoseconds", error);
      }
      return value;
    }

    /** Reports runtime failures. Failures in this callback are logged without stopping workers. */
    public Builder errorHandler(Consumer<Throwable> value) {
      errorHandler = Objects.requireNonNull(value, "errorHandler");
      return this;
    }

    public Builder fetchOnlyKnownKinds(boolean value) {
      fetchOnlyKnownKinds = value;
      return this;
    }

    /** Replaces operation hooks; only hook interfaces are activated, after plugins. */
    public Builder hooks(Hook... value) {
      hooks = List.of(value);
      return this;
    }

    /** Overrides the client ID (at most 100 UTF-8 bytes). Each client otherwise generates an ID. */
    public Builder id(String value) {
      if (value.isEmpty() || value.getBytes(StandardCharsets.UTF_8).length > 100)
        throw new IllegalArgumentException("Invalid client ID");
      id = value;
      return this;
    }

    /** Sets the attempt timeout; a negative duration disables it, including during stop. */
    public Builder jobTimeout(Duration value) {
      duration(value, "jobTimeout");
      if (value.isZero())
        throw new IllegalArgumentException(
            "Job timeout must be positive or negative to disable it");
      jobTimeout = value;
      jobTimeoutConfigured = true;
      return this;
    }

    public Builder leadership(boolean value) {
      leadership = value;
      return this;
    }

    public Builder maintenanceInterval(Duration value) {
      maintenanceInterval = positive(value, "maintenanceInterval");
      return this;
    }

    /** Replaces operation middleware; the first middleware is outermost, after plugins. */
    public Builder middleware(Middleware... value) {
      middleware = List.of(value);
      return this;
    }

    /** Observes runtime activity. Callback failures are reported without affecting execution. */
    public Builder observe(Consumer<String> value) {
      observers.add(Objects.requireNonNull(value, "observer"));
      return this;
    }

    /** Registers a periodic definition before starting this runtime. */
    public Builder periodic(PeriodicJob<?> job) {
      Objects.requireNonNull(job, "job");
      if (job.id() != null
          && periodic.stream().anyMatch(existing -> job.id().equals(existing.id())))
        throw new IllegalArgumentException("Duplicate periodic job ID: " + job.id());
      periodic.add(job);
      return this;
    }

    public <A> Builder periodic(
        String id,
        Schedule schedule,
        JobType<A> type,
        A args,
        InsertOptions options,
        boolean runOnStart) {
      return periodic(new PeriodicJob<>(id, schedule, type, args, options, runOnStart));
    }

    /** Replaces plugins, enabling their hook and middleware interfaces before other callbacks. */
    public Builder plugins(Plugin... value) {
      plugins = List.of(value);
      return this;
    }

    public Builder pollInterval(Duration value) {
      pollInterval = positive(value, "pollInterval");
      return this;
    }

    public Builder pollOnly(boolean value) {
      pollOnly = value;
      return this;
    }

    private static Duration positive(Duration value, String name) {
      duration(value, name);
      if (value.isNegative() || value.isZero())
        throw new IllegalArgumentException(name + " must be positive");
      return value;
    }

    public Builder queue(String name, int maxWorkers) {
      RunHandle.validateQueue(name, maxWorkers);
      queues.put(name, maxWorkers);
      return this;
    }

    public Builder reindex(List<String> names, Duration interval) {
      reindexNames = List.copyOf(names);
      reindexInterval = interval == null ? null : positive(interval, "reindex interval");
      return this;
    }

    /**
     * Sets when abandoned jobs may be rescued. Must not be shorter than the attempt timeout.
     * Defaults to one hour, plus an explicitly configured positive job timeout.
     */
    public Builder rescueAfter(Duration value) {
      rescueAfter = positive(value, "rescueAfter");
      rescueAfterConfigured = true;
      return this;
    }

    public Builder retention(Duration cancelled, Duration completed, Duration discarded) {
      cancelledRetention = duration(cancelled, "cancelled retention");
      completedRetention = duration(completed, "completed retention");
      discardedRetention = duration(discarded, "discarded retention");
      return this;
    }

    public Builder retryPolicy(RetryPolicy value) {
      retryPolicy = Objects.requireNonNull(value, "retryPolicy");
      return this;
    }

    /**
     * Sets the election and heartbeat interval; leadership leases include ten seconds of margin.
     * Must be positive and shorter than one day so active queues do not expire between heartbeats.
     */
    public Builder serviceInterval(Duration value) {
      positive(value, "serviceInterval");
      if (value.compareTo(RunHandle.QUEUE_RETENTION) >= 0)
        throw new IllegalArgumentException(
            "serviceInterval must be shorter than the queue retention period of one day");
      serviceInterval = value;
      return this;
    }

    public Builder stopTimeout(Duration value) {
      stopTimeout = positive(value, "stopTimeout");
      return this;
    }

    public Builder stuckThreshold(Duration value) {
      stuckThreshold = positive(value, "stuckThreshold");
      return this;
    }

    /** Adds a worker handler for a job type. Each job kind may be added only once. */
    public <A> Builder worker(JobType<A> type, Workers.Handler<A> handler) {
      Objects.requireNonNull(type, "type");
      Objects.requireNonNull(handler, "handler");
      if (handlers.putIfAbsent(type.kind(), new Workers.Registration<>(type, handler)) != null)
        throw new IllegalArgumentException("Duplicate worker kind");
      return this;
    }

    /** Replaces the handler registry with a snapshot of the supplied workers. */
    public Builder workers(Workers value) {
      var registrations = Objects.requireNonNull(value, "workers").snapshot();
      handlers.clear();
      handlers.putAll(registrations);
      return this;
    }
  }

  private static final class Commit {
    final Connection connection;
    boolean inserted;

    Commit(Connection connection) {
      this.connection = connection;
    }
  }

  Pilot pilot() {
    return pilot;
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
    public Job.InsertResult reinsert(Connection connection, Job<JsonNode> original) {
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
                      List.of(type.submission(original.args(), options)),
                      0,
                      () -> {
                        insertBegin(c, type, original.args(), options);
                        return List.of(
                            insertOne(c, type, original.args(), options, original, null));
                      })
                  .getFirst());
    }
  }

  List<Hook> hooks() {
    return allHooks;
  }

  List<WorkerMiddleware> workerMiddleware() {
    return workerMiddleware;
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
            pilot.afterStateChange(c, driver(), job);
            notify(c, Protocol.cancel(id, job.queue()));
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
            pilot.afterDelete(c, driver(), deleted);
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
                      null,
                      "{}",
                      Json.encode(updates),
                      "completed",
                      database.timestamp(clock.instant()),
                      database.timestamp(job.scheduledAt()),
                      job.attempt(),
                      null,
                      null,
                      job.id(),
                      job.attempt(),
                      database.timestamp(job.attemptedAt()));
              var rows = statement.executeQuery()) {
            if (!rows.next()) return get(c, job.id()).mapArgs(ignored -> job.args());
            var completed = read(rows);
            pilot.afterStateChange(c, driver(), completed);
            return completed.mapArgs(ignored -> job.args());
          }
        });
  }

  /**
   * Deletes a page of matching non-running jobs in query order. Running jobs do not count toward
   * the limit. Concurrent changes that stop selected jobs matching the filters or cursor can reduce
   * the returned batch size. Unfiltered deletion requires {@code all=true}.
   */
  public List<Job<JsonNode>> deleteMany(JobQuery query, boolean all) {
    return transaction(connection -> deleteMany(connection, query, all));
  }

  /** Deletes matching non-running jobs atomically within the caller's transaction. */
  public List<Job<JsonNode>> deleteMany(Connection connection, JobQuery query, boolean all) {
    if (!all && !query.hasFilters())
      throw new IllegalArgumentException("Bulk deletion requires filters or explicit all=true");
    return atomic(
        connection,
        c -> {
          // Reserve SQLite's writer before listing so another commit cannot invalidate the
          // snapshot.
          if (database.dialect() == Database.Dialect.SQLITE)
            try (var statement = Sql.prepare(c, Sql.query(database, "writer_lock"))) {
              statement.executeUpdate();
            }
          var result = new ArrayList<Job<JsonNode>>();
          var jobs = list(c, query, true).jobs();
          var params = new ArrayList<Object>();
          String sql = query.deleteSql(database, params);
          try (var statement = Sql.prepare(c, sql, params.toArray())) {
            for (var job : jobs) {
              statement.setLong(params.size() + 1, job.id());
              try (var rows = statement.executeQuery()) {
                // Recheck every filter atomically: a peer may have updated this job since listing.
                if (!rows.next()) continue;
                var deleted = read(rows);
                pilot.afterDelete(c, driver(), deleted);
                result.add(deleted);
              }
            }
          }
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

  public <A> Job.InsertResult insert(JobType<A> type, A args) {
    return insert(type, args, InsertOptions.defaults());
  }

  public <A> Job.InsertResult insert(JobType<A> type, A args, InsertOptions options) {
    return transaction(c -> insert(c, type, args, options));
  }

  public <A> Job.InsertResult insert(Connection connection, JobType<A> type, A args) {
    return insert(connection, type, args, InsertOptions.defaults());
  }

  public <A> Job.InsertResult insert(
      Connection connection, JobType<A> type, A args, InsertOptions options) {
    return insert(connection, type, args, options, null);
  }

  <A> Job.InsertResult insert(
      Connection connection,
      JobType<A> type,
      A args,
      InsertOptions options,
      Instant scheduledDefault) {
    return atomic(
        connection,
        c -> {
          return insertMiddleware(
                  c,
                  List.of(type.submission(args, options)),
                  0,
                  () -> {
                    insertBegin(c, type, args, options);
                    return List.of(insertOne(c, type, args, options, null, scheduledDefault));
                  })
              .getFirst();
        });
  }

  private void insertBegin(
      Connection connection, JobType<?> type, Object args, InsertOptions options) throws Exception {
    for (var hook : allHooks)
      if (hook instanceof HookInsertBegin insertHook)
        insertHook.insertBegin(connection, type, args, options);
  }

  private List<Job.InsertResult> insertMiddleware(
      Connection connection,
      List<Submission<?>> submissions,
      int index,
      java.util.concurrent.Callable<List<Job.InsertResult>> next)
      throws Exception {
    return index == insertMiddleware.size()
        ? next.call()
        : insertMiddleware
            .get(index)
            .insertMany(
                connection,
                submissions,
                () -> insertMiddleware(connection, submissions, index + 1, next));
  }

  /** Inserts jobs of one kind atomically, inheriting the job type's defaults. */
  public <A> List<Job.InsertResult> insertMany(JobType<A> type, List<A> args) {
    return insertMany(type, args, InsertOptions.defaults());
  }

  /** Inserts jobs of one kind with common options in a new transaction. */
  public <A> List<Job.InsertResult> insertMany(
      JobType<A> type, List<A> args, InsertOptions options) {
    return transaction(c -> insertMany(c, type, args, options));
  }

  /** Inserts jobs of one kind atomically in the caller's transaction. */
  public <A> List<Job.InsertResult> insertMany(
      Connection connection, JobType<A> type, List<A> args) {
    return insertMany(connection, type, args, InsertOptions.defaults());
  }

  /** Inserts jobs of one kind with common options in the caller's transaction. */
  public <A> List<Job.InsertResult> insertMany(
      Connection connection, JobType<A> type, List<A> args, InsertOptions options) {
    return insertMany(connection, args.stream().map(arg -> type.submission(arg, options)).toList());
  }

  /** Inserts mixed job kinds and per-job options atomically, in input order. */
  public List<Job.InsertResult> insertMany(List<? extends Submission<?>> submissions) {
    return transaction(c -> insertMany(c, submissions));
  }

  /** Inserts mixed job kinds and per-job options inside the caller's transaction. */
  public List<Job.InsertResult> insertMany(
      Connection connection, List<? extends Submission<?>> submissions) {
    return insertBatch(connection, List.copyOf(submissions));
  }

  private <A> Job.InsertResult insertWithHooks(
      Connection connection, JobType<A> type, A args, InsertOptions options) {
    try {
      insertBegin(connection, type, args, options);
    } catch (Exception error) {
      throw propagate(error);
    }
    return insertOne(connection, type, args, options);
  }

  private <A> Job.InsertResult insertSubmission(Connection connection, Submission<A> value) {
    return insertWithHooks(connection, value.type(), value.args(), value.options());
  }

  private List<Job.InsertResult> insertBatch(
      Connection connection, List<Submission<?>> submissions) {
    if (submissions.isEmpty()) throw new IllegalArgumentException("Cannot insert an empty batch");
    return atomic(
        connection,
        c ->
            insertMiddleware(
                c,
                submissions,
                0,
                () -> {
                  var results = new ArrayList<Job.InsertResult>();
                  var keys = new java.util.HashSet<String>();
                  for (var submission : submissions) {
                    var result = insertSubmission(c, submission);
                    if (result.job().uniqueKey() != null && !keys.add(result.job().uniqueKey()))
                      throw new RiverException(
                          RiverException.Code.REJECTED, "Batch repeats a unique key");
                    results.add(result);
                  }
                  return List.copyOf(results);
                }));
  }

  private <A> Job.InsertResult insertOne(
      Connection connection, JobType<A> type, A args, InsertOptions options) {
    return insertOne(connection, type, args, options, null, null);
  }

  private <A> Job.InsertResult insertOne(
      Connection connection,
      JobType<A> type,
      A args,
      InsertOptions options,
      Job<JsonNode> original,
      Instant scheduledDefault) {
    var resolved = options.resolve(type.defaults());
    validate(resolved);
    String encodedArgs = Json.encode(args);
    try {
      var prepared =
          pilot.prepare(connection, driver(), new Pilot.Insert(type, encodedArgs, resolved));
      encodedArgs = prepared.args();
      resolved = prepared.options();
      validate(resolved);
    } catch (Exception error) {
      throw propagate(error);
    }
    Instant now = clock.instant();
    Instant scheduled =
        resolved.scheduledAt() != null
            ? resolved.scheduledAt()
            : scheduledDefault != null ? scheduledDefault : now;
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
          if (rows.next()) return new Job.InsertResult(read(rows), true);
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
      boolean nonce = database.usesUniqueNonce(connection);
      if (nonce)
        ((tools.jackson.databind.node.ObjectNode) metadata)
            .put(
                Protocol.METADATA_UNIQUE_NONCE,
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
      String insertSql = Sql.query(database, "insert");
      if (postgres)
        insertSql =
            insertSql.replace(
                "{duplicate}",
                Sql.query(database, nonce ? "insert_duplicate_nonce" : "insert_duplicate_xmax"));
      try (var statement = Sql.prepare(connection, insertSql, params);
          var rows = statement.executeQuery()) {
        if (!rows.next())
          throw new RiverException(RiverException.Code.REJECTED, "Insertion returned no row");
        var job = read(rows);
        boolean duplicate =
            nonce
                ? !job.metadata()
                    .path(Protocol.METADATA_UNIQUE_NONCE)
                    .equals(metadata.path(Protocol.METADATA_UNIQUE_NONCE))
                : rows.getBoolean("duplicate");
        if (!duplicate) pilot.afterInsert(connection, driver(), job);
        if (!duplicate && job.state() == Job.State.AVAILABLE) notifyInsert(connection, job.queue());
        return new Job.InsertResult(job, duplicate);
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
    return list(connection, query, false);
  }

  private JobQuery.Page list(Connection connection, JobQuery query, boolean forDelete) {
    return atomic(
        connection,
        transaction -> {
          var params = new ArrayList<Object>();
          String sql = query.sql(database, params, forDelete);
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
          notify(c, Protocol.requestResign());
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
    if (commit != null && commit.connection == connection && topic.equals(Protocol.TOPIC_INSERT))
      commit.inserted = true;
    if (!database.supportsNotifications(connection)) return;
    try (var statement = Sql.prepare(connection, Sql.query(database, "notify"), topic, payload)) {
      statement.execute();
    }
  }

  void notify(Connection connection, Protocol.Notification notification) throws SQLException {
    notify(connection, notification.topic(), notification.payload());
  }

  void notifyInsert(Connection connection, String queue) throws SQLException {
    notify(connection, Protocol.insert(queue));
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
            boolean updated = rows.next();
            var job = updated ? read(rows) : before;
            pilot.afterStateChange(c, driver(), job);
            if (updated) notifyInsert(c, job.queue());
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
      var commit = ownedCommit.get();
      if (commit != null && commit.connection != connection) commit = null;
      boolean inserted = commit != null && commit.inserted;
      try {
        T result = action.run(connection);
        connection.releaseSavepoint(savepoint);
        return result;
      } catch (Throwable e) {
        // Roll back local wake-ups along with the database notifications in this savepoint.
        if (commit != null) commit.inserted = inserted;
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
    return job.mapArgs(ignored -> Json.decode(pilot.decode(job), type.argsType()));
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

  /** A job or queue event. The payload corresponding to the event kind is non-null. */
  public record Event(EventKind kind, Job<JsonNode> job, Queues.Queue queue) {
    public Event(EventKind kind, Job<JsonNode> job) {
      this(kind, job, null);
    }
  }

  /** Event names shared with other River clients and the conformance protocol. */
  public enum EventKind {
    JOB_CANCELLED,
    JOB_COMPLETED,
    JOB_FAILED,
    JOB_INTERRUPTED,
    JOB_SNOOZED,
    QUEUE_PAUSED,
    QUEUE_RESUMED;

    @com.fasterxml.jackson.annotation.JsonValue
    public String value() {
      return name().toLowerCase(java.util.Locale.ROOT);
    }
  }

  /** A job and its individual options in an atomic batch. */
  public record Submission<A>(JobType<A> type, A args, InsertOptions options) {
    public Submission {
      Objects.requireNonNull(type, "type");
      Objects.requireNonNull(args, "args");
      Objects.requireNonNull(options, "options");
    }
  }

  /** An idempotent subscription handle whose close method cannot throw a checked exception. */
  @FunctionalInterface
  public interface Subscription extends AutoCloseable {
    @Override
    void close();
  }

  /** A callback run in the same JDBC transaction as the application's writes. */
  @FunctionalInterface
  public interface Transaction<T> {
    T run(Connection connection) throws Exception;
  }
}
