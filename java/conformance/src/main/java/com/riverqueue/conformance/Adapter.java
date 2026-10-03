package com.riverqueue.conformance;

import com.riverqueue.Client;
import com.riverqueue.Database;
import com.riverqueue.InsertOptions;
import com.riverqueue.Job;
import com.riverqueue.JobQuery;
import com.riverqueue.JobType;
import com.riverqueue.Json;
import com.riverqueue.Migrator;
import com.riverqueue.RetryPolicy;
import com.riverqueue.RiverException;
import com.riverqueue.Schedule;
import com.riverqueue.Unique;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

/** Private process adapter for the shared River conformance contract. */
public final class Adapter implements AutoCloseable {
  private static final JobType<Echo> ECHO = JobType.of("conformance_echo", Echo.class);
  private static final List<String> INSERT_METHODS =
      List.of(
          "handshake",
          "insert",
          "insert_many",
          "tx_begin",
          "tx_commit",
          "tx_insert",
          "tx_insert_many",
          "tx_rollback",
          "unique_key");
  private final String applicationName =
      System.getenv().getOrDefault("RIVER_CONFORMANCE_APPLICATION_NAME", "river-conformance-java");
  private final JsonNode contract;
  private final Database database;
  private final com.zaxxer.hikari.HikariDataSource pool;
  private final List<String> methods;
  private final JsonNode capabilities;
  private final String profile;
  private RetryPolicy.Default retryPolicy = RetryPolicy.defaults();
  private final RuntimeAdapter runtime = new RuntimeAdapter();
  private Client river;
  private final Map<String, Connection> transactions = new HashMap<>();

  private Adapter() throws Exception {
    String url = System.getenv("RIVER_CONFORMANCE_DATABASE_URL");
    boolean sqlite =
        System.getenv()
            .getOrDefault("RIVER_CONFORMANCE_DATABASE_KIND", "postgres")
            .equals("sqlite");
    var source =
        Database.connect(
            sqlite && !url.startsWith("jdbc:") && !url.startsWith("sqlite:")
                ? "sqlite:" + url
                : url,
            applicationName);
    pool = Connections.pool(source);
    database = new Database(pool, source.dialect());
    river = new Client(database).withExtension(runtime.instrumentation());
    try (var input = Adapter.class.getResourceAsStream("/contract.json")) {
      contract = Json.MAPPER.readTree(input);
    }
    if (sqlite) {
      boolean full =
          System.getenv().getOrDefault("RIVER_CONFORMANCE_PROFILE", "").equals("sqlite-runtime-v1");
      try (var input =
          Adapter.class.getResourceAsStream(
              full ? "/sqlite-runtime-profile.json" : "/sqlite-profile.json")) {
        var declaration = Json.MAPPER.readTree(input);
        methods = strings(declaration.path("methods"));
        capabilities = declaration.path("capabilities");
        profile = full ? "sqlite-runtime-v1" : "portable-storage-v1";
      }
    } else if (System.getenv()
        .getOrDefault("RIVER_CONFORMANCE_PROFILE", "")
        .equals("insert-only-v1")) {
      methods = INSERT_METHODS;
      capabilities = Json.tree(List.of("insert", "lifecycle", "transactions", "unique_jobs"));
      profile = "insert-only-v1";
    } else {
      var all = new ArrayList<String>();
      for (var method : contract.path("methods")) all.add(method.path("name").asString());
      all.sort(String::compareTo);
      methods = List.copyOf(all);
      try (var input = Adapter.class.getResourceAsStream("/manifest.json")) {
        var supported = new ArrayList<String>();
        for (var entry : Json.MAPPER.readTree(input).path("capabilities").properties())
          if (entry.getValue().asString().equals("complete")) supported.add(entry.getKey());
        supported.sort(String::compareTo);
        capabilities = Json.tree(supported);
      }
      profile = "postgres-full-v1";
    }
  }

  public static void main(String[] args) throws Exception {
    try (var adapter = new Adapter();
        var input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
      for (String line; (line = input.readLine()) != null; ) {
        JsonNode id = Json.MAPPER.nullNode();
        ObjectNode response = Json.object().put("jsonrpc", "2.0");
        try {
          JsonNode request;
          try {
            request = Json.parse(line);
          } catch (Exception error) {
            throw new Failure(-32700, "Invalid JSON");
          }
          if (!request.isObject()
              || !request.path("jsonrpc").asString("").equals("2.0")
              || !request.path("method").isString()
              || !request.has("id")) throw new Failure(-32600, "Invalid JSON-RPC request");
          id = request.path("id");
          String method = request.path("method").asString();
          if (!adapter.methods.contains(method))
            throw new Failure(-32601, "Method not in profile: " + method);
          var params = request.has("params") ? request.path("params") : Json.object();
          for (var entry : adapter.contract.path("methods"))
            if (entry.path("name").asString().equals(method))
              adapter.validate(entry.path("params"), params);
          String rawParams = Json.members(line).getOrDefault("params", "{}");
          response.set("result", adapter.handle(method, params, rawParams));
        } catch (Exception error) {
          int code =
              switch (error) {
                case Failure failure -> failure.code;
                case RiverException riverError ->
                    switch (riverError.code()) {
                      case DATABASE -> -32003;
                      case NOT_FOUND -> -32001;
                      case REJECTED -> -32002;
                      case UNSUPPORTED -> -32004;
                    };
                case SQLException ignored -> -32003;
                case IllegalArgumentException ignored -> -32002;
                case java.time.DateTimeException ignored -> -32002;
                default -> -32000;
              };
          if (code == -32000) error.printStackTrace(System.err);
          response.set(
              "error",
              Json.object()
                  .put("code", code)
                  .put(
                      "message",
                      error.getMessage() == null ? error.toString() : error.getMessage()));
        }
        response.set("id", id);
        System.out.println(Json.encode(response));
        System.out.flush();
      }
    }
  }

  @Override
  public void close() throws SQLException {
    runtime.stop(true);
    for (var connection : transactions.values()) {
      try {
        connection.rollback();
      } finally {
        connection.close();
      }
    }
    pool.close();
  }

  private JsonNode handle(String method, JsonNode params, String rawParams) throws Exception {
    return switch (method) {
      case "benchmark_enqueue" -> {
        int count = params.path("jobs").asInt();
        if (count < 1) throw new IllegalArgumentException("jobs must be positive");
        long[] latencies = new long[count];
        long started = System.nanoTime();
        for (int i = 0; i < count; i++) {
          long before = System.nanoTime();
          river.insert(ECHO, new Echo("", 0, "benchmark-enqueue-" + i));
          latencies[i] = System.nanoTime() - before;
        }
        long elapsed = System.nanoTime() - started;
        java.util.Arrays.sort(latencies);
        yield Json.tree(
            Map.of(
                "duration_ns",
                elapsed,
                "p95_ns",
                latencies[Math.max(0, (count * 95 + 99) / 100 - 1)]));
      }
      case "handshake" ->
          Json.tree(
              Map.ofEntries(
                  Map.entry("adapter_version", 20),
                  Map.entry("application_name", applicationName),
                  Map.entry(
                      "backend",
                      database.dialect() == Database.Dialect.POSTGRES ? "postgres" : "sqlite"),
                  Map.entry("capabilities", capabilities),
                  Map.entry("implementation", "java"),
                  Map.entry("implementation_version", "0.48.0-alpha.1"),
                  Map.entry("methods", methods),
                  Map.entry("migration_lines", Map.of("main", 8)),
                  Map.entry("profile", profile),
                  Map.entry("protocol_revision", 1)));
      case "clock_set" -> {
        river =
            new Client(
                database,
                Clock.fixed(Instant.parse(params.path("now").asString()), ZoneOffset.UTC));
        yield Json.object();
      }
      case "rng_seed" -> {
        retryPolicy =
            RetryPolicy.defaults(
                new java.util.Random(
                    new java.math.BigInteger(params.path("seed").toString()).longValue()));
        yield Json.object();
      }
      case "retry_delay" ->
          Json.object()
              .put("delay_ns", retryPolicy.delay(params.path("error_count").asInt()).toNanos());
      case "cron_next" -> {
        var schedule = Schedule.cron(params.path("expression").asString());
        var next = OffsetDateTime.parse(params.path("from").asString());
        var times = new ArrayList<String>();
        for (int i = 0; i < params.path("count").asInt(); i++) {
          var found = schedule.next(next);
          if (found.isEmpty()) break;
          next = found.get();
          times.add(java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(next));
        }
        yield Json.tree(Map.of("next", times));
      }
      case "migrate" -> {
        boolean down = params.path("direction").asString("up").equals("down");
        Integer target =
            params.has("target_version") ? params.path("target_version").asInt() : null;
        if (target != null) {
          if (target == -1) target = 0;
          else if (down && target == 0) target = null;
        }
        Integer steps = params.has("max_steps") ? params.path("max_steps").asInt() : null;
        // The adapter protocol's zero downward target means exactly one version, independently
        // of its step limit. The public Java API uses an omitted target and named options.
        if (down && target == null) steps = 1;
        yield Json.tree(
            new Migrator(database.withSchema(params.path("schema").asString("")))
                .migrate(
                    down ? Migrator.Direction.DOWN : Migrator.Direction.UP,
                    new Migrator.Options(target, steps, params.path("dry_run").asBoolean(false))));
      }
      case "reset" -> {
        runtime.stop(true);
        river.driver().reset();
        river = new Client(database).withExtension(runtime.instrumentation());
        yield Json.object();
      }
      case "get" -> normalized(scoped(params).get(params.path("id").asLong()));
      case "tx_get" -> normalized(river.get(transaction(params), params.path("id").asLong()));
      case "list" -> page(river.list(query(params)));
      case "tx_list" -> page(river.list(transaction(params), query(params)));
      case "cancel" -> normalized(river.cancel(params.path("id").asLong()));
      case "tx_cancel" -> normalized(river.cancel(transaction(params), params.path("id").asLong()));
      case "delete" -> normalized(river.delete(params.path("id").asLong()));
      case "tx_delete" -> normalized(river.delete(transaction(params), params.path("id").asLong()));
      case "retry" -> normalized(river.retry(params.path("id").asLong()));
      case "tx_retry" -> normalized(river.retry(transaction(params), params.path("id").asLong()));
      case "update" ->
          normalized(
              river.transaction(
                  c ->
                      params.has("output")
                          ? river.output(c, params.path("id").asLong(), params.path("output"))
                          : river.get(c, params.path("id").asLong())));
      case "tx_update" ->
          normalized(
              params.has("output")
                  ? river.output(
                      transaction(params), params.path("id").asLong(), params.path("output"))
                  : river.get(transaction(params), params.path("id").asLong()));
      case "delete_many" ->
          Json.object()
              .set(
                  "jobs",
                  Json.tree(
                      river
                          .transaction(
                              c ->
                                  river.deleteMany(
                                      c, query(params), params.path("all").asBoolean(false)))
                          .stream()
                          .map(Adapter::normalized)
                          .toList()));
      case "delete_finalized" ->
          Json.object()
              .put(
                  "deleted",
                  (int)
                      river.transaction(
                          c ->
                              river
                                  .driver()
                                  .deleteFinalized(
                                      c,
                                      Instant.parse(params.path("before").asString()),
                                      params.path("limit").asInt(),
                                      strings(params.path("queues_excluded")),
                                      params.path("queues_included").isArray()
                                          ? strings(params.path("queues_included"))
                                          : null)));
      case "tx_delete_many" ->
          Json.object()
              .set(
                  "jobs",
                  Json.tree(
                      river
                          .deleteMany(
                              transaction(params),
                              query(params),
                              params.path("all").asBoolean(false))
                          .stream()
                          .map(Adapter::normalized)
                          .toList()));
      case "raw_insert_exact_json", "raw_job_exact_json", "raw_job_row", "raw_job_timestamps" ->
          RawStorage.handle(database, method, params);
      case "barrier_create",
          "barrier_release",
          "queue_add",
          "queue_remove",
          "runtime_stats",
          "start",
          "stop",
          "wait",
          "work" ->
          runtime.handle(scoped(params), method, params);
      case "queue_get" -> Json.tree(river.queues().get(params.path("name").asString()));
      case "queue_list" ->
          Json.tree(Map.of("queues", river.queues().list(params.path("limit").asInt(100))));
      case "queue_pause" -> {
        river.queues().pause(params.path("name").asString());
        yield Json.object();
      }
      case "queue_resume" -> {
        river.queues().resume(params.path("name").asString());
        yield Json.object();
      }
      case "queue_update" ->
          Json.tree(
              river
                  .queues()
                  .update(
                      params.path("name").asString(),
                      params.has("metadata") ? params.path("metadata") : null));
      case "tx_queue_get" ->
          Json.tree(river.queues().get(transaction(params), params.path("name").asString()));
      case "tx_queue_list" ->
          Json.tree(
              Map.of(
                  "queues",
                  river.queues().list(transaction(params), params.path("limit").asInt(100))));
      case "tx_queue_pause" -> {
        river.queues().pause(transaction(params), params.path("name").asString());
        yield Json.object();
      }
      case "tx_queue_resume" -> {
        river.queues().resume(transaction(params), params.path("name").asString());
        yield Json.object();
      }
      case "tx_queue_update" ->
          Json.tree(
              river
                  .queues()
                  .update(
                      transaction(params),
                      params.path("name").asString(),
                      params.has("metadata") ? params.path("metadata") : null));
      case "tx_fail" -> {
        try (var statement = transaction(params).createStatement()) {
          statement.execute("SELECT 1/0");
        }
        yield Json.object();
      }
      case "request_resign" -> {
        if (params.has("handle")) river.requestResign(transaction(params));
        else river.requestResign();
        yield Json.object();
      }
      case "leader",
          "listener_count",
          "connection_count",
          "fault_disconnect_application",
          "fault_disconnect_listeners",
          "fault_expire_leader",
          "raw_insert_no_notify",
          "raw_finalize",
          "raw_notifications",
          "raw_replace_json_text",
          "raw_insert_full_row" ->
          RawStorage.handle(database, method, params);
      case "insert" -> normalized(insert(null, params).job());
      case "insert_many" -> river.transaction(c -> insertMany(c, params.path("jobs")));
      case "tx_begin" -> {
        String handle = params.path("handle").asString();
        if (transactions.containsKey(handle))
          throw new Failure(-32002, "Transaction already exists");
        var connection = database.connection();
        connection.setAutoCommit(false);
        transactions.put(handle, connection);
        yield Json.object();
      }
      case "tx_insert" -> normalized(insert(transaction(params), params.path("job")).job());
      case "tx_insert_many" -> insertMany(transaction(params), params.path("jobs"));
      case "tx_commit", "tx_rollback" -> {
        var connection = transaction(params);
        transactions.remove(params.path("handle").asString());
        try (connection) {
          if (method.equals("tx_commit")) connection.commit();
          else connection.rollback();
        }
        yield Json.object();
      }
      case "unique_key" -> {
        var options = unique(params.path("options"), true);
        var paths = new ArrayList<List<String>>();
        for (var path : params.path("selected_unique_components")) {
          var components = new ArrayList<String>();
          for (var component : path) components.add(component.asString());
          paths.add(components);
        }
        String key =
            options.key(
                params.path("kind").asString(),
                Json.members(rawParams).get("args"),
                paths,
                Instant.parse(params.path("now").asString()),
                params.path("queue").asString(),
                params.path("scheduled_at").isString()
                    ? Instant.parse(params.path("scheduled_at").asString())
                    : null);
        if (key == null) throw new Failure(-32002, "Uniqueness is disabled");
        yield Json.object().put("sha256", key).put("state_mask", options.stateMask());
      }
      default -> throw new Failure(-32601, "Unknown method");
    };
  }

  private Job.InsertResult insert(Connection connection, JsonNode params) {
    var args =
        new Echo(
            params.path("behavior").asString(""),
            params.path("duration_ms").asLong(0),
            params.path("message").asString(""));
    var options = options(params.path("opts"));
    return connection == null
        ? scoped(params).insert(ECHO, args, options)
        : river.insert(connection, ECHO, args, options);
  }

  private JsonNode insertMany(Connection connection, JsonNode jobs) throws SQLException {
    if (jobs.isEmpty()) throw new IllegalArgumentException("Cannot insert an empty batch");
    var savepoint = connection.setSavepoint();
    try {
      var results = Json.MAPPER.createArrayNode();
      var keys = new HashSet<String>();
      for (var params : jobs) {
        var result = insert(connection, params);
        if (result.job().uniqueKey() != null && !keys.add(result.job().uniqueKey()))
          throw new IllegalArgumentException("Batch repeats a unique key");
        var value =
            Json.object().put("unique_skipped_as_duplicate", result.uniqueSkippedAsDuplicate());
        value.set("job", normalized(result.job()));
        results.add(value);
      }
      connection.releaseSavepoint(savepoint);
      return Json.object().set("results", results);
    } catch (Exception error) {
      connection.rollback(savepoint);
      connection.releaseSavepoint(savepoint);
      throw error;
    }
  }

  static JsonNode normalized(Job<?> job) {
    var value = (ObjectNode) Json.tree(job);
    if (value.path("metadata") instanceof ObjectNode metadata)
      metadata.remove("river:unique_nonce");
    if (!fitsDouble(value.path("metadata"))) value.putNull("metadata");
    value.put("state", job.state().value());
    if (job.uniqueStates() != null)
      value.set(
          "unique_states", Json.tree(job.uniqueStates().stream().map(Job.State::value).toList()));
    return value;
  }

  private Client scoped(JsonNode params) {
    return params.path("schema").asString("").isEmpty()
        ? river
        : new Client(database.withSchema(params.path("schema").asString()))
            .withExtension(runtime.instrumentation());
  }

  private static boolean fitsDouble(JsonNode value) {
    if (value.isNumber()) {
      try {
        return Double.isFinite(value.asDouble());
      } catch (RuntimeException e) {
        return false;
      }
    }
    for (var child : value) if (!fitsDouble(child)) return false;
    return true;
  }

  private static JsonNode page(JobQuery.Page page) {
    return Json.object()
        .put("cursor", page.cursor())
        .set("jobs", Json.tree(page.jobs().stream().map(Adapter::normalized).toList()));
  }

  private static JobQuery query(JsonNode params) {
    var ids = new ArrayList<Long>();
    for (var id : params.path("ids")) ids.add(id.asLong());
    var priorities = new ArrayList<Integer>();
    for (var priority : params.path("priorities")) priorities.add(priority.asInt());
    return new JobQuery(
        params.path("after").asString(null),
        params.path("direction").asString("asc").equals("desc"),
        ids,
        strings(params.path("kinds")),
        params.path("limit").asInt(100),
        params.has("metadata") ? params.path("metadata") : Json.object(),
        JobQuery.Order.valueOf(
            params.path("order_by").asString("time").toUpperCase(java.util.Locale.ROOT)),
        priorities,
        strings(params.path("queues")),
        strings(params.path("states")).stream().map(Job.State::of).toList(),
        strings(params.path("tags_all")),
        strings(params.path("tags_any")));
  }

  private static List<String> strings(JsonNode values) {
    var result = new ArrayList<String>();
    for (var value : values) result.add(value.asString());
    return List.copyOf(result);
  }

  private static InsertOptions options(JsonNode value) {
    var builder = InsertOptions.builder();
    if (value.has("max_attempts")) builder.maxAttempts(value.path("max_attempts").asInt());
    if (value.has("metadata")) builder.metadata(value.path("metadata"));
    if (value.has("pending")) builder.pending(value.path("pending").asBoolean());
    if (value.has("priority")) builder.priority(value.path("priority").asInt());
    if (value.has("queue")) builder.queue(value.path("queue").asString());
    if (value.has("scheduled_at"))
      builder.scheduledAt(Instant.parse(value.path("scheduled_at").asString()));
    if (value.has("tags")) {
      var tags = new ArrayList<String>();
      for (var tag : value.path("tags")) tags.add(tag.asString());
      builder.tags(tags.toArray(String[]::new));
    }
    if (value.has("unique")) builder.unique(unique(value.path("unique"), false));
    return builder.build();
  }

  private Connection transaction(JsonNode params) {
    var connection = transactions.get(params.path("handle").asString());
    if (connection == null) throw new Failure(-32001, "Transaction not found");
    return connection;
  }

  private static Unique unique(JsonNode value, boolean nanos) {
    var states = value.has("by_state") ? new HashSet<Job.State>() : null;
    if (states != null)
      for (var state : value.path("by_state")) states.add(Job.State.of(state.asString()));
    long period = value.path(nanos ? "by_period_nanos" : "by_period_ms").asLong(0);
    return new Unique(
        value.path("by_args").asBoolean(false),
        period == 0 ? null : nanos ? Duration.ofNanos(period) : Duration.ofMillis(period),
        value.path("by_queue").asBoolean(false),
        states,
        value.path("exclude_kind").asBoolean(false));
  }

  private void validate(JsonNode schema, JsonNode value) {
    if (schema.has("$ref")) {
      validate(contract.at(schema.path("$ref").asString().substring(1)), value);
      return;
    }
    var type = schema.path("type");
    if (!type.isMissingNode()) {
      var types = new ArrayList<String>();
      if (type.isString()) types.add(type.asString());
      else for (var t : type) types.add(t.asString());
      boolean valid =
          types.stream()
              .anyMatch(
                  t ->
                      switch (t) {
                        case "object" -> value.isObject();
                        case "array" -> value.isArray();
                        case "string" -> value.isString();
                        case "integer" -> value.isIntegralNumber();
                        case "number" -> value.isNumber();
                        case "boolean" -> value.isBoolean();
                        case "null" -> value.isNull();
                        default -> false;
                      });
      if (!valid) throw new Failure(-32602, "Invalid parameter type");
    }
    if (schema.has("enum")) {
      boolean found = false;
      for (var item : schema.path("enum")) if (item.equals(value)) found = true;
      if (!found) throw new Failure(-32602, "Invalid enum parameter");
    }
    if (value.isObject()) {
      for (var field : schema.path("required"))
        if (!value.has(field.asString()))
          throw new Failure(-32602, "Missing parameter: " + field.asString());
      for (var entry : value.properties()) {
        var child = schema.path("properties").path(entry.getKey());
        if (child.isMissingNode()) {
          if (schema.has("additionalProperties")
              && schema.path("additionalProperties").isBoolean()
              && !schema.path("additionalProperties").asBoolean())
            throw new Failure(-32602, "Unknown parameter: " + entry.getKey());
        } else validate(child, entry.getValue());
      }
    }
    if (value.isArray() && schema.has("items"))
      for (var item : value) validate(schema.path("items"), item);
    if (value.isNumber()
        && schema.has("minimum")
        && value.asDouble() < schema.path("minimum").asDouble())
      throw new Failure(-32602, "Parameter below minimum");
    if (value.isString()
        && schema.has("minLength")
        && value.asString().length() < schema.path("minLength").asInt())
      throw new Failure(-32602, "Parameter is too short");
  }

  public record Echo(String behavior, long durationMs, String message) {}

  static final class Failure extends RuntimeException {
    private static final long serialVersionUID = 1L;
    final int code;

    Failure(int code, String message) {
      super(message);
      this.code = code;
    }
  }
}
