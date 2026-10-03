package com.riverqueue;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import tools.jackson.databind.JsonNode;

/** Immutable job filters and portable pagination cursors; defaults to ascending job ID order. */
public record JobQuery(
    String after,
    boolean descending,
    List<Long> ids,
    List<String> kinds,
    int limit,
    JsonNode metadata,
    Order order,
    List<Integer> priorities,
    List<String> queues,
    List<Job.State> states,
    List<String> tagsAll,
    List<String> tagsAny) {
  public JobQuery {
    ids = List.copyOf(ids);
    kinds = List.copyOf(kinds);
    priorities = List.copyOf(priorities);
    queues = List.copyOf(queues);
    states = List.copyOf(states);
    tagsAll = List.copyOf(tagsAll);
    tagsAny = List.copyOf(tagsAny);
    metadata = metadata.deepCopy();
    if (limit < 1 || limit > 10000)
      throw new IllegalArgumentException("List limit must be between 1 and 10000");
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public JsonNode metadata() {
    return metadata.deepCopy();
  }

  /** Fluent construction for combined filters without a positional argument list. */
  public static final class Builder {
    private String after;
    private boolean descending;
    private List<Long> ids = List.of();
    private List<String> kinds = List.of();
    private int limit = 100;
    private JsonNode metadata = Json.object();
    private Order order = Order.ID;
    private List<Integer> priorities = List.of();
    private List<String> queues = List.of();
    private List<Job.State> states = List.of();
    private List<String> tagsAll = List.of();
    private List<String> tagsAny = List.of();

    private Builder() {}

    public Builder after(String value) {
      after = value;
      return this;
    }

    public JobQuery build() {
      return new JobQuery(
          after,
          descending,
          ids,
          kinds,
          limit,
          metadata,
          order,
          priorities,
          queues,
          states,
          tagsAll,
          tagsAny);
    }

    public Builder descending(boolean value) {
      descending = value;
      return this;
    }

    public Builder ids(long... values) {
      ids = java.util.Arrays.stream(values).boxed().toList();
      return this;
    }

    public Builder kinds(String... values) {
      kinds = List.of(values);
      return this;
    }

    public Builder limit(int value) {
      limit = value;
      return this;
    }

    public Builder metadata(Object value) {
      metadata = Json.tree(value);
      return this;
    }

    public Builder order(Order value) {
      order = java.util.Objects.requireNonNull(value);
      return this;
    }

    public Builder priorities(Integer... values) {
      priorities = List.of(values);
      return this;
    }

    public Builder queues(String... values) {
      queues = List.of(values);
      return this;
    }

    public Builder states(Job.State... values) {
      states = List.of(values);
      return this;
    }

    public Builder tagsAll(String... values) {
      tagsAll = List.of(values);
      return this;
    }

    public Builder tagsAny(String... values) {
      tagsAny = List.of(values);
      return this;
    }
  }

  public static JobQuery all() {
    return new JobQuery(
        null,
        false,
        List.of(),
        List.of(),
        100,
        Json.object(),
        Order.ID,
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of());
  }

  public JobQuery after(String value) {
    return new JobQuery(
        value,
        descending,
        ids,
        kinds,
        limit,
        metadata,
        order,
        priorities,
        queues,
        states,
        tagsAll,
        tagsAny);
  }

  public JobQuery limit(int value) {
    return new JobQuery(
        after,
        descending,
        ids,
        kinds,
        value,
        metadata,
        order,
        priorities,
        queues,
        states,
        tagsAll,
        tagsAny);
  }

  public JobQuery states(Job.State... value) {
    return new JobQuery(
        after,
        descending,
        ids,
        kinds,
        limit,
        metadata,
        order,
        priorities,
        queues,
        List.of(value),
        tagsAll,
        tagsAny);
  }

  boolean hasFilters() {
    return !ids.isEmpty()
        || !kinds.isEmpty()
        || hasMetadataFilter()
        || !priorities.isEmpty()
        || !queues.isEmpty()
        || !states.isEmpty()
        || !tagsAll.isEmpty()
        || !tagsAny.isEmpty();
  }

  private boolean hasMetadataFilter() {
    return !metadata.isObject() || !metadata.isEmpty();
  }

  private static String sqliteMetadata(
      Database database,
      List<Object> parameters,
      JsonNode wanted,
      String type,
      String value,
      int depth) {
    // Array containment is unordered, but each wanted object must match one complete element.
    // Correlated predicates retain that boundary instead of comparing independent JSON paths.
    String query;
    if (wanted.isContainer()) {
      var children = new ArrayList<String>();
      String alias = "metadata_" + depth;
      if (wanted.isObject()) {
        for (var entry : wanted.properties()) {
          parameters.add(entry.getKey());
          children.add(
              Sql.query(database, "filter_metadata_member")
                  .replace(
                      "{predicate}",
                      sqliteMetadata(
                          database,
                          parameters,
                          entry.getValue(),
                          alias + ".type",
                          alias + ".value",
                          depth + 1)));
        }
      } else {
        for (var element : wanted) {
          children.add(
              Sql.query(database, "filter_metadata_element")
                  .replace(
                      "{predicate}",
                      sqliteMetadata(
                          database,
                          parameters,
                          element,
                          alias + ".type",
                          alias + ".value",
                          depth + 1)));
        }
      }
      query =
          Sql.query(database, "filter_metadata_container")
              .replace("{children}", children.isEmpty() ? "true" : String.join(" AND ", children))
              .replace("{kind}", wanted.isObject() ? "object" : "array")
              .replace("{alias}", alias);
    } else {
      parameters.add(Json.encode(wanted));
      query = Sql.query(database, "filter_metadata_scalar");
    }
    return query.replace("{type}", type).replace("{value}", value);
  }

  String timeField() {
    return switch (order) {
      case ID -> "";
      case FINALIZED_AT -> "finalized_at";
      case SCHEDULED_AT -> "scheduled_at";
      case TIME ->
          states.isEmpty()
              ? "scheduled_at"
              : switch (states.getFirst()) {
                case AVAILABLE, PENDING, RETRYABLE, SCHEDULED -> "scheduled_at";
                case RUNNING -> "attempted_at";
                case CANCELLED, COMPLETED, DISCARDED -> "finalized_at";
              };
    };
  }

  String cursor(Job<?> job) {
    Instant time =
        switch (timeField()) {
          case "created_at" -> job.createdAt();
          case "scheduled_at" -> job.scheduledAt();
          case "attempted_at" -> job.attemptedAt();
          case "finalized_at" -> job.finalizedAt();
          default -> null;
        };
    var cursor =
        Json.object()
            .put("id", job.id())
            .put("kind", job.kind())
            .put("queue", job.queue())
            .put("sort_field", order.name().toLowerCase(java.util.Locale.ROOT));
    cursor.set("time", Json.tree(time == null ? Instant.parse("0001-01-01T00:00:00Z") : time));
    return Base64.getUrlEncoder()
        .encodeToString(Json.encode(cursor).getBytes(StandardCharsets.UTF_8));
  }

  String deleteSql(Database database, List<Object> parameters) {
    return Sql.query(database, "delete_filtered").replace("{where}", where(database, parameters));
  }

  String sql(Database database, List<Object> parameters, boolean forDelete) {
    String where = where(database, parameters);
    String field = timeField();
    String sort = descending ? "DESC" : "ASC";
    String orderSQL =
        (field.isEmpty()
                ? ""
                : field + " " + sort + (descending ? " NULLS FIRST, " : " NULLS LAST, "))
            + "id "
            + sort;
    parameters.add(limit);
    return Sql.query(database, forDelete ? "list_deletable" : "list")
        .replace("{where}", where)
        .replace("{order}", orderSQL);
  }

  private String where(Database database, List<Object> parameters) {
    var filters = new ArrayList<String>();
    filter(database, filters, parameters, "ids", ids);
    filter(database, filters, parameters, "kinds", kinds);
    filter(database, filters, parameters, "priorities", priorities);
    filter(database, filters, parameters, "queues", queues);
    filter(database, filters, parameters, "states", states.stream().map(Job.State::value).toList());
    filter(database, filters, parameters, "tags_all", tagsAll);
    filter(database, filters, parameters, "tags_any", tagsAny);
    if (hasMetadataFilter()) {
      if (database.dialect() == Database.Dialect.SQLITE)
        filters.add(
            sqliteMetadata(database, parameters, metadata, "json_type(metadata)", "metadata", 0));
      else {
        filters.add(Sql.query(database, "filter_metadata"));
        parameters.add(Json.encode(metadata));
      }
    }
    String field = timeField();
    if (after != null) {
      JsonNode cursor;
      try {
        cursor =
            Json.parse(new String(Base64.getUrlDecoder().decode(after), StandardCharsets.UTF_8));
      } catch (RuntimeException e) {
        throw new IllegalArgumentException("Invalid job cursor", e);
      }
      long id = cursor.path("id").asLong();
      Instant time = Instant.parse(cursor.path("time").asString());
      boolean zero = time.equals(Instant.parse("0001-01-01T00:00:00Z"));
      boolean nullable = field.equals("finalized_at") || field.equals("attempted_at");
      String query;
      if (field.isEmpty() || (zero && !nullable)) {
        query = Sql.query(database, "cursor_id");
        parameters.add(id);
      } else if (zero) {
        query = Sql.query(database, descending ? "cursor_null_desc" : "cursor_null_asc");
        parameters.add(id);
      } else {
        query =
            Sql.query(database, nullable && !descending ? "cursor_time_nullable" : "cursor_time");
        parameters.add(database.timestamp(time));
        parameters.add(database.timestamp(time));
        parameters.add(id);
      }
      filters.add(query.replace("{field}", field).replace("{comparison}", descending ? "<" : ">"));
    }
    return filters.isEmpty() ? "true" : String.join(" AND ", filters);
  }

  private static void filter(
      Database database, List<String> clauses, List<Object> params, String name, List<?> values) {
    if (values.isEmpty()) return;
    clauses.add(Sql.query(database, "filter_" + name));
    params.add(Json.encode(values));
  }

  public enum Order {
    FINALIZED_AT,
    ID,
    SCHEDULED_AT,
    TIME
  }

  public record Page(String cursor, List<Job<JsonNode>> jobs) {
    public Page {
      jobs = List.copyOf(jobs);
    }
  }
}
