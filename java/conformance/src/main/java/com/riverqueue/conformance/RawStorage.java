package com.riverqueue.conformance;

import com.riverqueue.Client;
import com.riverqueue.Database;
import com.riverqueue.Json;
import java.sql.SQLException;
import java.util.HexFormat;
import java.util.List;
import tools.jackson.databind.JsonNode;

/** Deliberate out-of-band SQL for the harness's storage and fault probes. */
final class RawStorage {
  private RawStorage() {}

  static JsonNode handle(Database database, String method, JsonNode params) throws SQLException {
    boolean sqlite = database.dialect() == Database.Dialect.SQLITE;
    try (var connection = database.connection()) {
      if (method.equals("raw_insert_full_row")) {
        String sql =
            """
          INSERT INTO river_job (args,attempt,attempted_at,attempted_by,created_at,errors,
          finalized_at,kind,max_attempts,metadata,priority,queue,scheduled_at,state,tags,unique_key,unique_states)
          VALUES ('{"nested":{"enabled":true},"values":[1,"two",null]}'::jsonb,
          3,'2026-01-02T03:04:06.123456Z',ARRAY['go-client','candidate-client'],'2026-01-02T03:04:05.6789Z',
          ARRAY[?::jsonb],'2026-01-02T03:04:07.000001Z','conformance_full_row',4,
          '{"output":{"ok":true},"river:rescue_count":2,"user":"metadata"}'::jsonb,
          2,'priority_jobs','2026-01-02T03:04:05.999999Z','discarded',ARRAY['alpha_tag','beta_tag'],decode(repeat('ab',32),'hex'),B'11110101') RETURNING id
          """;
        long id;
        try (var statement = connection.prepareStatement(sql)) {
          statement.setString(
              1,
              Json.encode(
                  new com.riverqueue.Job.AttemptError(
                      java.time.Instant.parse("2026-01-02T03:04:06.123456Z"),
                      3,
                      "worker failed: escaped \"detail\"",
                      "frame one\nframe two")));
          try (var rows = statement.executeQuery()) {
            rows.next();
            id = rows.getLong(1);
          }
        }
        return Adapter.normalized(new Client(database).get(id));
      }
      if (method.equals("leader")) {
        try (var statement = connection.createStatement();
            var rows = statement.executeQuery("SELECT leader_id,elected_at FROM river_leader")) {
          var value = Json.object().putNull("leader_id").putNull("elected_at");
          if (rows.next()) {
            value.put("leader_id", rows.getString(1));
            value.set("elected_at", Json.tree(Database.instant(rows.getString(2))));
          }
          return value;
        }
      }
      if (method.equals("request_resign")) {
        try (var statement =
            connection.prepareStatement(
                sqlite
                    ? "INSERT INTO river_notification(topic,payload) VALUES ('river_leadership',?)"
                    : "SELECT pg_notify(current_schema()||'.river_leadership',?)")) {
          statement.setString(1, "{\"action\":\"request_resign\",\"leader_id\":\"\"}");
          statement.execute();
          return Json.object();
        }
      }
      if (method.equals("fault_expire_leader")) {
        try (var statement = connection.createStatement()) {
          statement.executeUpdate("UPDATE river_leader SET expires_at='2000-01-01 00:00:00'");
        }
        return Json.object();
      }
      if (method.equals("connection_count") || method.equals("listener_count")) {
        try (var statement =
            connection.prepareStatement(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name=?"
                    + (method.equals("listener_count") ? " AND query LIKE 'LISTEN%'" : ""))) {
          statement.setString(
              1,
              System.getenv()
                  .getOrDefault("RIVER_CONFORMANCE_APPLICATION_NAME", "river-conformance-java"));
          try (var rows = statement.executeQuery()) {
            rows.next();
            return Json.object().put("count", rows.getLong(1));
          }
        }
      }
      if (method.equals("fault_disconnect_application")
          || method.equals("fault_disconnect_listeners")) {
        String name =
            method.equals("fault_disconnect_application")
                ? params.path("application_name").asString()
                : System.getenv()
                    .getOrDefault("RIVER_CONFORMANCE_APPLICATION_NAME", "river-conformance-java");
        if (!name.startsWith("river-conformance-"))
          throw new IllegalArgumentException("Only conformance connections may be terminated");
        try (var statement =
            connection.prepareStatement(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name=? AND pid<>pg_backend_pid()"
                    + (method.equals("fault_disconnect_listeners")
                        ? " AND query LIKE 'LISTEN%'"
                        : ""))) {
          statement.setString(1, name);
          int count = 0;
          try (var rows = statement.executeQuery()) {
            while (rows.next()) count++;
          }
          return Json.object().put("count", count);
        }
      }
      if (method.equals("raw_notifications")) {
        var notifications = Json.MAPPER.createArrayNode();
        try (var statement =
            connection.prepareStatement(
                "SELECT id,payload,topic FROM river_notification WHERE id>? ORDER BY id")) {
          statement.setLong(1, params.path("after_id").asLong());
          try (var rows = statement.executeQuery()) {
            while (rows.next())
              notifications.add(
                  Json.object()
                      .put("id", rows.getLong(1))
                      .put("payload", rows.getString(2))
                      .put("payload_type", "text")
                      .put("topic", rows.getString(3)));
          }
        }
        return Json.object().set("notifications", notifications);
      }
      if (method.equals("raw_insert_no_notify")) {
        String args =
            Json.encode(
                new Adapter.Echo(
                    params.path("behavior").asString(""),
                    params.path("duration_ms").asLong(0),
                    params.path("message").asString("")));
        long id;
        try (var statement =
            connection.prepareStatement(
                "INSERT INTO river_job(args,kind,max_attempts) VALUES ("
                    + (sqlite ? "jsonb(?)" : "?::jsonb")
                    + ",?,?) RETURNING id")) {
          statement.setString(1, args);
          statement.setString(2, params.path("kind").asString("conformance_echo"));
          statement.setInt(3, params.path("opts").path("max_attempts").asInt(25));
          try (var rows = statement.executeQuery()) {
            rows.next();
            id = rows.getLong(1);
          }
        }
        return Adapter.normalized(new Client(database).get(id));
      }
      if (method.equals("raw_finalize")) {
        String error =
            "{\"at\":\"2026-02-03T04:05:06.789Z\",\"attempt\":1,\"error\":\"external discard\",\"trace\":\"external trace\"}";
        String sql =
            "UPDATE river_job SET state="
                + (sqlite ? "?" : "?::river_job_state")
                + ",finalized_at="
                + (sqlite ? "datetime('now','subsec')" : "now()")
                + ",metadata="
                + (sqlite ? "jsonb_patch(metadata,jsonb(?))" : "metadata||?::jsonb")
                + ",errors="
                + (params.path("state").asString().equals("completed")
                    ? (sqlite
                        ? "CASE WHEN ? IS NULL THEN errors ELSE errors END"
                        : "CASE WHEN ?::text IS NULL THEN errors ELSE errors END")
                    : (sqlite
                        ? "jsonb_insert(coalesce(errors,jsonb('[]')),'$[#]',jsonb(?))"
                        : "array_append(errors,?::jsonb)"))
                + " WHERE id=? AND state='running'";
        try (var statement = connection.prepareStatement(sql)) {
          statement.setString(1, params.path("state").asString());
          statement.setString(
              2, params.has("metadata") ? Json.encode(params.path("metadata")) : "{}");
          statement.setString(3, error);
          statement.setLong(4, params.path("id").asLong());
          statement.executeUpdate();
        }
        return Adapter.normalized(new Client(database).get(params.path("id").asLong()));
      }
      if (method.equals("raw_replace_json_text")) {
        String column = params.path("column").asString();
        if (!List.of("args", "attempted_by", "errors", "metadata", "tags").contains(column))
          throw new IllegalArgumentException("Invalid JSON column");
        var result = Json.object();
        try (var statement =
            connection.prepareStatement(
                "SELECT CASE WHEN typeof("
                    + column
                    + ")='text' THEN "
                    + column
                    + " ELSE json("
                    + column
                    + ") END,typeof("
                    + column
                    + ") FROM river_job WHERE id=?")) {
          statement.setLong(1, params.path("id").asLong());
          try (var rows = statement.executeQuery()) {
            rows.next();
            result.put("previous", rows.getString(1)).put("previous_type", rows.getString(2));
          }
        }
        try (var statement =
            connection.prepareStatement("UPDATE river_job SET " + column + "=? WHERE id=?")) {
          statement.setString(1, params.path("text").asString(null));
          statement.setLong(2, params.path("id").asLong());
          statement.executeUpdate();
        }
        return result;
      }
      if (method.equals("raw_insert_exact_json")) {
        String sql =
            sqlite
                ? "INSERT INTO river_job (id,args,kind,metadata) VALUES (?,jsonb(?),'conformance_exact_json',jsonb(?)) RETURNING id"
                : "INSERT INTO river_job (id,args,kind,metadata) VALUES (coalesce(?,nextval('river_job_id_seq')),?::jsonb,'conformance_exact_json',?::jsonb) RETURNING id";
        try (var statement = connection.prepareStatement(sql)) {
          statement.setObject(1, params.has("id") ? params.path("id").asLong() : null);
          statement.setString(
              2, "{\"decimal\":0.12345678901234567890123456789,\"integer\":9223372036854775807}");
          statement.setString(
              3, params.path("metadata_json").asString("{\"negative\":-9223372036854775808}"));
          try (var rows = statement.executeQuery()) {
            rows.next();
            return Json.object().put("id", rows.getLong(1));
          }
        }
      }
      String projection = "*";
      if (sqlite)
        projection =
            "*, json(args) AS args_text,json(metadata) AS metadata_text,json(attempted_by) AS attempted_by_text,json(errors) AS errors_text,json(tags) AS tags_text,typeof(unique_key) AS key_type,typeof(unique_states) AS states_type";
      else
        projection =
            "*,args::text AS args_text,metadata::text AS metadata_text,array_to_json(attempted_by)::text AS attempted_by_text,array_to_json(errors)::text AS errors_text,array_to_json(tags)::text AS tags_text,pg_typeof(unique_key)::text AS key_type,pg_typeof(unique_states)::text AS states_type";
      try (var statement =
          connection.prepareStatement("SELECT " + projection + " FROM river_job WHERE id=?")) {
        statement.setLong(1, params.path("id").asLong());
        try (var rows = statement.executeQuery()) {
          if (!rows.next()) throw new IllegalArgumentException("Raw job not found");
          var result = Json.object();
          if (method.equals("raw_job_exact_json")) {
            for (String column : List.of("args", "metadata"))
              for (var entry : Json.members(rows.getString(column + "_text")).entrySet())
                if (List.of(
                        "decimal",
                        "integer",
                        "negative",
                        "big_integer",
                        "beyond_float",
                        "long_decimal")
                    .contains(entry.getKey())) result.put(entry.getKey(), entry.getValue());
            return result;
          }
          if (method.equals("raw_job_timestamps"))
            return result
                .put("created_at", rows.getString("created_at"))
                .put("scheduled_at", rows.getString("scheduled_at"));
          var binary = Json.object();
          for (String column : List.of("args", "attempted_by", "errors", "metadata", "tags")) {
            result.put(column, rows.getString(column + "_text"));
            if (sqlite) {
              byte[] data = rows.getBytes(column);
              binary.put(
                  column, data == null ? null : HexFormat.of().withUpperCase().formatHex(data));
            }
          }
          for (String column :
              List.of("attempted_at", "created_at", "finalized_at", "scheduled_at"))
            result.put(column, rows.getString(column));
          byte[] key = rows.getBytes("unique_key");
          result.put(
              "unique_key", key == null ? null : HexFormat.of().withUpperCase().formatHex(key));
          result.put("unique_key_type", key == null || !sqlite ? null : rows.getString("key_type"));
          result.put("unique_states", rows.getString("unique_states"));
          result.put(
              "unique_states_type",
              rows.getString("unique_states") == null || !sqlite
                  ? null
                  : rows.getString("states_type"));
          result.set("jsonb", sqlite ? binary : Json.MAPPER.nullNode());
          return result;
        }
      }
    }
  }
}
