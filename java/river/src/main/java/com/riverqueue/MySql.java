package com.riverqueue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import javax.sql.rowset.RowSetProvider;
import tools.jackson.databind.node.ObjectNode;

/** MySQL's locking writes and readback; all SQL remains in the query catalog. */
final class MySql {
  private MySql() {}

  static ResultSet mutate(Database database, Connection connection, String name, Object[] params)
      throws SQLException {
    if (connection.getAutoCommit())
      throw new SQLException("MySQL row-returning writes require a transaction");
    return switch (name) {
      case "claim", "claim_known" -> claim(database, connection, name, params);
      case "delete" -> delete(database, connection, params);
      case "insert" -> {
        update(database, connection, name, params);
        yield rows(database, connection, "inserted");
      }
      case "leader_elect" -> {
        update(database, connection, name, params);
        yield rows(database, connection, "leader_elected", params[0], params[1]);
      }
      case "complete", "rescue_update" -> {
        int idIndex = name.equals("complete") ? 9 : 7;
        int metadataIndex = name.equals("complete") ? 6 : 4;
        Object[] merged = params.clone();
        try (var before = rows(database, connection, "lock_get", params[idIndex])) {
          if (before.next()) {
            ObjectNode metadata = (ObjectNode) Json.parse(before.getString("metadata"));
            metadata.setAll((ObjectNode) Json.parse((String) params[metadataIndex]));
            merged[metadataIndex] = Json.encode(metadata);
          }
        }
        boolean changed = update(database, connection, name, merged) != 0;
        yield rows(database, connection, changed ? "lock_get" : "empty_jobs", params[idIndex]);
      }
      case "cancel", "output", "retry", "queue_update", "schedule_available" -> {
        Object id = params[name.equals("retry") ? 1 : params.length - 1];
        boolean changed = update(database, connection, name, params) != 0;
        String read =
            switch (name) {
              case "queue_update" -> changed ? "queue_get" : "empty_queues";
              case "schedule_available" -> changed ? "scheduled_queue" : "empty_queues";
              default -> changed ? "lock_get" : "empty_jobs";
            };
        yield rows(database, connection, read, id);
      }
      default -> throw new IllegalArgumentException("Unknown MySQL mutation: " + name);
    };
  }

  private static ResultSet claim(
      Database database, Connection connection, String name, Object[] params) throws SQLException {
    var ids = new ArrayList<Long>();
    try (var selected =
        rows(
            database, connection, name + "_select", Arrays.copyOfRange(params, 2, params.length))) {
      while (selected.next()) ids.add(selected.getLong("id"));
    }
    if (ids.isEmpty()) return rows(database, connection, "empty_jobs", 0);
    for (long id : ids) update(database, connection, "claim_one", params[0], params[1], id);
    return rows(database, connection, "claimed", Json.encode(ids));
  }

  private static ResultSet delete(Database database, Connection connection, Object[] params)
      throws SQLException {
    var snapshot = RowSetProvider.newFactory().createCachedRowSet();
    try (var selected = rows(database, connection, "delete_rows", params)) {
      snapshot.populate(selected);
    }
    try {
      update(database, connection, "delete", params);
      return snapshot;
    } catch (SQLException error) {
      snapshot.close();
      throw error;
    }
  }

  private static ResultSet rows(
      Database database, Connection connection, String name, Object... params) throws SQLException {
    return Sql.queryRows(connection, Sql.query(database, name), params);
  }

  private static int update(Database database, Connection connection, String name, Object... params)
      throws SQLException {
    try (var statement = Sql.prepare(connection, Sql.query(database, name), params)) {
      return statement.executeUpdate();
    }
  }

  /** Splits migration scripts without enabling multi-statement application queries. */
  static List<String> statements(String script) {
    var tokens =
        Pattern.compile(
                "'(?:''|\\\\.|[^'\\\\])*'|\"(?:\"\"|\\\\.|[^\"\\\\])*\"|`(?:``|[^`])*`|--[^\\n]*|#[^\\n]*|/\\*.*?\\*/|;|.",
                Pattern.DOTALL)
            .matcher(script);
    var statements = new ArrayList<String>();
    var buffer = new StringBuilder();
    while (tokens.find()) {
      String token = tokens.group();
      if (token.startsWith("--") || token.startsWith("#") || token.startsWith("/*")) {
        buffer.append(' ');
      } else if (token.equals(";")) {
        if (!buffer.toString().isBlank()) statements.add(buffer.toString().strip());
        buffer.setLength(0);
      } else buffer.append(token);
    }
    if (!buffer.toString().isBlank()) statements.add(buffer.toString().strip());
    return List.copyOf(statements);
  }
}
