package com.riverqueue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/** Internal query catalog; runtime SQL lives beside the migration resources. */
final class Sql {
  private static final Map<Database.Dialect, Map<String, String>> QUERIES =
      Map.of(
          Database.Dialect.POSTGRES,
          load("postgres.sql"),
          Database.Dialect.SQLITE,
          load("sqlite.sql"));

  private Sql() {}

  static PreparedStatement prepare(Connection connection, String sql, Object... params)
      throws SQLException {
    var statement = connection.prepareStatement(sql);
    try {
      for (int i = 0; i < params.length; i++) statement.setObject(i + 1, params[i]);
      return statement;
    } catch (SQLException e) {
      statement.close();
      throw e;
    }
  }

  static String query(Database database, String name) {
    String query = QUERIES.get(database.dialect()).get(name);
    if (query == null) throw new IllegalArgumentException("Unknown query: " + name);
    return query
        .replace("{schema}", database.prefix())
        .replace("{schema_name}", database.schema())
        .replace("{columns}", QUERIES.get(database.dialect()).get("columns"));
  }

  static String resource(String path) {
    try (var stream = Sql.class.getResourceAsStream(path)) {
      if (stream == null) throw new IllegalArgumentException("Missing SQL resource: " + path);
      return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static Map<String, String> load(String name) {
    var result = new HashMap<String, String>();
    for (String section : resource(name).split("(?m)^-- name: ")) {
      int newline = section.indexOf('\n');
      if (newline > 0)
        result.put(section.substring(0, newline).trim(), section.substring(newline + 1).strip());
    }
    return Map.copyOf(result);
  }
}
