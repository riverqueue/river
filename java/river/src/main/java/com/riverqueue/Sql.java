package com.riverqueue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/** Internal query catalog; runtime SQL lives beside the migration resources. */
final class Sql {
  private static final Map<Database.Dialect, Map<String, String>> QUERIES =
      Map.of(
          Database.Dialect.MYSQL,
          load("mysql.sql"),
          Database.Dialect.POSTGRES,
          load("postgres.sql"),
          Database.Dialect.SQLITE,
          load("sqlite.sql"));

  private Sql() {}

  /** Executes a row-returning write within the caller's transaction. */
  static Mutation mutation(Database database, Connection connection, String name, Object... params)
      throws SQLException {
    return new Mutation(database, connection, name, params);
  }

  static final class Mutation implements AutoCloseable {
    private final Database database;
    private final Connection connection;
    private final String name;
    private final Object[] params;
    private ResultSet rows;

    private Mutation(Database database, Connection connection, String name, Object[] params) {
      this.database = database;
      this.connection = connection;
      this.name = name;
      this.params = params;
    }

    ResultSet executeQuery() throws SQLException {
      if (database.dialect() == Database.Dialect.MYSQL) {
        rows = MySql.mutate(database, connection, name, params);
      } else {
        String sql = query(database, name);
        if (name.equals("insert") && database.dialect() == Database.Dialect.POSTGRES)
          sql =
              sql.replace(
                  "{duplicate}",
                  query(
                      database,
                      database.usesUniqueNonce(connection)
                          ? "insert_duplicate_nonce"
                          : "insert_duplicate_xmax"));
        rows = queryRows(connection, sql, params);
      }
      return rows;
    }

    @Override
    public void close() throws SQLException {
      if (rows != null) rows.close();
    }
  }

  static ResultSet queryRows(Connection connection, String sql, Object... params)
      throws SQLException {
    var statement = prepare(connection, sql, params);
    try {
      statement.closeOnCompletion();
      return statement.executeQuery();
    } catch (SQLException error) {
      statement.close();
      throw error;
    }
  }

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
