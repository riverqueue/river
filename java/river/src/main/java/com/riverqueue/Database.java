package com.riverqueue;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import javax.sql.DataSource;

/** JDBC integration; applications may supply their existing connection pool. */
public final class Database {
  private final Connector connector;
  private final Dialect dialect;
  private final String schema;
  private volatile Capabilities capabilities;

  public Database(DataSource source, Dialect dialect) {
    this(source::getConnection, dialect, "");
  }

  private Database(Connector connector, Dialect dialect, String schema) {
    this.connector = connector;
    this.dialect = dialect;
    this.schema = schema;
    if (!schema.isEmpty() && !schema.matches("[a-zA-Z_][a-zA-Z0-9_]{0,45}"))
      throw new IllegalArgumentException("Invalid schema name");
    if (dialect == Dialect.SQLITE && !schema.isEmpty())
      throw new IllegalArgumentException("SQLite does not support custom schemas");
  }

  public static Database connect(String url) {
    return connect(url, "river-java");
  }

  public static Database connect(String url, String applicationName) {
    if (url.startsWith("sqlite:") || url.startsWith("jdbc:sqlite:")) {
      String jdbc = url.startsWith("jdbc:") ? url : "jdbc:" + url;
      return new Database(() -> DriverManager.getConnection(jdbc), Dialect.SQLITE, "");
    }
    String jdbc;
    var properties = new Properties();
    properties.setProperty("ApplicationName", applicationName);
    properties.setProperty("user", System.getProperty("user.name"));
    if (url.startsWith("jdbc:")) jdbc = url;
    else {
      URI uri = URI.create(url);
      if (uri.getRawUserInfo() != null) {
        // URI credentials use percent encoding; URLDecoder otherwise turns literal '+' into spaces.
        String[] auth = uri.getRawUserInfo().replace("+", "%2B").split(":", 2);
        properties.setProperty("user", URLDecoder.decode(auth[0], StandardCharsets.UTF_8));
        if (auth.length > 1)
          properties.setProperty("password", URLDecoder.decode(auth[1], StandardCharsets.UTF_8));
      }
      jdbc =
          "jdbc:postgresql://"
              + uri.getHost()
              + (uri.getPort() < 0 ? "" : ":" + uri.getPort())
              + uri.getRawPath()
              + (uri.getRawQuery() == null ? "" : "?" + uri.getRawQuery());
    }
    String target = jdbc;
    return new Database(
        () -> DriverManager.getConnection(target, properties), Dialect.POSTGRES, "");
  }

  public Connection connection() throws SQLException {
    var connection = connector.open();
    try {
      if (dialect == Dialect.SQLITE) {
        try (var statement = connection.createStatement()) {
          statement.execute("PRAGMA busy_timeout=5000");
          statement.execute("PRAGMA foreign_keys=ON");
          // Changing journal mode can return BUSY immediately despite busy_timeout when two
          // processes open a fresh database. Retry initialization within the same five-second
          // limit.
          long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
          while (true) {
            try {
              try (var rows = statement.executeQuery("PRAGMA journal_mode")) {
                if (rows.next() && rows.getString(1).equalsIgnoreCase("wal")) break;
              }
              statement.execute("PRAGMA journal_mode=WAL");
              break;
            } catch (SQLException error) {
              if ((error.getErrorCode() != 5 && error.getErrorCode() != 6)
                  || System.nanoTime() >= deadline) throw error;
              try {
                Thread.sleep(25);
              } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new SQLException("Interrupted configuring SQLite journal mode", interrupted);
              }
            }
          }
        }
      }
      return connection;
    } catch (SQLException error) {
      connection.close();
      throw error;
    }
  }

  public Dialect dialect() {
    return dialect;
  }

  public String schema() {
    return schema;
  }

  boolean supportsNotifications(Connection connection) throws SQLException {
    return dialect == Dialect.SQLITE || capabilities(connection).notifications();
  }

  boolean usesUniqueNonce(Connection connection) throws SQLException {
    return dialect == Dialect.SQLITE || capabilities(connection).uniqueNonce();
  }

  private Capabilities capabilities(Connection connection) throws SQLException {
    var detected = capabilities;
    if (detected == null)
      try (var statement = Sql.prepare(connection, Sql.query(this, "database_version"));
          var rows = statement.executeQuery()) {
        rows.next();
        String version = rows.getString(1).toLowerCase(java.util.Locale.ROOT);
        boolean yugabyte = version.contains("yugabyte") || version.contains("-yb");
        detected = new Capabilities(!yugabyte || rows.getBoolean(2), yugabyte);
        capabilities = detected;
      }
    return detected;
  }

  public Database withSchema(String name) {
    return new Database(connector, dialect, name);
  }

  String prefix() {
    return schema.isEmpty() ? "" : '"' + schema + "\".";
  }

  Object timestamp(Instant value) {
    if (value == null) return null;
    if (dialect == Dialect.POSTGRES)
      return OffsetDateTime.ofInstant(value.truncatedTo(ChronoUnit.MICROS), ZoneOffset.UTC);
    value = value.plusNanos(500000).truncatedTo(ChronoUnit.MILLIS);
    return DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS")
        .withZone(ZoneOffset.UTC)
        .format(value);
  }

  public static Instant instant(String value) {
    if (value == null) return null;
    String normalized = value.replace(' ', 'T');
    if (!normalized.endsWith("Z")
        && !normalized.substring(10).contains("+")
        && !normalized.substring(10).contains("-")) normalized += "Z";
    if (normalized.matches(".*[+-][0-9]{2}$")) normalized += ":00";
    return OffsetDateTime.parse(normalized).toInstant();
  }

  private record Capabilities(boolean notifications, boolean uniqueNonce) {}

  @FunctionalInterface
  private interface Connector {
    Connection open() throws SQLException;
  }

  public enum Dialect {
    POSTGRES,
    SQLITE
  }
}
