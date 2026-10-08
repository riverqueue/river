package com.riverqueue.conformance;

import com.riverqueue.Database;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;
import javax.sql.DataSource;

/** Exercises the same application-owned pooling integration used in deployments. */
final class Connections implements DataSource {
  private final Database source;

  private Connections(Database source) {
    this.source = source;
  }

  static HikariDataSource pool(Database source) {
    var config = new HikariConfig();
    config.setDataSource(new Connections(source));
    config.setMaximumPoolSize(8);
    config.setMinimumIdle(0);
    config.setConnectionTimeout(10_000);
    config.setInitializationFailTimeout(-1);
    return new HikariDataSource(config);
  }

  @Override
  public Connection getConnection() throws SQLException {
    return source.connection();
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    throw new SQLFeatureNotSupportedException("Use the configured database credentials");
  }

  @Override
  public PrintWriter getLogWriter() {
    return null;
  }

  @Override
  public int getLoginTimeout() {
    return 0;
  }

  @Override
  public Logger getParentLogger() {
    return Logger.getLogger("com.riverqueue");
  }

  @Override
  public boolean isWrapperFor(Class<?> type) {
    return type.isInstance(this);
  }

  @Override
  public void setLogWriter(PrintWriter writer) {}

  @Override
  public void setLoginTimeout(int seconds) {}

  @Override
  public <T> T unwrap(Class<T> type) throws SQLException {
    if (type.isInstance(this)) return type.cast(this);
    throw new SQLException("Not a wrapper for " + type.getName());
  }
}
