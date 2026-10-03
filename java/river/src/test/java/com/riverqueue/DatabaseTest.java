package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@Isolated("Captures JDBC connection properties without opening a database connection")
class DatabaseTest {
  @ParameterizedTest
  @CsvSource({
    "alice+tag:p+ass, alice+tag, p+ass",
    "alice%2Btag:p%2Bass, alice+tag, p+ass",
    "alice%20tag:p%20ass, alice tag, p ass",
    "alice%3Atag:p%40ss%3A%2F%25, alice:tag, p@ss:/%",
    "alice%252Btag:p%252Bass, alice%2Btag, p%2Bass",
    "caf%C3%A9:p%C3%A4ss, café, päss"
  })
  void postgresUriPreservesCredentials(String userInfo, String user, String password)
      throws Exception {
    var originalDrivers = Collections.list(DriverManager.getDrivers());
    var captured = new AtomicReference<Properties>();
    var target = new AtomicReference<String>();
    var driver =
        new org.postgresql.Driver() {
          @Override
          public Connection connect(String url, Properties properties) throws SQLException {
            captured.set(properties);
            target.set(url);
            throw new SQLException("Captured connection attempt");
          }
        };
    try {
      // DriverManager tries drivers in registration order; keep the real drivers from connecting.
      for (var original : originalDrivers) DriverManager.deregisterDriver(original);
      DriverManager.registerDriver(driver);
      var database =
          Database.connect(
              "postgres://" + userInfo + "@localhost:5432/river_test?sslmode=require", "uri-test");
      assertEquals(
          "Captured connection attempt",
          assertThrows(SQLException.class, database::connection).getMessage());
      assertEquals(user, captured.get().getProperty("user"));
      assertEquals(password, captured.get().getProperty("password"));
      assertEquals("uri-test", captured.get().getProperty("ApplicationName"));
      assertEquals("jdbc:postgresql://localhost:5432/river_test?sslmode=require", target.get());
    } finally {
      DriverManager.deregisterDriver(driver);
      for (var original : originalDrivers) DriverManager.registerDriver(original);
    }
  }
}
