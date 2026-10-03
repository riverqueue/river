package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Tag("postgres")
@EnabledIfEnvironmentVariable(named = "RIVER_TEST_DATABASE_URL", matches = ".+")
class PostgresCapabilitiesTest {
  private static final JobType<Args> TYPE = JobType.of("yugabyte_test", Args.class);
  @RegisterExtension final TestDatabase databases = new TestDatabase();
  @TempDir Path directory;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void yugabyteInsertsUseNoncesWithoutPostgresSystemColumns(boolean notificationsEnabled) {
    var database = databases.open(directory.resolve("river.db"));
    Assumptions.assumeTrue(database.dialect() == Database.Dialect.POSTGRES);
    var notified = new AtomicInteger();
    var source =
        (DataSource)
            Proxy.newProxyInstance(
                DataSource.class.getClassLoader(),
                new Class<?>[] {DataSource.class},
                (proxy, method, args) -> {
                  if (!method.getName().equals("getConnection"))
                    throw new UnsupportedOperationException();
                  var connection = database.connection();
                  return Proxy.newProxyInstance(
                      Connection.class.getClassLoader(),
                      new Class<?>[] {Connection.class},
                      (ignored, operation, arguments) -> {
                        if (operation.getName().equals("prepareStatement")) {
                          var sql = (String) arguments[0];
                          if (sql.equals(Sql.query(database, "database_version")))
                            return connection.prepareStatement(
                                "SELECT 'PostgreSQL 15.12-YB-2025.2.3.0', " + notificationsEnabled);
                          if (sql.contains("xmax"))
                            throw new SQLException("YugabyteDB has no xmax column");
                          if (sql.contains("pg_notify")) notified.incrementAndGet();
                        }
                        try {
                          return operation.invoke(connection, arguments);
                        } catch (InvocationTargetException error) {
                          throw error.getCause();
                        }
                      });
                });
    var client = new Client(new Database(source, database.dialect()).withSchema(database.schema()));
    var plain = client.insert(TYPE, new Args("plain"));
    assertFalse(plain.uniqueSkippedAsDuplicate());
    assertTrue(plain.job().metadata().hasNonNull(Protocol.METADATA_UNIQUE_NONCE));

    var options = InsertOptions.builder().unique(Unique.args()).build();
    var original = client.insert(TYPE, new Args("unique"), options);
    var duplicate = client.insert(TYPE, new Args("unique"), options);
    assertFalse(original.uniqueSkippedAsDuplicate());
    assertTrue(duplicate.uniqueSkippedAsDuplicate());
    assertEquals(original.job(), duplicate.job());

    var batch = client.insertMany(TYPE, List.of(new Args("one"), new Args("two")), options);
    var repeated = client.insertMany(TYPE, List.of(new Args("one"), new Args("two")), options);
    assertTrue(batch.stream().noneMatch(Job.InsertResult::uniqueSkippedAsDuplicate));
    assertTrue(repeated.stream().allMatch(Job.InsertResult::uniqueSkippedAsDuplicate));
    assertEquals(
        batch.stream().map(Job.InsertResult::job).toList(),
        repeated.stream().map(Job.InsertResult::job).toList());
    assertEquals(notificationsEnabled ? 4 : 0, notified.get());
  }

  private record Args(String value) {}
}
