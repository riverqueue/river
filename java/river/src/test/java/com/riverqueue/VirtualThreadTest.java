package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class VirtualThreadTest {
  @TempDir Path directory;

  @Test
  void blockingClaimDoesNotStarveOtherVirtualThreads() throws Exception {
    probe("claim");
  }

  @Test
  void blockingLeadershipDoesNotStarveOtherVirtualThreads() throws Exception {
    probe("leadership");
  }

  private void probe(String scenario) throws Exception {
    // One carrier makes monitor pinning deterministic, independent of the host's CPU count.
    var output = directory.resolve("probe.log");
    var process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--enable-native-access=ALL-UNNAMED",
                "-Djdk.virtualThreadScheduler.parallelism=1",
                "-Djdk.virtualThreadScheduler.maxPoolSize=1",
                "-cp",
                System.getProperty(
                    "surefire.test.class.path", System.getProperty("java.class.path")),
                Probe.class.getName(),
                scenario,
                directory.resolve("river.db").toString())
            .redirectErrorStream(true)
            .redirectOutput(output.toFile())
            .start();
    try {
      assertTrue(process.waitFor(30, TimeUnit.SECONDS), () -> "Probe timed out: " + scenario);
      assertEquals(
          0,
          process.exitValue(),
          () -> {
            try {
              return Files.readString(output);
            } catch (java.io.IOException error) {
              throw new java.io.UncheckedIOException(error);
            }
          });
    } finally {
      process.destroyForcibly();
    }
  }

  public static final class Probe {
    public static void main(String[] args) throws Exception {
      var entered = new CountDownLatch(1);
      var release = new CountDownLatch(1);
      var database = Database.connect("jdbc:sqlite:" + args[1]);
      new Migrator(database).migrate();
      var client =
          new Client(database)
              .withPlugin(
                  new Plugin() {
                    @Override
                    public List<Client.Decoded> claim(
                        Connection connection,
                        Client.Driver driver,
                        Claim claim,
                        Client.Transaction<List<Client.Decoded>> next)
                        throws Exception {
                      if (args[0].equals("claim")) {
                        entered.countDown();
                        release.await();
                      }
                      return next.run(connection);
                    }
                  })
              .withExtension(
                  new Extension() {
                    @Override
                    public void periodicStarted() {
                      if (!args[0].equals("leadership")) return;
                      entered.countDown();
                      try {
                        release.await();
                      } catch (InterruptedException error) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(error);
                      }
                    }
                  });
      try (var workers =
          client
              .workers()
              .queue("default", 1)
              .pollOnly(true)
              .leadership(args[0].equals("leadership"))
              .stopTimeout(Duration.ofSeconds(5))
              .add(JobType.of("noop", String.class), context -> {})
              .start()) {
        try {
          if (!entered.await(10, TimeUnit.SECONDS))
            throw new IllegalStateException("Probe did not enter " + args[0]);
          var unblock = Thread.startVirtualThread(release::countDown);
          if (!unblock.join(Duration.ofSeconds(5)))
            throw new IllegalStateException(
                "Blocked " + args[0] + " pinned the only carrier thread");
        } finally {
          release.countDown();
        }
      }
    }
  }
}
