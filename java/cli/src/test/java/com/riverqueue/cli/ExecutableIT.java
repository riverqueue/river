package com.riverqueue.cli;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ExecutableIT {
  @TempDir Path directory;

  private String command(String... args) throws Exception {
    var command =
        new ArrayList<>(
            List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-jar",
                "target/river-cli-0.48.0-alpha.1-all.jar"));
    command.addAll(List.of(args));
    var output = directory.resolve("process.log").toFile();
    var builder = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(output);
    builder.environment().put("DATABASE_URL", "jdbc:sqlite:" + directory.resolve("river.db"));
    var process = builder.start();
    try {
      assertTrue(process.waitFor(30, TimeUnit.SECONDS), "CLI timed out");
      String text = java.nio.file.Files.readString(output.toPath());
      assertEquals(0, process.exitValue(), text);
      return text;
    } finally {
      process.destroyForcibly();
    }
  }

  @Test
  void bundledJarRunsWithoutExternalClasspath() throws Exception {
    assertTrue(command("--version").contains("0.48.0-alpha.1"));
    assertTrue(command("migrate-up").contains("008 [up]"));
    assertTrue(command("migrate-list").contains("applied"));
    assertTrue(command("migrate-down", "--target-version", "0").contains("001 [down]"));
    assertFalse(command("migrate-list").contains("applied"));
  }
}
