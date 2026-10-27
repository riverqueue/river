package com.riverqueue.cli;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ExecutableIT {
  @TempDir Path directory;

  private String command(boolean executableJar, String... args) throws Exception {
    var command =
        new ArrayList<>(
            List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--enable-native-access=ALL-UNNAMED"));
    var jar = Path.of("target/river-cli.jar").toAbsolutePath().toString();
    if (executableJar) {
      command.addAll(List.of("-jar", jar));
    } else {
      command.addAll(
          List.of(
              "-cp",
              jar + File.pathSeparator + Path.of("target/dependency/*").toAbsolutePath(),
              "com.riverqueue.cli.MigrationCli"));
    }
    command.addAll(List.of(args));
    var output = directory.resolve("process.log").toFile();
    var builder =
        new ProcessBuilder(command)
            .directory(directory.toFile())
            .redirectErrorStream(true)
            .redirectOutput(output);
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

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void thinJarRunsWithRuntimeDependencies(boolean executableJar) throws Exception {
    assertEquals("river 0.2.0", command(executableJar, "--version").strip());
    assertTrue(command(executableJar, "migrate-up").contains("008 [up]"));
    assertTrue(command(executableJar, "migrate-list").contains("applied"));
    assertTrue(
        command(executableJar, "migrate-down", "--target-version", "0").contains("001 [down]"));
    assertFalse(command(executableJar, "migrate-list").contains("applied"));
  }
}
