package com.riverqueue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import tools.jackson.databind.JsonNode;

final class Conformance {
  private Conformance() {}

  static JsonNode fixture(String name) throws IOException {
    return Json.parse(read(name));
  }

  static String read(String name) throws IOException {
    // Resolve from the checkout, including when tests run directly from an IDE.
    var root = Path.of("").toAbsolutePath();
    while (root != null && !Files.isRegularFile(root.resolve("go.work"))) root = root.getParent();
    if (root == null) throw new IOException("Cannot find the River repository's go.work");
    var path = root.resolve("conformance/testdata").resolve(name);
    try {
      return Files.readString(path);
    } catch (NoSuchFileException e) {
      throw new IOException(
          "Missing conformance fixture "
              + path
              + "; run `make generate/fixtures` from the repository root",
          e);
    }
  }
}
