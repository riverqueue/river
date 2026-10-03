package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tools.jackson.databind.node.ObjectNode;

class JobQueryTest {
  @TempDir Path directory;
  @RegisterExtension final TestDatabase databases = new TestDatabase();

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "{\"value\":true}|{\"value\":1}|false",
        "{\"value\":false}|{\"value\":0}|false",
        "{\"value\":1}|{\"value\":true}|false",
        "{\"value\":1}|{\"value\":1.0}|true",
        "{\"value\":\"1\"}|{\"value\":1}|false",
        "{\"value\":\"text\"}|{\"value\":{\"key\":1}}|false",
        "{\"value\":\"text\"}|{\"value\":[1]}|false",
        "{\"value\":[{\"a\":1},{\"b\":2}]}|{\"value\":[{\"b\":2},{\"a\":1}]}|true",
        "{\"value\":[1,2]}|{\"value\":[2,1]}|true",
        "{\"value\":[1]}|{\"value\":[1,1]}|true",
        "{\"value\":[[1]]}|{\"value\":[1]}|false",
        "{\"value\":[1]}|{\"value\":1}|false",
        "{\"value\":[{\"a\":1,\"b\":2}]}|{\"value\":[{\"b\":2}]}|true",
        "{\"value\":[{\"a\":1},{\"b\":2}]}|{\"value\":[{\"a\":1,\"b\":2}]}|false",
        "{\"a.b\":{\"c d\":null}}|{\"a.b\":{\"c d\":null}}|true",
        "{\"value\":1}|[]|false",
        "{\"value\":1}|1|false",
        "{\"value\":1}|true|false",
        "{\"value\":1}|null|false"
      })
  void metadataContainmentPreservesJsonSemantics(String stored, String fragment, boolean matches) {
    var client = new Client(databases.open(directory.resolve("river.db")));
    var inserted =
        client
            .insert(
                JobType.of("metadata_filter", String.class),
                "test",
                InsertOptions.builder().metadata(Json.parse(stored)).build())
            .job();
    var query = JobQuery.builder().metadata(Json.parse(fragment)).build();
    var expected = matches ? List.of(inserted.id()) : List.<Long>of();
    assertEquals(expected, client.list(query).jobs().stream().map(Job::id).toList());
    assertEquals(expected, client.deleteMany(query, false).stream().map(Job::id).toList());
    assertEquals(matches ? 0 : 1, client.list(JobQuery.all()).jobs().size());
  }

  @ParameterizedTest
  @CsvSource({"object,false", "array,false", "object,true", "array,true"})
  void metadataFiltersRequireEmptyContainers(String container, boolean nested) {
    var client = new Client(databases.open(directory.resolve("river.db")));
    var type = JobType.of("metadata_filter", String.class);
    String empty = container.equals("object") ? "{}" : "[]";
    String populated = container.equals("object") ? "{\"id\":1}" : "[1]";
    String wrongType = container.equals("object") ? "[]" : "{}";
    var expected = new ArrayList<Long>();
    var remaining = new ArrayList<Long>();
    for (String value :
        List.of(empty, populated, wrongType, "null", "1", "true", "\"text\"", "missing")) {
      var metadata = Json.object().put("plan", "pro");
      if (!value.equals("missing"))
        metadata.set(
            "tenant",
            nested ? Json.object().set("settings", Json.parse(value)) : Json.parse(value));
      long id =
          client.insert(type, value, InsertOptions.builder().metadata(metadata).build()).job().id();
      (value.equals(empty) || value.equals(populated) ? expected : remaining).add(id);
    }
    if (nested) {
      var metadata = Json.object().put("plan", "pro").set("tenant", Json.object());
      remaining.add(
          client
              .insert(
                  type, "missing nested key", InsertOptions.builder().metadata(metadata).build())
              .job()
              .id());
    }
    var filter = Json.object().put("plan", "pro");
    filter.set(
        "tenant", nested ? Json.object().set("settings", Json.parse(empty)) : Json.parse(empty));
    var query = JobQuery.builder().metadata(filter).build();
    assertEquals(expected, client.list(query).jobs().stream().map(Job::id).toList());
    assertEquals(expected, client.deleteMany(query, false).stream().map(Job::id).toList());
    assertEquals(remaining, client.list(JobQuery.all()).jobs().stream().map(Job::id).toList());
  }

  @Test
  void metadataFilterIsImmutable() {
    var source = Json.object().set("tenant", Json.object().put("id", "original"));
    var query = JobQuery.builder().metadata(source).build();
    ((ObjectNode) source.path("tenant")).put("id", "changed source");
    ((ObjectNode) query.metadata().path("tenant")).put("id", "changed accessor");
    assertEquals("original", query.metadata().path("tenant").path("id").asString());
  }
}
