package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.node.ObjectNode;

class JobUpdateOptionsTest {
  @Test
  void buildersSnapshotInputsAndOptionsCopyTheirAccessors() {
    var metadata = Json.object().set("nested", Json.object().put("saved", true));
    var output = Json.object().put("saved", true);
    var builder = JobUpdateOptions.builder().metadata(metadata).output(output);
    ((ObjectNode) metadata.path("nested")).put("saved", false);
    output.put("saved", false);
    var options = builder.build();
    builder.metadata(Map.of("later", true)).output("later");
    ((ObjectNode) options.metadata().path("nested")).put("saved", false);
    ((ObjectNode) options.output()).put("saved", false);

    assertEquals(Json.parse("{\"nested\":{\"saved\":true}}"), options.metadata());
    assertEquals(Json.parse("{\"saved\":true}"), options.output());
  }

  @Test
  void constructorCopiesInputs() {
    var metadata = Json.object().put("saved", true);
    var output = Json.object().put("saved", true);
    var options = new JobUpdateOptions(metadata, output);
    metadata.put("saved", false);
    output.put("saved", false);

    assertEquals(Json.parse("{\"saved\":true}"), options.metadata());
    assertEquals(Json.parse("{\"saved\":true}"), options.output());
  }

  @Test
  void defaultsDistinguishOmittedOutputFromJsonNull() {
    assertEquals(Json.object(), JobUpdateOptions.defaults().metadata());
    assertNull(JobUpdateOptions.defaults().output());
    assertTrue(JobUpdateOptions.builder().output(null).build().output().isNull());
    assertEquals(Json.parse("false"), JobUpdateOptions.builder().output(false).build().output());
  }

  @Test
  void metadataRequiresAnObject() {
    for (var metadata : Json.parse("[null,[],true,42,\"text\"]")) {
      assertThrows(IllegalArgumentException.class, () -> new JobUpdateOptions(metadata, null));
      assertThrows(
          IllegalArgumentException.class,
          () -> JobUpdateOptions.builder().metadata(metadata).build());
    }
    assertThrows(
        IllegalArgumentException.class, () -> JobUpdateOptions.builder().metadata(null).build());
  }
}
