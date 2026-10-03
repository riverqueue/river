package com.riverqueue;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonToken;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.module.SimpleModule;
import tools.jackson.databind.node.ObjectNode;

/** Shared JSON encoding. Raw argument values are preserved when calculating unique keys. */
public final class Json {
  public static final JsonMapper MAPPER =
      JsonMapper.builder()
          .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
          .enable(tools.jackson.databind.DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
          .addModule(
              new SimpleModule()
                  .addSerializer(
                      Instant.class,
                      new ValueSerializer<Instant>() {
                        @Override
                        public void serialize(
                            Instant value, JsonGenerator generator, SerializationContext context) {
                          generator.writeString(
                              new DateTimeFormatterBuilder()
                                  .appendInstant(-1)
                                  .toFormatter()
                                  .format(value));
                        }
                      }))
          .disable(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS)
          .build();
  public static final Comparator<String> UTF8_ORDER =
      (a, b) ->
          java.util.Arrays.compareUnsigned(
              a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));

  private Json() {}

  public static <T> T decode(JsonNode value, Class<T> type) {
    return MAPPER.treeToValue(value, type);
  }

  public static String encode(Object value) {
    return goEscapes(MAPPER.writeValueAsString(value));
  }

  public static ObjectNode object() {
    return MAPPER.createObjectNode();
  }

  public static JsonNode parse(String value) {
    return MAPPER.readTree(value);
  }

  public static JsonNode tree(Object value) {
    return MAPPER.valueToTree(value);
  }

  public static String goEscapes(String value) {
    return value
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("&", "\\u0026")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029");
  }

  public static Map<String, String> members(String source) {
    var result = new LinkedHashMap<String, String>();
    try (var parser = MAPPER.createParser(source)) {
      if (parser.nextToken() != JsonToken.START_OBJECT)
        throw new IllegalArgumentException("Unique args must encode a JSON object");
      while (parser.nextToken() != JsonToken.END_OBJECT) {
        String key = parser.currentName();
        parser.nextToken();
        int start = (int) parser.currentTokenLocation().getCharOffset();
        parser.skipChildren();
        if (parser.currentToken() == JsonToken.VALUE_STRING) parser.getString();
        int end = (int) parser.currentLocation().getCharOffset();
        result.putIfAbsent(key, source.substring(start, end));
      }
    }
    return result;
  }

  public static String compact(String source) {
    var result = new StringBuilder();
    boolean quoted = false;
    boolean escaped = false;
    for (int i = 0; i < source.length(); i++) {
      char c = source.charAt(i);
      if (quoted || !Character.isWhitespace(c)) result.append(c);
      if (escaped) escaped = false;
      else if (quoted && c == '\\') escaped = true;
      else if (c == '"') quoted = !quoted;
    }
    return result.toString();
  }

  static String sjsonKey(String value) {
    return value.chars().allMatch(c -> c >= 32 && c <= 127 && c != '"' && c != '\\')
        ? '"' + value + '"'
        : encode(value);
  }
}
