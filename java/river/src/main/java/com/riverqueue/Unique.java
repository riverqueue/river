package com.riverqueue;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** River's uniqueness dimensions and byte-compatible SHA-256 protocol. */
public record Unique(
    boolean byArgs,
    Duration byPeriod,
    boolean byQueue,
    Set<Job.State> byState,
    boolean excludeKind) {
  public Unique {
    byState = byState == null ? null : Set.copyOf(byState);
    if (byPeriod != null && byPeriod.compareTo(Duration.ofSeconds(1)) < 0)
      throw new IllegalArgumentException("Unique period must be at least one second");
    if (excludeKind && !byArgs && !byQueue && byPeriod == null)
      throw new IllegalArgumentException("Excluding kind requires another uniqueness dimension");
    if (byState != null
        && !byState.isEmpty()
        && !byState.containsAll(
            Set.of(Job.State.AVAILABLE, Job.State.PENDING, Job.State.RUNNING, Job.State.SCHEDULED)))
      throw new IllegalArgumentException(
          "Unique states must include available, pending, running, scheduled");
  }

  public static Unique args() {
    return new Unique(true, null, false, null, false);
  }

  public static Unique none() {
    return new Unique(false, null, false, null, false);
  }

  public Unique per(Duration value) {
    return new Unique(byArgs, value, byQueue, byState, excludeKind);
  }

  public Unique perQueue() {
    return new Unique(byArgs, byPeriod, true, byState, excludeKind);
  }

  /**
   * Overrides the states in which jobs remain unique, subject to River's required active states.
   */
  public Unique states(Job.State... values) {
    return new Unique(byArgs, byPeriod, byQueue, Set.of(values), excludeKind);
  }

  /** Shares uniqueness across kinds; another uniqueness dimension must be enabled. */
  public Unique excludeKind(boolean value) {
    return new Unique(byArgs, byPeriod, byQueue, byState, value);
  }

  public boolean enabled() {
    return byArgs || byPeriod != null || byQueue || byState != null || excludeKind;
  }

  public int stateMask() {
    return byState == null || byState.isEmpty()
        ? 245
        : byState.stream().mapToInt(Job.State::bit).reduce(0, (a, b) -> a | b);
  }

  public List<Job.State> states() {
    return java.util.Arrays.stream(Job.State.values())
        .filter(s -> (stateMask() & s.bit()) != 0)
        .toList();
  }

  public String key(
      String kind,
      String encodedArgs,
      List<List<String>> fields,
      Instant now,
      String queue,
      Instant scheduledAt) {
    if (!enabled()) return null;
    var input = new StringBuilder();
    if (!excludeKind) input.append("&kind=").append(kind);
    if (byArgs) input.append("&args=").append(arguments(encodedArgs, fields));
    if (byPeriod != null) {
      Instant time = scheduledAt == null ? now : scheduledAt;
      var nanos =
          BigInteger.valueOf(time.getEpochSecond())
              .add(BigInteger.valueOf(62135596800L))
              .multiply(BigInteger.valueOf(1000000000))
              .add(BigInteger.valueOf(time.getNano()));
      var period =
          BigInteger.valueOf(byPeriod.getSeconds())
              .multiply(BigInteger.valueOf(1000000000))
              .add(BigInteger.valueOf(byPeriod.getNano()));
      var truncated = nanos.subtract(nanos.mod(period));
      var seconds = truncated.divideAndRemainder(BigInteger.valueOf(1000000000));
      input
          .append("&period=")
          .append(
              Instant.ofEpochSecond(
                      seconds[0].longValueExact() - 62135596800L, seconds[1].longValue())
                  .truncatedTo(ChronoUnit.SECONDS));
    }
    if (byQueue) input.append("&queue=").append(queue);
    try {
      return HexFormat.of()
          .formatHex(
              MessageDigest.getInstance("SHA-256")
                  .digest(input.toString().getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }

  private static String arguments(String source, List<List<String>> paths) {
    if (paths.isEmpty() && source.matches("\\s*\\[\\s*]\\s*")) return "{}";
    Map<String, String> members = Json.members(source);
    var selected = new LinkedHashMap<String, Object>();
    if (paths.isEmpty()) {
      members.keySet().stream()
          .sorted(Json.UTF8_ORDER)
          .forEach(k -> selected.put(k, members.get(k)));
    } else {
      var sorted =
          paths.stream()
              .distinct()
              .sorted((a, b) -> Json.UTF8_ORDER.compare(String.join(".", a), String.join(".", b)))
              .toList();
      for (var path : sorted) {
        if (path.isEmpty() || path.stream().anyMatch(p -> p.isEmpty() || p.matches("[0-9]+|-1")))
          throw new IllegalArgumentException("Invalid unique field path");
        for (var other : sorted)
          if (other.size() > path.size() && other.subList(0, path.size()).equals(path))
            throw new IllegalArgumentException("Overlapping unique field paths");
        String current = source;
        for (String part : path) {
          if (current == null || !current.stripLeading().startsWith("{")) {
            current = null;
            break;
          }
          current = Json.members(current).get(part);
        }
        if (current != null) select(selected, path, current);
      }
      if (selected.isEmpty()) return "";
    }
    return write(selected);
  }

  @SuppressWarnings("unchecked")
  private static void select(Map<String, Object> into, List<String> path, String value) {
    if (path.size() == 1) into.put(path.getFirst(), value);
    else
      select(
          (Map<String, Object>)
              into.computeIfAbsent(path.getFirst(), _ -> new LinkedHashMap<String, Object>()),
          path.subList(1, path.size()),
          value);
  }

  private static String write(Map<String, ?> values) {
    var result = new ArrayList<String>();
    for (var entry : values.entrySet()) {
      String encoded;
      if (entry.getValue() instanceof Map<?, ?> map) {
        var nested = new LinkedHashMap<String, Object>();
        map.forEach((k, v) -> nested.put((String) k, v));
        encoded = write(nested);
      } else encoded = (String) entry.getValue();
      result.add(Json.sjsonKey(entry.getKey()) + ":" + encoded);
    }
    return "{" + String.join(",", result) + "}";
  }
}
