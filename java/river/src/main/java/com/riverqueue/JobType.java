package com.riverqueue;

import java.util.List;
import java.util.Objects;

/** An explicit stable wire kind and Java argument type, usually a record. */
public record JobType<A>(
    String kind, Class<A> argsType, InsertOptions defaults, List<List<String>> uniqueFields) {
  public JobType {
    Objects.requireNonNull(kind);
    Objects.requireNonNull(argsType);
    Objects.requireNonNull(defaults);
    Objects.requireNonNull(uniqueFields);
    uniqueFields = uniqueFields.stream().map(List::copyOf).toList();
    // Match the kind_length constraint enforced by both shared Go database schemas.
    if (kind.length() > 127 || !Protocol.USER_SPECIFIED_ID_OR_KIND.matcher(kind).matches())
      throw new IllegalArgumentException("Invalid job kind: " + kind);
  }

  public static <A> JobType<A> of(String kind, Class<A> argsType) {
    return new JobType<>(kind, argsType, InsertOptions.defaults(), List.of());
  }

  /** Describes one job for a mixed-kind bulk insertion. */
  public Client.Submission<A> submission(A args) {
    return submission(args, InsertOptions.defaults());
  }

  /** Describes one job and its insertion overrides for a bulk insertion. */
  public Client.Submission<A> submission(A args, InsertOptions options) {
    return new Client.Submission<>(this, args, options);
  }

  public JobType<A> withDefaults(InsertOptions options) {
    return new JobType<>(kind, argsType, options, uniqueFields);
  }

  /** Selects nested JSON paths for argument uniqueness, with each path expressed as components. */
  public JobType<A> uniqueBy(List<List<String>> fields) {
    return new JobType<>(kind, argsType, defaults, fields);
  }

  /** Selects top-level JSON fields for argument uniqueness; dots are literal field characters. */
  public JobType<A> uniqueBy(String... fields) {
    return uniqueBy(java.util.Arrays.stream(fields).map(List::of).toList());
  }
}
