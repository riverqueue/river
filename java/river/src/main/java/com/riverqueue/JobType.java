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
    if (!kind.matches("[a-zA-Z0-9_][a-zA-Z0-9_\\-\\[\\]<>/.·:+]{1,126}"))
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

  public JobType<A> uniqueBy(List<List<String>> fields) {
    return new JobType<>(kind, argsType, defaults, fields);
  }
}
