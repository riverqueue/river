package com.riverqueue;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Objects;
import tools.jackson.databind.node.ObjectNode;

/**
 * A recurring job definition shared by builder configuration and runtime registration. The ID may
 * be null for an anonymous registration; named IDs must be unique within a runtime. Arguments are
 * reused for each occurrence. RunOnStart also applies when adding a job while leading.
 */
public record PeriodicJob<A>(
    String id,
    Schedule schedule,
    JobType<A> type,
    A args,
    InsertOptions options,
    boolean runOnStart) {
  public PeriodicJob {
    if (id != null
        && (id.getBytes(StandardCharsets.UTF_8).length > 127
            || !Protocol.USER_SPECIFIED_ID_OR_KIND.matcher(id).matches()))
      throw new IllegalArgumentException("Invalid periodic job ID");
    Objects.requireNonNull(schedule, "schedule");
    Objects.requireNonNull(type, "type");
    Objects.requireNonNull(args, "args");
    Objects.requireNonNull(options, "options");
  }

  void insert(Client river, Instant scheduledAt) {
    var base = options.resolve(type.defaults());
    var metadata = (ObjectNode) base.metadata().deepCopy();
    metadata.put("periodic", true);
    if (id != null) metadata.put(Protocol.METADATA_PERIODIC_JOB_ID, id);
    var options =
        new InsertOptions(
            base.maxAttempts(),
            metadata,
            base.pending(),
            base.priority(),
            base.queue(),
            base.scheduledAt(),
            base.tags(),
            base.unique());
    // A periodic occurrence supplies the default date without turning an available job into
    // a scheduled one or overriding an explicitly configured date, matching Go's enqueuer.
    river.transaction(c -> river.insert(c, type, args, options, scheduledAt));
  }
}
