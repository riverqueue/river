package com.riverqueue;

import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/** Stored and transmitted values shared with the other River implementations. */
final class Protocol {
  static final String METADATA_OUTPUT = "output";
  static final String METADATA_PERIODIC_JOB_ID = "river:periodic_job_id";
  static final String METADATA_RESCUE_COUNT = "river:rescue_count";
  static final String METADATA_RESUMABLE_CURSOR = "river:resumable_cursor";
  static final String METADATA_RESUMABLE_STEP = "river:resumable_step";
  static final String METADATA_UNIQUE_NONCE = "river:unique_nonce";

  static final String TOPIC_CONTROL = "river_control";
  static final String TOPIC_INSERT = "river_insert";
  static final String TOPIC_LEADERSHIP = "river_leadership";

  static final Pattern USER_SPECIFIED_ID_OR_KIND =
      Pattern.compile("[a-zA-Z0-9_][a-zA-Z0-9_\\-\\[\\]<>/.·:+]+");

  private Protocol() {}

  static Notification cancel(long id, String queue) {
    return new Notification(
        TOPIC_CONTROL, Json.encode(Map.of("action", "cancel", "job_id", id, "queue", queue)));
  }

  static Notification insert(String queue) {
    return new Notification(TOPIC_INSERT, Json.encode(Map.of("queue", queue)));
  }

  static Notification queueMetadataChanged(String queue, Object metadata) {
    return new Notification(
        TOPIC_CONTROL,
        Json.encode(Map.of("action", "metadata_changed", "metadata", metadata, "queue", queue)));
  }

  static Notification queuePause(String queue, boolean pause) {
    return new Notification(
        TOPIC_CONTROL, Json.encode(Map.of("action", pause ? "pause" : "resume", "queue", queue)));
  }

  static Notification requestResign() {
    return new Notification(
        TOPIC_LEADERSHIP, Json.encode(Map.of("action", "request_resign", "leader_id", "")));
  }

  static Notification resigned(String id) {
    return new Notification(
        TOPIC_LEADERSHIP, Json.encode(Map.of("action", "resigned", "leader_id", id)));
  }

  record Notification(String topic, String payload) {}

  /** Shared by both database listeners; dispatching itself requires no database I/O. */
  record Dispatcher(
      String clientId,
      Map<Long, WorkContext<?>> active,
      Map<Long, Long> pendingCancellation,
      Consumer<QueueNotice> queues,
      Consumer<LeadershipNotice> leadership) {
    void dispatch(String topic, String payload) {
      if (!topic.equals(TOPIC_CONTROL)
          && !topic.equals(TOPIC_INSERT)
          && !topic.equals(TOPIC_LEADERSHIP)) return;
      var value = Json.parse(payload);
      String action = value.path("action").asString("");
      switch (topic) {
        case TOPIC_CONTROL -> {
          if (action.equals("cancel") && value.path("job_id").isIntegralNumber()) {
            long jobId = value.path("job_id").asLong();
            // Publish pending cancellation first so an attempt registered concurrently sees it.
            pendingCancellation.put(jobId, System.nanoTime());
            var context = active.get(jobId);
            if (context != null) {
              context.requestCancellation(WorkContext.Cancellation.REMOTE);
              pendingCancellation.remove(jobId);
            }
          } else if ((action.equals("metadata_changed")
                  || action.equals("pause")
                  || action.equals("resume"))
              && value.path("queue").isString()) {
            queues.accept(new QueueNotice(value.path("queue").asString(), action));
          }
        }
        case TOPIC_INSERT -> {
          if (value.path("queue").isString())
            queues.accept(new QueueNotice(value.path("queue").asString(), "insert"));
        }
        case TOPIC_LEADERSHIP -> {
          if (action.equals("request_resign")) leadership.accept(LeadershipNotice.REQUEST_RESIGN);
          else if (action.equals("resigned")
              && !clientId.equals(value.path("leader_id").asString()))
            leadership.accept(LeadershipNotice.CHANGED);
        }
        default -> {}
      }
    }
  }

  enum LeadershipNotice {
    CHANGED,
    REQUEST_RESIGN
  }

  record QueueNotice(String queue, String action) {}
}
