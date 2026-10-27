package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class HookMiddlewareTest {
  private static final JobType<String> TYPE = JobType.of("hook_test", String.class);
  @RegisterExtension final TestDatabase databases = new TestDatabase();
  @TempDir Path directory;

  private Client client() {
    return new Client(databases.open(directory.resolve("river.db")));
  }

  @ParameterizedTest
  @ValueSource(strings = {"single", "bulk", "mixed", "reinsert"})
  void insertHooksAndMiddlewareRunInOrder(String operation) {
    var base = client();
    var trace = new ArrayList<String>();
    var connectionSeen = new AtomicReference<Connection>();
    var submissionsSeen = new AtomicReference<List<Client.Submission<?>>>();
    var resultsSeen = new AtomicReference<List<Job.InsertResult>>();
    class Instrumentation implements HookInsertBegin, JobInsertMiddleware {
      private final String name;

      Instrumentation(String name) {
        this.name = name;
      }

      @Override
      public void insertBegin(
          Connection connection, JobType<?> type, Object args, InsertOptions options) {
        assertSame(connectionSeen.get(), connection);
        trace.add(name + ":hook:" + Json.tree(args).asString());
      }

      @Override
      public List<Job.InsertResult> insertMany(
          Connection connection,
          List<Client.Submission<?>> submissions,
          Callable<List<Job.InsertResult>> next)
          throws Exception {
        if (connectionSeen.compareAndSet(null, connection)) {
          submissionsSeen.set(submissions);
          assertThrows(UnsupportedOperationException.class, submissions::clear);
        } else {
          assertSame(connectionSeen.get(), connection);
          assertSame(submissionsSeen.get(), submissions);
        }
        trace.add(name + ":before");
        var result = next.call();
        resultsSeen.set(result);
        trace.add(name + ":after");
        return result;
      }
    }
    var first = new Instrumentation("first");
    var second = new Instrumentation("second");
    var river =
        base.withHooks(first)
            .withMiddleware(first)
            .withHooks(second)
            .withMiddleware(second)
            .withPilot(new Pilot() {});
    List<Job.InsertResult> results =
        switch (operation) {
          case "single" -> List.of(river.insert(TYPE, "one"));
          case "bulk" -> river.insertMany(TYPE, List.of("one", "two"));
          case "mixed" ->
              river.insertMany(
                  List.of(
                      TYPE.submission("one"),
                      JobType.of("other_kind", String.class).submission("two")));
          case "reinsert" -> {
            var original = base.insert(TYPE, "one").job();
            assertTrue(trace.isEmpty(), "Registration must leave the original client unchanged");
            var reinserted =
                river.transaction(connection -> river.driver().reinsert(connection, original));
            yield List.of(reinserted);
          }
          default -> throw new IllegalArgumentException(operation);
        };
    var expected =
        new ArrayList<>(
            List.of("first:before", "second:before", "first:hook:one", "second:hook:one"));
    if (results.size() == 2) expected.addAll(List.of("first:hook:two", "second:hook:two"));
    expected.addAll(List.of("second:after", "first:after"));
    assertEquals(expected, trace);
    assertEquals(results, resultsSeen.get());
    assertEquals(
        results.stream().map(result -> result.job().kind()).toList(),
        submissionsSeen.get().stream().map(submission -> submission.type().kind()).toList());
    assertEquals(
        results.stream().map(result -> result.job().args().asString()).toList(),
        submissionsSeen.get().stream()
            .map(submission -> Json.tree(submission.args()).asString())
            .toList());
  }

  @ParameterizedTest
  @CsvSource({
    "single,false",
    "single,true",
    "bulk,false",
    "bulk,true",
    "mixed,false",
    "mixed,true"
  })
  void insertMiddlewareFailureRollsBackWrites(String operation, boolean callerTransaction)
      throws Exception {
    var base = client();
    var failure = new IOException("after insertion");
    var river =
        base.withMiddleware(
            (JobInsertMiddleware)
                (connection, submissions, next) -> {
                  next.call();
                  base.insert(connection, TYPE, "middleware write");
                  throw failure;
                });
    try (var connection = base.database().connection()) {
      connection.setAutoCommit(false);
      if (callerTransaction) base.insert(connection, TYPE, "keep");
      var error =
          assertThrows(
              RiverException.class,
              () -> {
                switch (operation) {
                  case "single" -> {
                    if (callerTransaction) river.insert(connection, TYPE, "reject");
                    else river.insert(TYPE, "reject");
                  }
                  case "bulk" -> {
                    if (callerTransaction)
                      river.insertMany(connection, TYPE, List.of("reject", "also reject"));
                    else river.insertMany(TYPE, List.of("reject", "also reject"));
                  }
                  case "mixed" -> {
                    var submissions =
                        List.of(TYPE.submission("reject"), TYPE.submission("also reject"));
                    if (callerTransaction) river.insertMany(connection, submissions);
                    else river.insertMany(submissions);
                  }
                  default -> throw new IllegalArgumentException(operation);
                }
              });
      assertSame(failure, error.getCause());
      connection.commit();
    }
    assertEquals(
        callerTransaction ? List.of("keep") : List.of(),
        base.list(JobQuery.all()).jobs().stream().map(job -> job.args().asString()).toList());
  }

  @Test
  void registrationRejectsNullEntries() {
    var river = client();
    assertThrows(NullPointerException.class, () -> river.withHooks((Hook) null));
    assertThrows(NullPointerException.class, () -> river.withMiddleware((Middleware) null));
  }

  @Test
  @SuppressWarnings("try") // Resources intentionally scope background activity.
  void workBeginFailureSkipsHandlerAndEndHooks() throws Exception {
    var trace = new ArrayList<String>();
    var river =
        client()
            .withHooks(
                (HookWorkBegin)
                    context -> {
                      trace.add("begin");
                      throw new IOException("begin failed");
                    },
                (HookWorkEnd)
                    (context, failure) -> {
                      trace.add("end");
                      return failure;
                    })
            .withMiddleware(
                (WorkerMiddleware)
                    (context, next) -> {
                      trace.add("before");
                      try {
                        next.run();
                      } finally {
                        trace.add("after");
                      }
                    });
    var completed = new CompletableFuture<Client.Event>();
    try (var workers =
            workers(river).worker(TYPE, context -> trace.add("handler")).build().start();
        var subscription = workers.client().subscribe(completed::complete)) {
      river.insert(TYPE, "one", InsertOptions.builder().maxAttempts(1).build());
      assertEquals(Job.State.DISCARDED, completed.get(5, TimeUnit.SECONDS).job().state());
    }
    assertEquals(List.of("before", "begin", "after"), trace);
  }

  @ParameterizedTest
  @CsvSource({
    "success,preserve",
    "failure,preserve",
    "error,preserve",
    "cancel,preserve",
    "failure,clear",
    "error,clear",
    "success,replace",
    "failure,replace",
    "success,throw"
  })
  @SuppressWarnings("try") // Resources intentionally scope background activity.
  void workEndHooksChainOutcomesInsideMiddleware(String outcome, String action) throws Exception {
    var trace = new ArrayList<String>();
    var failure = new IOException("handler failed");
    var fatal = new AssertionError("handler error");
    var replacement = new IOException("replacement");
    var observed = new AtomicReference<Throwable>();
    var river =
        client()
            .withHooks(
                (HookWorkBegin) context -> trace.add("begin"),
                (HookWorkEnd)
                    (context, error) -> {
                      trace.add("end:first");
                      switch (outcome) {
                        case "success" -> assertNull(error);
                        case "failure" -> assertSame(failure, error);
                        case "error" -> assertSame(fatal, error);
                        case "cancel" -> assertInstanceOf(WorkContext.Control.class, error);
                        default -> throw new IllegalArgumentException(outcome);
                      }
                      return switch (action) {
                        case "clear" -> null;
                        case "replace" -> replacement;
                        case "throw" -> throw replacement;
                        default -> error;
                      };
                    },
                (HookWorkEnd)
                    (context, error) -> {
                      trace.add("end:second");
                      observed.set(error);
                      return error;
                    });
    for (var name : List.of("first", "second")) {
      river =
          river.withMiddleware(
              (WorkerMiddleware)
                  (context, next) -> {
                    trace.add(name + ":before");
                    try {
                      next.run();
                    } finally {
                      trace.add(name + ":after");
                    }
                  });
    }
    var completed = new CompletableFuture<Client.Event>();
    try (var workers =
            workers(river)
                .worker(
                    TYPE,
                    context -> {
                      trace.add("handler");
                      switch (outcome) {
                        case "failure" -> throw failure;
                        case "error" -> throw fatal;
                        case "cancel" -> context.cancel("cancelled");
                        default -> {}
                      }
                    })
                .build()
                .start();
        var subscription = workers.client().subscribe(completed::complete)) {
      river.insert(TYPE, "one", InsertOptions.builder().maxAttempts(1).build());
      var expected =
          action.equals("clear") || (outcome.equals("success") && action.equals("preserve"))
              ? Job.State.COMPLETED
              : outcome.equals("cancel") ? Job.State.CANCELLED : Job.State.DISCARDED;
      assertEquals(expected, completed.get(5, TimeUnit.SECONDS).job().state());
    }
    var expected =
        new ArrayList<>(List.of("first:before", "second:before", "begin", "handler", "end:first"));
    if (!action.equals("throw")) expected.add("end:second");
    expected.addAll(List.of("second:after", "first:after"));
    assertEquals(expected, trace);
    if (action.equals("replace")) assertSame(replacement, observed.get());
    if (action.equals("clear")) assertNull(observed.get());
  }

  private Client.Builder workers(Client river) {
    return river.toBuilder()
        .queue("default", 1)
        .leadership(false)
        .pollOnly(true)
        .pollInterval(Duration.ofMillis(10))
        .stopTimeout(Duration.ofSeconds(5));
  }
}
