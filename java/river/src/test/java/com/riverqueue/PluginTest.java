package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.JsonNode;

class PluginTest {
  private static final JobType<String> TYPE = JobType.of("plugin_test", String.class);
  @RegisterExtension final TestDatabase databases = new TestDatabase();
  @TempDir Path directory;

  private Client client() {
    return new Client(databases.open(directory.resolve("river.db")));
  }

  @ParameterizedTest
  @ValueSource(strings = {"hooks", "middleware"})
  void explicitRegistrationActivatesOnlyItsOwnInterfaces(String registration) {
    var trace = new CopyOnWriteArrayList<String>();
    var plugin = new RecordingPlugin("explicit", trace);
    var base = client();
    var river = registration.equals("hooks") ? base.withHooks(plugin) : base.withMiddleware(plugin);
    river.insert(TYPE, "one");
    assertEquals(
        registration.equals("hooks")
            ? List.of("explicit:insert_begin")
            : List.of("explicit:insert_before", "explicit:insert_after"),
        trace);
  }

  @Test
  void pluginRegistrationCopiesInputsAndPreservesThePilot() {
    var trace = new CopyOnWriteArrayList<String>();
    var base =
        client()
            .withPilot(
                new Pilot() {
                  @Override
                  public void afterInsert(
                      Connection connection, Client.Driver driver, Job<JsonNode> job) {
                    trace.add("storage");
                  }
                });
    Plugin[] plugins = {(HookInsertBegin) (connection, type, args, options) -> trace.add("first")};
    var first = base.withPlugins(plugins);
    plugins[0] = (HookInsertBegin) (connection, type, args, options) -> fail("Replaced input");
    var second =
        first.withPlugins(
            (HookInsertBegin) (connection, type, args, options) -> trace.add("second"),
            new Plugin() {});
    base.insert(TYPE, "base");
    assertEquals(List.of("storage"), trace);
    trace.clear();
    first.insert(TYPE, "first");
    assertEquals(List.of("first", "storage"), trace);
    trace.clear();
    second.insert(TYPE, "second");
    assertEquals(List.of("first", "second", "storage"), trace);
  }

  @Test
  void pluginRegistrationRetainsDuplicates() {
    var trace = new CopyOnWriteArrayList<String>();
    var plugin = new RecordingPlugin("repeat", trace);
    var river = client().withPlugins(plugin, plugin).withHooks(plugin).withMiddleware(plugin);
    river.insert(TYPE, "one");
    assertEquals(
        List.of(
            "repeat:insert_before",
            "repeat:insert_before",
            "repeat:insert_before",
            "repeat:insert_begin",
            "repeat:insert_begin",
            "repeat:insert_begin",
            "repeat:insert_after",
            "repeat:insert_after",
            "repeat:insert_after"),
        trace);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @SuppressWarnings("try") // Resources intentionally scope background activity.
  void pluginsActivateAllJobInterfacesBeforeExplicitRegistrations(boolean pluginsFirst)
      throws Exception {
    var trace = new CopyOnWriteArrayList<String>();
    var first = new RecordingPlugin("first", trace);
    var second = new RecordingPlugin("second", trace);
    var explicit = new RecordingPlugin("explicit", trace);
    var base = client();
    var river =
        (pluginsFirst
                ? base.withPlugins(first, second).withHooks(explicit).withMiddleware(explicit)
                : base.withHooks(explicit)
                    .withMiddleware(explicit)
                    .withPlugins(first)
                    .withPlugins(second))
            .withPilot(new Pilot() {});
    var completed = new CompletableFuture<Workers.Event>();
    try (var workers =
            river
                .workers()
                .queue("default", 1)
                .leadership(false)
                .pollOnly(true)
                .pollInterval(Duration.ofMillis(10))
                .stopTimeout(Duration.ofSeconds(5))
                .add(TYPE, context -> trace.add("handler"))
                .start();
        var subscription = workers.subscribe(completed::complete)) {
      river.insert(TYPE, "one");
      assertEquals(Job.State.COMPLETED, completed.get(5, TimeUnit.SECONDS).job().state());
    }
    assertEquals(
        List.of(
            "first:insert_before",
            "second:insert_before",
            "explicit:insert_before",
            "first:insert_begin",
            "second:insert_begin",
            "explicit:insert_begin",
            "explicit:insert_after",
            "second:insert_after",
            "first:insert_after",
            "first:work_before",
            "second:work_before",
            "explicit:work_before",
            "first:work_begin",
            "second:work_begin",
            "explicit:work_begin",
            "handler",
            "first:work_end",
            "second:work_end",
            "explicit:work_end",
            "explicit:work_after",
            "second:work_after",
            "first:work_after"),
        trace);
  }

  @Test
  void pluginsIncludePeriodicStartHooks() throws Exception {
    var started = new LinkedBlockingQueue<String>();
    Plugin first = (HookPeriodicJobsStart) () -> started.add("first");
    Plugin second = (HookPeriodicJobsStart) () -> started.add("second");
    var river =
        client()
            .withHooks((HookPeriodicJobsStart) () -> started.add("explicit"))
            .withPlugins(first, second);
    try (var workers =
        river
            .workers()
            .queue("default", 1)
            .pollOnly(true)
            .serviceInterval(Duration.ofHours(1))
            .stopTimeout(Duration.ofSeconds(5))
            .add(TYPE, context -> {})
            .start()) {
      assertEquals("first", started.poll(5, TimeUnit.SECONDS));
      assertEquals("second", started.poll(5, TimeUnit.SECONDS));
      assertEquals("explicit", started.poll(5, TimeUnit.SECONDS));
      assertTrue(workers.isLeader());
    }
    assertTrue(started.isEmpty());
  }

  @Test
  void registrationRejectsNullPlugins() {
    var river = client();
    assertThrows(NullPointerException.class, () -> river.withPlugins((Plugin[]) null));
    assertThrows(NullPointerException.class, () -> river.withPlugins(new Plugin() {}, null));
  }

  private static final class RecordingPlugin
      implements HookInsertBegin,
          HookWorkBegin,
          HookWorkEnd,
          JobInsertMiddleware,
          WorkerMiddleware {
    private final String name;
    private final List<String> trace;

    RecordingPlugin(String name, List<String> trace) {
      this.name = name;
      this.trace = trace;
    }

    @Override
    public void insertBegin(
        Connection connection, JobType<?> type, Object args, InsertOptions options) {
      trace.add(name + ":insert_begin");
    }

    @Override
    public List<Job.InsertResult> insertMany(
        Connection connection,
        List<Client.Submission<?>> submissions,
        Callable<List<Job.InsertResult>> next)
        throws Exception {
      trace.add(name + ":insert_before");
      var results = next.call();
      trace.add(name + ":insert_after");
      return results;
    }

    @Override
    public void work(WorkContext<?> context, WorkContext.Step next) throws Exception {
      trace.add(name + ":work_before");
      next.run();
      trace.add(name + ":work_after");
    }

    @Override
    public void workBegin(WorkContext<?> context) {
      trace.add(name + ":work_begin");
    }

    @Override
    public Throwable workEnd(WorkContext<?> context, Throwable failure) {
      trace.add(name + ":work_end");
      return failure;
    }
  }
}
