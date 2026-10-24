package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PeriodicJobBundleTest {
  private static final JobType<String> TYPE = JobType.of("periodic_bundle_test", String.class);

  @Test
  void addManyRejectsDuplicatesWithoutPartiallyRegisteringJobs() {
    var wakes = new AtomicInteger();
    var bundle = new PeriodicJobBundle(List.of(), true, () -> true, wakes::incrementAndGet);
    var existing = bundle.add(job("existing"));
    assertThrows(
        IllegalArgumentException.class, () -> bundle.addMany(List.of(job("new"), job("existing"))));
    assertThrows(
        IllegalArgumentException.class, () -> bundle.addMany(List.of(job("new"), job("new"))));
    assertFalse(bundle.removeById("new"));
    assertEquals(0, wakes.get());
    assertTrue(bundle.remove(existing));

    var handles = bundle.addMany(List.of(job("first"), job("second")));
    assertEquals(0, wakes.get());
    assertTrue(bundle.removeById("first"));
    assertFalse(bundle.remove(handles.getFirst()), "Handles must follow input order");
    assertTrue(bundle.remove(handles.get(1)));
  }

  @Test
  void additionsWakeOnlyRunOnStartJobsWhileRunning() {
    var running = new java.util.concurrent.atomic.AtomicBoolean(true);
    var wakes = new AtomicInteger();
    var bundle = new PeriodicJobBundle(List.of(), true, running::get, wakes::incrementAndGet);
    bundle.add(job("scheduled"));
    assertEquals(0, wakes.get());
    var immediate =
        new PeriodicJob<>(
            "immediate",
            Schedule.every(Duration.ofHours(1)),
            TYPE,
            "args",
            InsertOptions.defaults(),
            true);
    bundle.addMany(List.of(job("another"), immediate));
    assertEquals(1, wakes.get());
    running.set(false);
    bundle.clear();
    var handle = bundle.add(immediate);
    assertEquals(1, wakes.get());
    assertTrue(bundle.remove(handle));
    bundle.add(immediate);
    assertTrue(bundle.removeById("immediate"));
  }

  @Test
  void anonymousRegistrationsHaveDistinctHandles() {
    var bundle = bundle();
    var handles = bundle.addMany(List.of(job(null), job(null)));
    assertNotSame(handles.getFirst(), handles.get(1));
    assertFalse(bundle.removeById(null));
    assertTrue(bundle.remove(handles.getFirst()));
    assertTrue(bundle.remove(handles.get(1)));
  }

  private static PeriodicJobBundle bundle() {
    return new PeriodicJobBundle(List.of(), true, () -> true, () -> {});
  }

  @Test
  void concurrentAddsDoNotRegisterDuplicateIds() throws Exception {
    var bundle = bundle();
    var start = new CompletableFuture<Void>();
    var added = new AtomicInteger();
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var attempts = new java.util.ArrayList<java.util.concurrent.Future<?>>();
      for (int i = 0; i < 10; i++)
        attempts.add(
            executor.submit(
                () -> {
                  start.join();
                  try {
                    bundle.add(job("same"));
                    added.incrementAndGet();
                  } catch (IllegalArgumentException expected) {
                    assertEquals("Duplicate periodic job ID: same", expected.getMessage());
                  }
                }));
      start.complete(null);
      for (var attempt : attempts) attempt.get(5, java.util.concurrent.TimeUnit.SECONDS);
    }
    assertEquals(1, added.get());
    assertTrue(bundle.removeById("same"));
    assertFalse(bundle.removeById("same"));
  }

  @Test
  void handlesCannotRemoveReplacementsOrJobsInAnotherBundle() {
    var first = bundle();
    var second = bundle();
    var old = first.add(job("reused"));
    assertTrue(first.removeById("reused"));
    var replacement = first.add(job("reused"));
    var other = second.add(job("reused"));
    assertFalse(first.remove(old));
    assertFalse(first.remove(other));
    assertFalse(second.remove(replacement));
    assertTrue(first.remove(replacement));
    assertTrue(second.remove(other));
  }

  private static PeriodicJob<String> job(String id) {
    return new PeriodicJob<>(
        id, Schedule.every(Duration.ofHours(1)), TYPE, "args", InsertOptions.defaults(), false);
  }

  @Test
  void removeManyAndClearReleaseIdsWithoutReusingHandles() {
    var bundle = bundle();
    var handles = bundle.addMany(List.of(job("first"), job("second"), job("third")));
    bundle.removeMany(List.of(handles.getFirst(), handles.get(1), handles.getFirst()));
    assertFalse(bundle.removeById("first"));
    assertFalse(bundle.removeById("second"));
    bundle.removeManyById(List.of("third", "missing"));
    assertFalse(bundle.remove(handles.get(2)));
    var cleared = bundle.add(job("first"));
    bundle.add(job("second"));
    bundle.clear();
    var replacement = bundle.add(job("first"));
    assertFalse(bundle.remove(cleared));
    assertFalse(bundle.removeById("second"));
    assertTrue(bundle.remove(replacement));
    bundle.clear();
  }
}
