package com.riverqueue;

import java.time.Instant;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BooleanSupplier;

/**
 * Thread-safe periodic registrations owned by a {@link RunHandle} instance. Only the leader
 * enqueues occurrences, so update every eligible runtime to change schedules cluster-wide. Removal
 * leaves previously inserted jobs alone, and an insertion already in progress may finish.
 * Registrations can still be added and removed after stop, but no further jobs are enqueued.
 */
public final class PeriodicJobBundle {
  private final boolean enabled;
  private final Map<Handle, Entry> entries = new LinkedHashMap<>();
  private final BooleanSupplier running;
  private final Runnable wake;

  PeriodicJobBundle(
      List<PeriodicJob<?>> jobs, boolean enabled, BooleanSupplier running, Runnable wake) {
    this.enabled = enabled;
    this.running = running;
    this.wake = wake;
    addMany(jobs);
  }

  /** Adds a definition, returning a handle that identifies this particular registration. */
  public Handle add(PeriodicJob<?> job) {
    return addMany(List.of(job)).getFirst();
  }

  /** Constructs a periodic definition and adds it to this bundle. */
  public <A> Handle add(
      String id,
      Schedule schedule,
      JobType<A> type,
      A args,
      InsertOptions options,
      boolean runOnStart) {
    return add(new PeriodicJob<>(id, schedule, type, args, options, runOnStart));
  }

  /**
   * Adds definitions atomically, returning handles in input order. Duplicate IDs leave the entire
   * bundle unchanged. RunOnStart jobs wake the leader for immediate insertion.
   */
  public List<Handle> addMany(List<? extends PeriodicJob<?>> jobs) {
    var additions = List.copyOf(jobs).stream().map(Entry::new).toList();
    if (additions.isEmpty()) return List.of();
    synchronized (entries) {
      if (!enabled) throw new IllegalStateException("Periodic jobs require leader election");
      var ids = new HashSet<String>();
      for (var entry : entries.values()) if (entry.job.id() != null) ids.add(entry.job.id());
      for (var entry : additions)
        if (entry.job.id() != null && !ids.add(entry.job.id()))
          throw new IllegalArgumentException("Duplicate periodic job ID: " + entry.job.id());
      for (var entry : additions) entries.put(entry.handle, entry);
    }
    if (running.getAsBoolean() && additions.stream().anyMatch(entry -> entry.job.runOnStart()))
      wake.run();
    return additions.stream().map(entry -> entry.handle).toList();
  }

  /** Removes all definitions from this bundle. */
  public void clear() {
    synchronized (entries) {
      entries.clear();
    }
  }

  boolean contains(Entry entry) {
    synchronized (entries) {
      return entries.get(entry.handle) == entry;
    }
  }

  /**
   * Removes the registration identified by a handle. Returns false for a removed handle or one
   * belonging to another bundle, even if a new registration reuses the same ID.
   */
  public boolean remove(Handle handle) {
    Objects.requireNonNull(handle, "handle");
    synchronized (entries) {
      return entries.remove(handle) != null;
    }
  }

  /**
   * Removes a named definition, returning whether it existed. Null never matches anonymous jobs.
   */
  public boolean removeById(String id) {
    if (id == null) return false;
    synchronized (entries) {
      return entries.values().removeIf(entry -> id.equals(entry.job.id()));
    }
  }

  /** Removes registrations by handle; unknown or already removed handles are ignored. */
  public void removeMany(List<Handle> handles) {
    var values = List.copyOf(handles);
    synchronized (entries) {
      values.forEach(entries::remove);
    }
  }

  /** Removes named definitions; unknown IDs are ignored. */
  public void removeManyById(List<String> ids) {
    var values = new HashSet<>(List.copyOf(ids));
    synchronized (entries) {
      entries.values().removeIf(entry -> values.contains(entry.job.id()));
    }
  }

  List<Entry> snapshot() {
    synchronized (entries) {
      return List.copyOf(entries.values());
    }
  }

  static final class Entry {
    final Handle handle = new Handle();
    final PeriodicJob<?> job;
    // Only the runtime's serial maintenance passes advance these fields.
    Instant next;
    Instant term;

    Entry(PeriodicJob<?> job) {
      this.job = job;
    }
  }

  /**
   * An opaque identity for one registration, returned by {@link #add(PeriodicJob)} or {@link
   * #addMany}.
   */
  public static final class Handle {
    private Handle() {}
  }
}
