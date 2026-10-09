---
name: river-job-lint
description: Review Go code used by River jobs for channel sends, receives, ranges, and select statements that can block without observing the job context. Use when auditing River worker implementations or their helpers for stuck-job risks, ignored timeouts or cancellation, graceful-shutdown hangs, goroutine leaks, or channel deadlocks.
---

# River Job Lint

Audit code that runs from River workers for channel operations that may prevent a job from returning after its context is cancelled. Base the review on [Go's stuck goroutine problem](https://riverqueue.com/blog/go-stuck-jobs): Go cannot forcibly terminate a goroutine, so blocking code must cooperate with cancellation.

## Audit workflow

1. Establish scope.
   - Find River `Work` methods, worker function adapters, and any files the user names.
   - Follow helper calls that execute synchronously from a job, especially helpers receiving a `context.Context` or a channel.
   - Inspect goroutines started by the job and both sides of their channels. A wait may be in the worker while the bug is an early return in its producer, or vice versa.
   - Skip vendored, generated, and test-only code unless the user includes it.

2. Inventory candidate channel operations.
   - Find every send (`ch <- value`) and receive (`<-ch`), including operations in assignments, returns, and select cases.
   - Find channel ranges (`for value := range ch`) and selects that wait only on application channels.
   - Treat text search as candidate generation. Read the surrounding control flow before reporting a finding.

3. Trace cancellation and channel lifecycle.
   - Determine whether the operation can block at runtime. A bare send or receive can block; a buffered channel is not automatically safe because its buffer may fill or be empty.
   - For a blocking operation, require a `select` case on `ctx.Done()` or on a derived context whose ancestry reaches the `ctx` passed to `Work`.
   - Flag helpers that replace the job context with `context.Background()` or `context.TODO()`, or that wait on a channel without accepting any cancellation signal.
   - For a channel range, prove that a producer closes the channel on every exit path. Prefer a cancellation-aware receive loop even when closure appears reliable.
   - Check nil-channel possibilities, early returns, error paths, and panics that can make a sender, receiver, or closer disappear.
   - Check goroutines that can outlive `Work`. Returning from the worker does not stop a child goroutine.

4. Separate hazards from safe operations.
   - Do not flag a select that observes the job context and returns promptly on cancellation.
   - Do not flag an operation proven non-blocking by local control flow, but require structural proof rather than an assumption that another goroutine “normally” responds.
   - A select with `default` makes that select non-blocking, but inspect its counterpart: returning early may strand a later sender.
   - A timer or deadline bounds a wait, but it may still ignore job cancellation until the timer fires. Report long or operationally significant delays.
   - A custom done channel is sufficient only when it is demonstrably tied to the job context and closed on every relevant path.

5. Report findings before changing code unless the user asks for fixes.
   - Prioritize concrete, reachable problems over speculative possibilities.
   - Include the worker or call path, file and line, blocking operation, missing cancellation path, and likely River impact.
   - Explain whether the job can ignore a timeout, explicit cancellation, or shutdown, and whether it can consume a worker slot indefinitely.
   - If no problems are found, state which workers and helpers were inspected and any limits in call-path coverage.

## Preferred fixes

Make a receive cancellation-aware:

```go
select {
case result, ok := <-resultCh:
	if !ok {
		return errResultsClosed
	}
	return handleResult(result)
case <-ctx.Done():
	return ctx.Err()
}
```

Make a send cancellation-aware:

```go
select {
case resultCh <- result:
	return nil
case <-ctx.Done():
	return ctx.Err()
}
```

Replace a channel range when cancellation must interrupt the wait:

```go
for {
	select {
	case result, ok := <-resultCh:
		if !ok {
			return nil
		}
		if err := handleResult(result); err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}
```

Keep the context case in the same select as the potentially blocking operation. A separate `if ctx.Err() != nil` check has a race between the check and the subsequent channel wait.

When fixing a multi-goroutine pipeline, make every potentially blocking send and receive cancellation-aware, define channel ownership explicitly, and close channels only from their sending side.

## Finding format

Use this compact structure:

```text
- High — path/to/worker.go:42: receive from resultCh can wait forever.
  Work waits for a producer that may return on an error without sending or
  closing the channel. The receive has no ctx.Done() case, so job timeout and
  shutdown cancellation cannot make Work return. Wrap the receive in a select
  with the job context and make the producer close its owned channel.
```

Use **High** for an unbounded wait reachable from a River job. Use **Medium** for bounded-but-slow cancellation, indirect goroutine leaks, or lifecycle hazards that need a specific race. Do not inflate severity when cancellation is already structurally guaranteed.
