# Testing River jobs

Testing helpers ship in `riverqueue`. They use your real PostgreSQL or SQLite
database through either driver; there is no fake queue, global testing mode,
implicit migration, or automatic cleanup. Require them explicitly:

```ruby
require "riverqueue/testing"
```

Use an isolated test database/schema and a stopped client with registered workers.
Do not run background consumers or concurrent tests against the same job rows.
Your test suite owns migrations and cleanup. Transactional tests work when the
client uses the same database connection as the test transaction: synchronous
execution stays on the calling thread. Sharing a database URL is not sufficient.

## RSpec

Add `rspec-expectations` (or `rspec`) to your test bundle, then:

```ruby
require "riverqueue/testing/rspec"

RSpec.configure do |config|
  config.include River::Testing::RSpec
end

expect { enqueue_order(42) }.to insert_job(
  client,
  args: {"order_id" => 42},
  kind: :fulfill_order,
  queue: :orders
)

expect { enqueue_orders }.to insert_jobs(client, count: 3, kind: :fulfill_order)
expect { ignore_duplicate_order }.not_to insert_job(client)
```

Attributes support composable RSpec expectations, including nested hash/array
matchers and time tolerances. Literal hashes still require all their keys to
match; use `a_hash_including` for a subset. JSON object keys are strings.
Identifier filters (`kind:`, `queue:`, and `state:`) accept symbols or strings;
literal JSON values and nested matchers are not coerced.

```ruby
expect { enqueue_order(42) }.to insert_job(
  client,
  args: a_hash_including("order_id" => 42),
  scheduled_at: be_within(1).of(expected_time)
)

expect { enqueue_orders }.to insert_jobs(client, kind: :fulfill_order).at_least(2)
expect { enqueue_orders }.to insert_jobs(client).at_most(5)
expect { enqueue_orders }.to insert_jobs(client).exactly(3)
```

`insert_job` and `insert_jobs` default to exactly one matching new row. `count:`
on `insert_jobs` remains shorthand for an exact count. Count chains accept
nonnegative integers; the last chain determines the count requirement. Other,
nonmatching jobs do not affect the count. Negation means **zero** matching rows,
not merely failure of the positive count requirement—even with `at_most` or
`at_least`. Use a positive `.exactly(0)` to assert a zero count explicitly.

Use `have_job` to inspect existing persisted jobs rather than insertions inside
a block. It defaults to at least one match and supports the same count chains:

```ruby
expect(client).to have_job(kind: :fulfill_order, queue: :orders)
expect(client).to have_job(args: a_hash_including("order_id" => 42)).exactly(1)
expect(client).not_to have_job(state: :discarded)

expect { enqueue_order_and_notification }.to(
  insert_job(client, kind: :fulfill_order)
    .and(insert_job(client, kind: :notify_order))
)
```

`have_job` includes every job state unless filtered with `state:`; it does not
mean only waiting jobs. Insertion matchers support block expectations and
`have_job` supports value expectations. Compound insertion expectations execute
the application block once. Matchers only read persisted rows; they do not run
workers or clean up data. `River::Testing.jobs(client)` also returns all rows
through paginated reads for custom assertions.

These richer comparisons are RSpec-only. Framework-neutral and Minitest
insertion assertions use the same identifier normalization, but otherwise retain
exact attribute equality and exact counts.

## Minitest and other frameworks

Add `minitest` to your test bundle, then explicitly include its integration:

```ruby
require "minitest/autorun"
require "riverqueue/testing/minitest"

class OrderTest < Minitest::Test
  include River::Testing::Minitest

  def test_enqueue
    row = assert_job_inserted client, args: {"order_id" => 42}, kind: :fulfill_order do
      enqueue_order(42)
    end
    assert_equal "orders", row.queue

    assert_jobs_inserted(client, count: 3) { enqueue_orders }
    assert_no_jobs_inserted(client) { ignore_duplicate_order }
  end
end
```

`client`, `enqueue_order`, and `enqueue_orders` above are application/test fixture
methods. The integration uses Minitest's assertion counts and failure reporting;
it does not enable autorun itself. Other frameworks can include
`River::Testing::Assertions` (failures raise `River::Testing::AssertionError`) or
use `River::Testing.inserted_jobs(client, **attributes) { ... }` directly.

Insertion checks compare row IDs before and after the block. A uniqueness conflict
returning an existing row does not count as an insertion. Rows deleted or rolled
back before the block ends cannot be observed. Exceptions from your block propagate.

## Execute one real attempt

```ruby
result = River::Testing.perform_job(client, row.id)

expect(result).to have_attributes(error: nil, outcome: :completed)
expect(result.job).to have_attributes(attempt: 1, state: "completed")
```

This claims the specified job atomically, then uses the real runtime, including
plugins, timeout, retries, error handling, snoozes, cancellation, resumable steps,
metadata/output persistence, and finalization. It does not start consumer threads,
maintenance services, or periodic producers. The client cannot be started or used
for another synchronous attempt until execution finishes.

`ExecutionResult` contains `id`, `error` (the original exception, or `nil`), `job`
(the row after execution, or `nil` if deleted), and `outcome`: `:completed`,
`:retried`, `:discarded`, `:cancelled`, `:snoozed`, `:interrupted`, or `:deleted`.
Worker errors are returned, not re-raised; assert the outcome so a failed worker
cannot accidentally pass your test. Database/finalization failures can still raise.

Minitest and framework-neutral assertion modules also provide
`assert_job_completed(result)`, `assert_job_cancelled(result)`, and
`assert_job_discarded(result)`; each returns the result on success.

Only available, scheduled, or retryable jobs are eligible. Future jobs require an
explicit override; this does not change their original `scheduled_at`:

```ruby
result = River::Testing.perform_job(client, row.id, allow_scheduled: true)
```

Direct execution bypasses queue pause, worker capacity, and Pro concurrency claim
controls. It is a worker test helper, not a scheduler simulator. Pro plugins may
have additional effects (such as a batch worker claiming additional jobs).
Use regular threaded integration tests for these behaviors and maintenance-driven
workflow progression. With Rails, continue to use `ActiveJob::TestHelper` for
adapter-independent tests; use these helpers with a real River adapter/client to
test persistence and execution.

## Drain a queue

```ruby
results = River::Testing.drain(client, max_jobs: 20, queue: :orders)
expect(results.map(&:outcome)).to all(eq(:completed))
```

Draining runs due jobs sequentially in priority, scheduled-time, then ID order,
including jobs inserted by workers. It does not sleep for future jobs or run
maintenance. Due scheduled/retryable jobs can be claimed directly. The default
limit is 100 attempts; if runnable work remains at the limit, it raises
`River::Testing::DrainLimitError`. Attempts already performed remain persisted.
