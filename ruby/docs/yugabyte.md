# YugabyteDB

Both SQL drivers support YugabyteDB through its Postgres-compatible YSQL
endpoint. Use the `pg` gem and the usual Postgres connection configuration;
no separate River driver or client flag is required. Apply the bundled
[Postgres migrations](migrations.md).

```ruby
db = Sequel.connect("postgres://yugabyte@localhost:5433/my_app")
client = River::Client.new(River::Driver::Sequel.new(db))

# Or Active Record:
ActiveRecord::Base.establish_connection("postgres://yugabyte@localhost:5433/my_app")
client = River::Client.new(River::Driver::ActiveRecord.new)
```

## Capability detection

Ruby follows Go's database detection and unique-insert strategies:

| Server | Duplicate detection |
| --- | --- |
| YugabyteDB | A random nonce in reserved metadata `river:unique_nonce` |
| Postgres 18+ | `OLD.id IS NOT NULL` in `RETURNING` |
| Earlier Postgres | `xmax != 0` |

Yugabyte's product identification takes precedence over its Postgres version.
The nonce is generated per returning insert batch, just as in Go's Postgres
drivers. Existing jobs without a nonce are still recognized as duplicates;
application metadata is preserved, apart from the reserved nonce key.

Detection happens at worker startup or lazily for insert-only clients. A
successful result is cached for each connection pool used by a driver, without
holding a lock across database I/O. Failed detection can be retried. Active
Record roles/shards use separate cache entries. Create a new driver after
changing server capabilities or notification settings.

## Notifications and transactions

If `yb_enable_listen_notify` is absent or false, the drivers omit native
notifications. Ruby workers already poll for jobs, queue settings, and remote
cancellations, so no additional polling option is necessary. Cancellation
markers and inserted jobs become visible to other connections only when the
application transaction commits; rollback leaves neither behind.

When native notifications are enabled, insertion and cancellation publish the
same commit-bound notifications as Go. Ruby listens for inserts, queue controls,
and leadership messages on a dedicated connection. Set `poll_only: true` in
`River::Config` to disable the receiver explicitly.

Native notifications require YugabyteDB 2025.2.3 or later and
`ysql_yb_enable_listen_notify=true` on both Masters and TServers. Follow
[Yugabyte's LISTEN/NOTIFY setup](https://docs.yugabyte.com/stable/api/ysql/the-sql-language/statements/cmd_listen_notify/),
including its replication prerequisites. This is optional, not a prerequisite
for using River.

Transactions are delegated to Active Record or Sequel, with River-owned writes
and hooks remaining in the transaction. Database errors propagate; River does
not replay application transactions or hooks automatically.

## Verification

Normal driver tests mirror Go's Postgres-based Yugabyte simulations, covering
an absent, disabled, or enabled notification setting. Disabled simulations make
`pg_notify` raise to detect accidental broadcasts. These do not emulate
Yugabyte's storage or transaction semantics.

Run the real-database tests separately with a disposable YSQL database:

```sh
make test/yugabyte YUGABYTE_DATABASE_URL=postgres://yugabyte@localhost:5433/river_test
```

The target requires a database and tests both adapters in temporary schemas,
including canonical migrations, batch uniqueness, metadata, transaction
rollback, and remote cancellation. For a server with native notifications
configured, also set `YUGABYTE_LISTEN_NOTIFY_ENABLED=1`; this additionally checks
notification delivery and commit/rollback behavior.
