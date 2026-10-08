# YugabyteDB

The Postgres drivers automatically use polling when YugabyteDB's
`yb_enable_listen_notify` setting is absent or disabled. This includes YugabyteDB
2025.2.1, even with the default `PollOnly: false`. New jobs are picked up on the
`FetchPollInterval`, and queue pause, resume, and metadata changes are picked up
by polling queue settings (every two seconds by default). Running job cancellation
requests are also polled every two seconds, including requests from other clients
and requests made with `JobCancelTx` once committed.

Native notifications require YugabyteDB **2025.2.3 or later**, with
`ysql_yb_enable_listen_notify=true` on **both Masters and TServers**. The feature
is disabled by default. See [Yugabyte's LISTEN/NOTIFY documentation](https://docs.yugabyte.com/stable/api/ysql/the-sql-language/statements/cmd_listen_notify/)
for the additional replication configuration requirements. `PollOnly: true`
continues to force polling even when native notifications are enabled.

Database capabilities are cached for the lifetime of the driver. After enabling
notifications, restart the application with a new driver to detect the change.
