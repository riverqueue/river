# riverqueue-sequel

[Sequel](https://sequel.jeremyevans.net/) driver for [River](https://github.com/riverqueue/river)'s [`riverqueue` gem for Ruby](https://rubygems.org/gems/riverqueue). Postgres and SQLite are supported.

Add this driver and only the database adapter used by the application to
`Gemfile`. The driver pulls in the core gem:

```ruby
gem "riverqueue-sequel"
gem "pg" # or: gem "sqlite3"
```

For Postgres, add `pg` to `Gemfile`:

```ruby
gem "pg"
```

Then initialize a client with a Sequel database:

```ruby
db = Sequel.connect("postgres://localhost/my_app")
client = River::Client.new(River::Driver::Sequel.new(db))
```

For SQLite, add `sqlite3` to `Gemfile`:

```ruby
gem "sqlite3"
```

Then initialize a client with an SQLite database:

```ruby
db = Sequel.connect("sqlite://storage/river.sqlite3", timeout: 5_000)
client = River::Client.new(River::Driver::Sequel.new(db))
```

Use current River migrations to create and update the SQLite database.

YugabyteDB is supported through the Postgres adapter, with automatic capability
detection. See [YugabyteDB setup and behavior](../../../docs/yugabyte.md).

## Development

See [development](./development.md).
