module github.com/riverqueue/river

go 1.21.4

replace github.com/riverqueue/river/riverdriver => ./riverdriver

replace github.com/riverqueue/river/riverdriver/riverpgxv5 => ./riverdriver/riverpgxv5

replace github.com/riverqueue/river/riverdriver/riverdatabasesql => ./riverdriver/riverdatabasesql

replace github.com/riverqueue/river/rivertype => ./rivertype

require (
	github.com/jackc/pgerrcode v0.0.0-20220416144525-469b46aa5efa
	github.com/jackc/pgx/v5 v5.5.5
	github.com/jackc/puddle/v2 v2.2.1
	github.com/riverqueue/river/riverdriver v0.0.25
	github.com/riverqueue/river/riverdriver/riverdatabasesql v0.0.25
	github.com/riverqueue/river/riverdriver/riverpgxv5 v0.0.25
	github.com/riverqueue/river/rivertype v0.0.25
	github.com/robfig/cron/v3 v3.0.1
	github.com/spf13/cobra v1.8.0
	github.com/stretchr/testify v1.9.0
	go.uber.org/goleak v1.3.0
	golang.org/x/mod v0.16.0
	golang.org/x/sync v0.6.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/lib/pq v1.10.9 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	golang.org/x/crypto v0.17.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
