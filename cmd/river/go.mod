module github.com/riverqueue/river/cmd/river

go 1.21.4

// replace github.com/riverqueue/river => ../..

// replace github.com/riverqueue/river/riverdriver => ../../riverdriver

// replace github.com/riverqueue/river/riverdriver/riverdatabasesql => ../../riverdriver/riverdatabasesql

// replace github.com/riverqueue/river/riverdriver/riverpgxv5 => ../../riverdriver/riverpgxv5

require (
	github.com/jackc/pgx/v5 v5.5.1
	github.com/riverqueue/river v0.0.13
	github.com/riverqueue/river/riverdriver/riverpgxv5 v0.0.13
	github.com/spf13/cobra v1.8.0
)

require (
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	github.com/riverqueue/river/riverdriver v0.0.13 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	golang.org/x/crypto v0.15.0 // indirect
	golang.org/x/sync v0.5.0 // indirect
	golang.org/x/text v0.14.0 // indirect
)
