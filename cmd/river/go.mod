module github.com/riverqueue/river/cmd/river

go 1.22.5

// replace github.com/riverqueue/river => ../..

// replace github.com/riverqueue/river/riverdriver => ../../riverdriver

// replace github.com/riverqueue/river/riverdriver/riverdatabasesql => ../../riverdriver/riverdatabasesql

// replace github.com/riverqueue/river/riverdriver/riverpgxv5 => ../../riverdriver/riverpgxv5

// replace github.com/riverqueue/river/rivertype => ../../rivertype

require (
	github.com/jackc/pgx/v5 v5.6.0
	github.com/lmittmann/tint v1.0.4
	github.com/riverqueue/river v0.10.2
	github.com/riverqueue/river/riverdriver v0.10.2
	github.com/riverqueue/river/riverdriver/riverpgxv5 v0.10.2
	github.com/riverqueue/river/rivertype v0.10.2
	github.com/spf13/cobra v1.8.0
	github.com/stretchr/testify v1.9.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/riverqueue/river/rivershared v0.10.2 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	go.uber.org/goleak v1.3.0 // indirect
	golang.org/x/crypto v0.25.0 // indirect
	golang.org/x/sync v0.7.0 // indirect
	golang.org/x/text v0.16.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
