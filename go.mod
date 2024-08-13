module github.com/riverqueue/river

go 1.21

toolchain go1.22.5

require (
	github.com/jackc/pgerrcode v0.0.0-20220416144525-469b46aa5efa
	github.com/jackc/pgx/v5 v5.6.0
	github.com/jackc/puddle/v2 v2.2.1
	github.com/riverqueue/river/riverdriver v0.11.2
	github.com/riverqueue/river/riverdriver/riverdatabasesql v0.11.2
	github.com/riverqueue/river/riverdriver/riverpgxv5 v0.11.2
	github.com/riverqueue/river/rivershared v0.11.2
	github.com/riverqueue/river/rivertype v0.11.2
	github.com/robfig/cron/v3 v3.0.1
	github.com/stretchr/testify v1.9.0
	go.uber.org/goleak v1.3.0
	golang.org/x/sync v0.8.0
	golang.org/x/text v0.17.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/lib/pq v1.10.9 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/crypto v0.17.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
