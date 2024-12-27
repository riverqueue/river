module github.com/riverqueue/river

go 1.22

toolchain go1.23.0

require (
	github.com/jackc/pgerrcode v0.0.0-20220416144525-469b46aa5efa
	github.com/jackc/pgx/v5 v5.7.2
	github.com/jackc/puddle/v2 v2.2.2
	github.com/riverqueue/river/riverdriver v0.15.0
	github.com/riverqueue/river/riverdriver/riverdatabasesql v0.15.0
	github.com/riverqueue/river/riverdriver/riverpgxv5 v0.15.0
	github.com/riverqueue/river/rivershared v0.15.0
	github.com/riverqueue/river/rivertype v0.15.0
	github.com/robfig/cron/v3 v3.0.1
	github.com/stretchr/testify v1.10.0
	github.com/tidwall/gjson v1.18.0
	github.com/tidwall/sjson v1.2.5
	go.uber.org/goleak v1.3.0
	golang.org/x/sync v0.10.0
	golang.org/x/text v0.21.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/lib/pq v1.10.9 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	golang.org/x/crypto v0.31.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/riverqueue/river/rivershared => ./rivershared
