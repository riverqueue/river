// Cross-language conformance tooling and the Go-generated fixtures that ports
// test against. It's a separate module so none of it ships in River's module
// zip, and it's never tagged or released.
module github.com/riverqueue/river/conformance

go 1.26.0

toolchain go1.26.6

require (
	github.com/jackc/pgx/v5 v5.11.0
	github.com/riverqueue/river v0.49.0
	github.com/riverqueue/river/riverdriver v0.49.0
	github.com/riverqueue/river/riverdriver/riverpgxv5 v0.49.0
	github.com/riverqueue/river/riverdriver/riversqlite v0.49.0
	github.com/riverqueue/river/rivershared v0.49.0
	github.com/riverqueue/river/rivertype v0.49.0
	github.com/robfig/cron/v3 v3.0.1
	github.com/stretchr/testify v1.12.1
	golang.org/x/mod v0.41.0
	modernc.org/sqlite v1.60.1
)

require (
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/tidwall/gjson v1.20.0 // indirect
	github.com/tidwall/match v1.2.0 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/tidwall/sjson v1.2.5 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/sync v0.23.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	modernc.org/libc v1.77.1 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.12.1 // indirect
)
