module github.com/riverqueue/river/cmd/river

go 1.23.0

toolchain go1.24.1

require (
	github.com/jackc/pgx/v5 v5.7.5
	github.com/lmittmann/tint v1.1.1
	github.com/riverqueue/river v0.22.0
	github.com/riverqueue/river/riverdriver v0.22.0
	github.com/riverqueue/river/riverdriver/riverpgxv5 v0.22.0
	github.com/riverqueue/river/riverdriver/riversqlite v0.0.0-00010101000000-000000000000
	github.com/riverqueue/river/rivershared v0.22.0
	github.com/riverqueue/river/rivertype v0.22.0
	github.com/spf13/cobra v1.9.1
	github.com/stretchr/testify v1.10.0
	modernc.org/sqlite v1.37.1
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/ncruces/go-strftime v0.1.9 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/spf13/pflag v1.0.6 // indirect
	github.com/tidwall/gjson v1.18.0 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/tidwall/sjson v1.2.5 // indirect
	go.uber.org/goleak v1.3.0 // indirect
	golang.org/x/crypto v0.37.0 // indirect
	golang.org/x/exp v0.0.0-20250408133849-7e4ce0ab07d0 // indirect
	golang.org/x/sync v0.14.0 // indirect
	golang.org/x/sys v0.33.0 // indirect
	golang.org/x/text v0.25.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	modernc.org/libc v1.65.7 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
)

// TODO(brandur): Remove this before first release including SQLite driver. It's
// needed temporarily because there's no riversqlite tag to target (the one
// referenced above is fake and does not exist).
replace github.com/riverqueue/river/riverdriver/riversqlite => ../../../river/riverdriver/riversqlite
