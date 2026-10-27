// Cross-language conformance tooling and the Go-generated fixtures that ports
// test against. It's a separate module so none of it ships in River's module
// zip, and it's never tagged or released.
module github.com/riverqueue/river/conformance

go 1.26.0

toolchain go1.26.6

require (
	github.com/riverqueue/river v0.49.0
	github.com/riverqueue/river/riverdriver v0.49.0
	github.com/riverqueue/river/rivershared v0.49.0
	github.com/riverqueue/river/rivertype v0.49.0
	github.com/robfig/cron/v3 v3.0.1
	github.com/stretchr/testify v1.12.1
	golang.org/x/mod v0.42.0
)

require go.yaml.in/yaml/v3 v3.0.5 // indirect

require (
	github.com/jackc/puddle/v2 v2.2.3 // indirect
	github.com/tidwall/gjson v1.20.0 // indirect
	github.com/tidwall/match v1.2.0 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/tidwall/sjson v1.2.5 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	golang.org/x/tools v0.52.0 // indirect
)
