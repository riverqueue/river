module github.com/riverqueue/river/riverdriver/riverdatabasesql

go 1.21.4

replace github.com/riverqueue/river/riverdriver => ../

replace github.com/riverqueue/river/rivertype => ../../rivertype

require (
	github.com/lib/pq v1.10.9
	github.com/riverqueue/river/riverdriver v0.4.0
	github.com/riverqueue/river/rivertype v0.4.0
	github.com/stretchr/testify v1.9.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rogpeppe/go-internal v1.11.0 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
