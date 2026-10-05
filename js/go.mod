// This module exists only to keep the JavaScript workspace out of River's Go
// module: Go excludes nested modules from the module zip that the Go proxy
// serves, and `go test ./...`, `go vet ./...`, and golangci-lint don't descend
// into them (or into `node_modules`). It contains no Go code. Don't tag it:
// a `js/vX.Y.Z` tag would be read as a version of this module.
module github.com/riverqueue/river/js
