.PHONY: generate
generate:
generate: generate/sqlc

.PHONY: generate/sqlc
generate/sqlc:
	cd riverdriver/riverdatabasesql/internal/dbsqlc && sqlc generate
	cd riverdriver/riverpgxv5/internal/dbsqlc && sqlc generate

.PHONY: lint
lint:
	cd . && golangci-lint run --fix
	cd cmd/river && golangci-lint run --fix
	cd riverdriver && golangci-lint run --fix
	cd riverdriver/riverdatabasesql && golangci-lint run --fix
	cd riverdriver/riverpgxv5 && golangci-lint run --fix
	cd rivertype && golangci-lint run --fix

.PHONY: test
test:
	cd . && go test ./... -p 1
	cd cmd/river && go test ./...
	cd riverdriver && go test ./...
	cd riverdriver/riverdatabasesql && go test ./...
	cd riverdriver/riverpgxv5 && go test ./...
	cd rivertype && go test ./...

.PHONY: verify
verify:
verify: verify/sqlc

.PHONY: verify/sqlc
verify/sqlc:
	cd riverdriver/riverdatabasesql/internal/dbsqlc && sqlc diff
	cd riverdriver/riverpgxv5/internal/dbsqlc && sqlc diff