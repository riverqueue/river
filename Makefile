.PHONY: generate
generate:
generate: generate/migrations
generate: generate/sqlc

.PHONY: generate/migrations
generate/migrations: ## sync changes of pgxv5 migrations to database/sql
	rsync -au --delete "riverdriver/riverpgxv5/migration/" "riverdriver/riverdatabasesql/migration/"

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
	cd rivershared && golangci-lint run --fix
	cd rivertype && golangci-lint run --fix

.PHONY: test
test:
	cd . && go test ./... -p 1
	cd cmd/river && go test ./...
	cd riverdriver && go test ./...
	cd riverdriver/riverdatabasesql && go test ./...
	cd riverdriver/riverpgxv5 && go test ./...
	cd rivershared && go test ./...
	cd rivertype && go test ./...

.PHONY: tidy
tidy:
	cd . && go mod tidy
	cd cmd/river && go mod tidy
	cd riverdriver && go mod tidy
	cd riverdriver/riverdatabasesql && go mod tidy
	cd riverdriver/riverpgxv5 && go mod tidy
	cd rivertype && go mod tidy

.PHONY: verify
verify:
verify: verify/migrations
verify: verify/sqlc

.PHONY: verify/migrations
verify/migrations:
	diff -qr riverdriver/riverpgxv5/migration riverdriver/riverdatabasesql/migration

.PHONY: verify/sqlc
verify/sqlc:
	cd riverdriver/riverdatabasesql/internal/dbsqlc && sqlc diff
	cd riverdriver/riverpgxv5/internal/dbsqlc && sqlc diff