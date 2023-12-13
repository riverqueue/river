.PHONY: generate
generate:
generate: generate/sqlc

.PHONY: generate/sqlc
generate/sqlc:
	cd internal/dbsqlc && sqlc generate
	cd riverdriver/riverdatabasesql/internal/dbsqlc && sqlc generate
	cd riverdriver/riverpgxv5/internal/dbsqlc && sqlc generate

.PHONY: verify
verify:
verify: verify/sqlc

.PHONY: verify/sqlc
verify/sqlc:
	cd internal/dbsqlc && sqlc diff
	cd riverdriver/riverdatabasesql/internal/dbsqlc && sqlc diff
	cd riverdriver/riverpgxv5/internal/dbsqlc && sqlc diff