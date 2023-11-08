.PHONY: generate
generate:
generate: generate/sqlc

.PHONY: generate/sqlc
generate/sqlc:
	cd internal/dbsqlc && sqlc generate