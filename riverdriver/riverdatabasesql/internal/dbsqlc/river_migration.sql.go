// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.22.0
// source: river_migration.sql

package dbsqlc

import (
	"context"

	"github.com/lib/pq"
)

const riverMigrationDeleteByVersionMany = `-- name: RiverMigrationDeleteByVersionMany :many
DELETE FROM river_migration
WHERE version = any($1::bigint[])
RETURNING id, created_at, version
`

func (q *Queries) RiverMigrationDeleteByVersionMany(ctx context.Context, db DBTX, version []int64) ([]*RiverMigration, error) {
	rows, err := db.QueryContext(ctx, riverMigrationDeleteByVersionMany, pq.Array(version))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []*RiverMigration
	for rows.Next() {
		var i RiverMigration
		if err := rows.Scan(&i.ID, &i.CreatedAt, &i.Version); err != nil {
			return nil, err
		}
		items = append(items, &i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const riverMigrationGetAll = `-- name: RiverMigrationGetAll :many
SELECT id, created_at, version
FROM river_migration
ORDER BY version
`

func (q *Queries) RiverMigrationGetAll(ctx context.Context, db DBTX) ([]*RiverMigration, error) {
	rows, err := db.QueryContext(ctx, riverMigrationGetAll)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []*RiverMigration
	for rows.Next() {
		var i RiverMigration
		if err := rows.Scan(&i.ID, &i.CreatedAt, &i.Version); err != nil {
			return nil, err
		}
		items = append(items, &i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const riverMigrationInsert = `-- name: RiverMigrationInsert :one
INSERT INTO river_migration (
  version
) VALUES (
  $1
) RETURNING id, created_at, version
`

func (q *Queries) RiverMigrationInsert(ctx context.Context, db DBTX, version int64) (*RiverMigration, error) {
	row := db.QueryRowContext(ctx, riverMigrationInsert, version)
	var i RiverMigration
	err := row.Scan(&i.ID, &i.CreatedAt, &i.Version)
	return &i, err
}

const riverMigrationInsertMany = `-- name: RiverMigrationInsertMany :many
INSERT INTO river_migration (
  version
)
SELECT
  unnest($1::bigint[])
RETURNING id, created_at, version
`

func (q *Queries) RiverMigrationInsertMany(ctx context.Context, db DBTX, version []int64) ([]*RiverMigration, error) {
	rows, err := db.QueryContext(ctx, riverMigrationInsertMany, pq.Array(version))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []*RiverMigration
	for rows.Next() {
		var i RiverMigration
		if err := rows.Scan(&i.ID, &i.CreatedAt, &i.Version); err != nil {
			return nil, err
		}
		items = append(items, &i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const tableExists = `-- name: TableExists :one
SELECT CASE WHEN to_regclass($1) IS NULL THEN false
            ELSE true END
`

func (q *Queries) TableExists(ctx context.Context, db DBTX, tableName string) (bool, error) {
	row := db.QueryRowContext(ctx, tableExists, tableName)
	var column_1 bool
	err := row.Scan(&column_1)
	return column_1, err
}
