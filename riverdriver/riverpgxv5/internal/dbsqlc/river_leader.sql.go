// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.25.0
// source: river_leader.sql

package dbsqlc

import (
	"context"
	"time"
)

const leaderAttemptElect = `-- name: LeaderAttemptElect :execrows
INSERT INTO river_leader(name, leader_id, elected_at, expires_at)
    VALUES ($1::text, $2::text, now(), now() + $3::interval)
ON CONFLICT (name)
    DO NOTHING
`

type LeaderAttemptElectParams struct {
	Name     string
	LeaderID string
	TTL      time.Duration
}

func (q *Queries) LeaderAttemptElect(ctx context.Context, db DBTX, arg *LeaderAttemptElectParams) (int64, error) {
	result, err := db.Exec(ctx, leaderAttemptElect, arg.Name, arg.LeaderID, arg.TTL)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

const leaderAttemptReelect = `-- name: LeaderAttemptReelect :execrows
INSERT INTO river_leader(name, leader_id, elected_at, expires_at)
    VALUES ($1::text, $2::text, now(), now() + $3::interval)
ON CONFLICT (name)
    DO UPDATE SET
        expires_at = now() + $3::interval
    WHERE
        river_leader.leader_id = $2::text
`

type LeaderAttemptReelectParams struct {
	Name     string
	LeaderID string
	TTL      time.Duration
}

func (q *Queries) LeaderAttemptReelect(ctx context.Context, db DBTX, arg *LeaderAttemptReelectParams) (int64, error) {
	result, err := db.Exec(ctx, leaderAttemptReelect, arg.Name, arg.LeaderID, arg.TTL)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

const leaderDeleteExpired = `-- name: LeaderDeleteExpired :execrows
DELETE FROM river_leader
WHERE name = $1::text
    AND expires_at < now()
`

func (q *Queries) LeaderDeleteExpired(ctx context.Context, db DBTX, name string) (int64, error) {
	result, err := db.Exec(ctx, leaderDeleteExpired, name)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

const leaderGetElectedLeader = `-- name: LeaderGetElectedLeader :one
SELECT elected_at, expires_at, leader_id, name
FROM river_leader
WHERE name = $1
`

func (q *Queries) LeaderGetElectedLeader(ctx context.Context, db DBTX, name string) (*RiverLeader, error) {
	row := db.QueryRow(ctx, leaderGetElectedLeader, name)
	var i RiverLeader
	err := row.Scan(
		&i.ElectedAt,
		&i.ExpiresAt,
		&i.LeaderID,
		&i.Name,
	)
	return &i, err
}

const leaderInsert = `-- name: LeaderInsert :one
INSERT INTO river_leader(
    elected_at,
    expires_at,
    leader_id,
    name
) VALUES (
    coalesce($1::timestamptz, now()),
    coalesce($2::timestamptz, now() + $3::interval),
    $4,
    $5
) RETURNING elected_at, expires_at, leader_id, name
`

type LeaderInsertParams struct {
	ElectedAt *time.Time
	ExpiresAt *time.Time
	TTL       time.Duration
	LeaderID  string
	Name      string
}

func (q *Queries) LeaderInsert(ctx context.Context, db DBTX, arg *LeaderInsertParams) (*RiverLeader, error) {
	row := db.QueryRow(ctx, leaderInsert,
		arg.ElectedAt,
		arg.ExpiresAt,
		arg.TTL,
		arg.LeaderID,
		arg.Name,
	)
	var i RiverLeader
	err := row.Scan(
		&i.ElectedAt,
		&i.ExpiresAt,
		&i.LeaderID,
		&i.Name,
	)
	return &i, err
}

const leaderResign = `-- name: LeaderResign :execrows
WITH currently_held_leaders AS (
  SELECT elected_at, expires_at, leader_id, name
  FROM river_leader
  WHERE
      name = $1::text
      AND leader_id = $2::text
  FOR UPDATE
),
notified_resignations AS (
  SELECT
      pg_notify($3, json_build_object('name', name, 'leader_id', leader_id, 'action', 'resigned')::text),
      currently_held_leaders.name
  FROM currently_held_leaders
)
DELETE FROM river_leader USING notified_resignations
WHERE river_leader.name = notified_resignations.name
`

type LeaderResignParams struct {
	Name            string
	LeaderID        string
	LeadershipTopic string
}

func (q *Queries) LeaderResign(ctx context.Context, db DBTX, arg *LeaderResignParams) (int64, error) {
	result, err := db.Exec(ctx, leaderResign, arg.Name, arg.LeaderID, arg.LeadershipTopic)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}
