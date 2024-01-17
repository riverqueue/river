// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.25.0

package dbsqlc

import (
	"database/sql/driver"
	"fmt"
	"time"
)

type RiverJobState string

const (
	RiverJobStateAvailable RiverJobState = "available"
	RiverJobStateCancelled RiverJobState = "cancelled"
	RiverJobStateCompleted RiverJobState = "completed"
	RiverJobStateDiscarded RiverJobState = "discarded"
	RiverJobStatePending   RiverJobState = "pending"
	RiverJobStateRetryable RiverJobState = "retryable"
	RiverJobStateRunning   RiverJobState = "running"
	RiverJobStateScheduled RiverJobState = "scheduled"
)

func (e *RiverJobState) Scan(src interface{}) error {
	switch s := src.(type) {
	case []byte:
		*e = RiverJobState(s)
	case string:
		*e = RiverJobState(s)
	default:
		return fmt.Errorf("unsupported scan type for RiverJobState: %T", src)
	}
	return nil
}

type NullRiverJobState struct {
	RiverJobState RiverJobState
	Valid         bool // Valid is true if RiverJobState is not NULL
}

// Scan implements the Scanner interface.
func (ns *NullRiverJobState) Scan(value interface{}) error {
	if value == nil {
		ns.RiverJobState, ns.Valid = "", false
		return nil
	}
	ns.Valid = true
	return ns.RiverJobState.Scan(value)
}

// Value implements the driver Valuer interface.
func (ns NullRiverJobState) Value() (driver.Value, error) {
	if !ns.Valid {
		return nil, nil
	}
	return string(ns.RiverJobState), nil
}

type RiverJob struct {
	ID          int64
	Args        []byte
	Attempt     int16
	AttemptedAt *time.Time
	AttemptedBy []string
	CreatedAt   time.Time
	Errors      []AttemptError
	FinalizedAt *time.Time
	Kind        string
	MaxAttempts int16
	Metadata    []byte
	Priority    int16
	Queue       string
	State       RiverJobState
	ScheduledAt time.Time
	Tags        []string
}

type RiverLeader struct {
	ElectedAt time.Time
	ExpiresAt time.Time
	LeaderID  string
	Name      string
}

type RiverMigration struct {
	ID        int64
	CreatedAt time.Time
	Version   int64
}
