package clientconfig

import (
	"time"

	"github.com/riverqueue/river/internal/dbunique"
)

// InsertOpts is a mirror of river.InsertOpts usable by other internal packages.
type InsertOpts struct {
	MaxAttempts int
	Metadata    []byte
	Pending     bool
	Priority    int
	Queue       string
	ScheduledAt time.Time
	Tags        []string
	UniqueOpts  dbunique.UniqueOpts
}
