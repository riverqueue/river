package dbsqlc

import (
	"github.com/riverqueue/river/internal/rivertype"
)

func JobRowFromInternal(internal *RiverJob) *rivertype.JobRow {
	tags := internal.Tags
	if tags == nil {
		tags = []string{}
	}
	return &rivertype.JobRow{
		ID:          internal.ID,
		Attempt:     max(int(internal.Attempt), 0),
		AttemptedAt: internal.AttemptedAt,
		AttemptedBy: internal.AttemptedBy,
		CreatedAt:   internal.CreatedAt,
		EncodedArgs: internal.Args,
		Errors:      internal.Errors,
		FinalizedAt: internal.FinalizedAt,
		Kind:        internal.Kind,
		MaxAttempts: max(int(internal.MaxAttempts), 0),
		Priority:    max(int(internal.Priority), 0),
		Queue:       internal.Queue,
		ScheduledAt: internal.ScheduledAt.UTC(), // TODO(brandur): Very weird this is the only place a UTC conversion happens.
		State:       string(internal.State),
		Tags:        tags,

		// metadata: internal.Metadata,
	}
}
