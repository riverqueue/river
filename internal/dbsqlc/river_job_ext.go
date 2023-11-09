package dbsqlc

import (
	"github.com/riverqueue/river/internal/util/sliceutil"
	"github.com/riverqueue/river/rivertype"
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
		Errors:      sliceutil.Map(internal.Errors, func(e AttemptError) rivertype.AttemptError { return AttemptErrorFromInternal(&e) }),
		FinalizedAt: internal.FinalizedAt,
		Kind:        internal.Kind,
		MaxAttempts: max(int(internal.MaxAttempts), 0),
		Priority:    max(int(internal.Priority), 0),
		Queue:       internal.Queue,
		ScheduledAt: internal.ScheduledAt.UTC(), // TODO(brandur): Very weird this is the only place a UTC conversion happens.
		State:       rivertype.JobState(internal.State),
		Tags:        tags,

		// metadata: internal.Metadata,
	}
}

func AttemptErrorFromInternal(e *AttemptError) rivertype.AttemptError {
	return rivertype.AttemptError{
		At:    e.At,
		Error: e.Error,
		Num:   int(e.Num),
		Trace: e.Trace,
	}
}
