package dbsqlc

import (
	"weavelab.xyz/river/internal/util/sliceutil"
	"weavelab.xyz/river/internal/util/valutil"
	"weavelab.xyz/river/rivertype"
)

func JobRowFromInternal(internal *RiverJob) *rivertype.JobRow {
	return &rivertype.JobRow{
		ID:          internal.ID,
		Attempt:     valutil.Max(int(internal.Attempt), 0),
		AttemptedAt: internal.AttemptedAt,
		AttemptedBy: internal.AttemptedBy,
		CreatedAt:   internal.CreatedAt,
		EncodedArgs: internal.Args,
		Errors:      sliceutil.Map(internal.Errors, func(e AttemptError) rivertype.AttemptError { return AttemptErrorFromInternal(&e) }),
		FinalizedAt: internal.FinalizedAt,
		Kind:        internal.Kind,
		MaxAttempts: valutil.Max(int(internal.MaxAttempts), 0),
		Metadata:    internal.Metadata,
		Priority:    valutil.Max(int(internal.Priority), 0),
		Queue:       internal.Queue,
		ScheduledAt: internal.ScheduledAt.UTC(), // TODO(brandur): Very weird this is the only place a UTC conversion happens.
		State:       rivertype.JobState(internal.State),
		Tags:        internal.Tags,
	}
}

func JobRowsFromInternal(internal []*RiverJob) []*rivertype.JobRow {
	rows := make([]*rivertype.JobRow, len(internal))
	for i, j := range internal {
		rows[i] = JobRowFromInternal(j)
	}
	return rows
}

func AttemptErrorFromInternal(e *AttemptError) rivertype.AttemptError {
	return rivertype.AttemptError{
		At:      e.At,
		Attempt: int(e.Attempt),
		Error:   e.Error,
		Trace:   e.Trace,
	}
}
