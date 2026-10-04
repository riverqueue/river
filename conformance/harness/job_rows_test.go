//go:build riverconformance

package harness_test

// rawJobRow is a job's JSON and timestamp columns as the database renders
// them (the raw_job_row method).
type rawJobRow struct {
	Args        string  `json:"args"`
	AttemptedAt *string `json:"attempted_at"`
	AttemptedBy *string `json:"attempted_by"`
	CreatedAt   string  `json:"created_at"`
	Errors      *string `json:"errors"`
	FinalizedAt *string `json:"finalized_at"`
	// JSONB holds SQLite's stored JSONB bytes as hex, and is nil on
	// PostgreSQL.
	JSONB *struct {
		Args        string  `json:"args"`
		AttemptedBy *string `json:"attempted_by"`
		Errors      *string `json:"errors"`
		Metadata    string  `json:"metadata"`
		Tags        string  `json:"tags"`
	} `json:"jsonb"`
	Metadata    string `json:"metadata"`
	ScheduledAt string `json:"scheduled_at"`
	Tags        string `json:"tags"`
	// UniqueKey is the stored unique key as uppercase hex.
	UniqueKey *string `json:"unique_key"`
	// UniqueKeyType is SQLite's typeof(unique_key), and nil on PostgreSQL.
	UniqueKeyType *string `json:"unique_key_type"`
	// UniqueStates is the stored state mask rendered as text.
	UniqueStates *string `json:"unique_states"`
	// UniqueStatesType is SQLite's typeof(unique_states), and nil on
	// PostgreSQL.
	UniqueStatesType *string `json:"unique_states_type"`
}
