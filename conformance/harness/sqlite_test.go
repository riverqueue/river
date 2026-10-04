//go:build riverconformance

package harness_test

// rawNotification is one SQLite outbox row as `raw_notifications` returns it.
type rawNotification struct {
	ID          int64  `json:"id"`
	Payload     string `json:"payload"`
	PayloadType string `json:"payload_type"`
	Topic       string `json:"topic"`
}
