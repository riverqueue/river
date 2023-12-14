package dbsqlc

import "time"

type AttemptError struct {
	At      time.Time `json:"at"`
	Attempt uint16    `json:"attempt"`
	Error   string    `json:"error"`
	Trace   string    `json:"trace"`
}
