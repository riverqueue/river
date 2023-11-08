package dbsqlc

import "time"

type AttemptError struct {
	At    time.Time `json:"at"`
	Error string    `json:"error"`
	Num   uint16    `json:"num"`
	Trace string    `json:"trace"`
}
