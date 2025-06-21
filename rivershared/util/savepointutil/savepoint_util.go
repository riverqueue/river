package savepointutil

import (
	"errors"
)

// BeginOnlyOnce is a small utility struct to help with safety. It's not
// strictly necessary, but designed to help us out problems when implementing
// savepoints in non-Pgx drivers (e.g. `database/sql` for Postgres or SQLite)
// where `Begin` might be called multiple times on the same subtransaction,
// which would silently produce the wrong result.
type BeginOnlyOnce struct {
	done            bool
	parent          *BeginOnlyOnce
	subTxInProgress bool
}

func NewBeginOnlyOnce(parent *BeginOnlyOnce) *BeginOnlyOnce {
	return &BeginOnlyOnce{
		parent: parent,
	}
}

func (t *BeginOnlyOnce) Begin() error {
	if t.subTxInProgress {
		return errors.New("subtransaction already in progress")
	}
	t.subTxInProgress = true
	return nil
}

func (t *BeginOnlyOnce) Done() {
	t.done = true
	if t.parent != nil {
		t.parent.subTxInProgress = false
	}
}

func (t *BeginOnlyOnce) IsDone() bool { return t.done }
