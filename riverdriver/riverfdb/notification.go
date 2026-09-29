//go:build foundationdb

package riverfdb

import (
	"context"
	"errors"
	"math"
	"strconv"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/riverqueue/river/riverdriver"
)

func (e *executor) NotificationDeleteBefore(ctx context.Context, params *riverdriver.NotificationDeleteBeforeParams) (int, error) {
	return transact(ctx, e, func(tx fdb.Transaction) (int, error) {
		if params.Max <= 0 {
			return 0, nil
		}
		entries, err := tx.GetRange(fdb.KeyRange{
			Begin: e.driver.key(params.Schema, "notification_expiry"),
			End:   e.driver.key(params.Schema, "notification_expiry", params.CreatedAtHorizon.Unix(), params.CreatedAtHorizon.Nanosecond()),
		}, fdb.RangeOptions{Limit: params.Max}).GetSliceWithError()
		if err != nil {
			return 0, err
		}
		for _, entry := range entries {
			tx.Clear(fdb.Key(entry.Value))
			tx.Clear(entry.Key)
		}
		// Keep the sequence key: subscriptions must never replay old buffered
		// messages or reuse IDs, even after the entire log has been deleted.
		return len(entries), nil
	})
}

func (e *executor) NotifyMany(ctx context.Context, params *riverdriver.NotifyManyParams) error {
	_, err := transact(ctx, e, func(tx fdb.Transaction) (struct{}, error) {
		return struct{}{}, e.notifyMany(tx, params)
	})
	return err
}

func (e *executor) notifyMany(tx fdb.Transaction, params *riverdriver.NotifyManyParams) error {
	if len(params.Payload) == 0 {
		return nil
	}
	sequenceKey := e.driver.key(params.Schema, "notification_sequence")
	id, err := notificationSequence(tx, sequenceKey)
	if err != nil {
		return err
	}
	if int64(len(params.Payload)) > math.MaxInt64-id {
		return errors.New("riverfdb: notification sequence exhausted")
	}
	createdAt := time.Now().UTC()
	for _, payload := range params.Payload {
		id++
		key := e.driver.key(params.Schema, "notification", id)
		if err := writeJSON(tx, key, &notificationRecord{ID: id, CreatedAt: createdAt, Payload: payload, Topic: params.Topic}); err != nil {
			return err
		}
		tx.Set(e.driver.key(params.Schema, "notification_expiry", createdAt.Unix(), createdAt.Nanosecond(), id), key)
	}
	// This normal read/write deliberately serializes publishers per schema.
	// Conflicting publishers retry, so IDs follow commit order. The sequence
	// also serves as the watched key and never returns to an earlier value.
	tx.Set(sequenceKey, []byte(strconv.FormatInt(id, 10)))
	return nil
}

type notificationRecord struct {
	ID        int64     `json:"id"`
	CreatedAt time.Time `json:"created_at"`
	Payload   string    `json:"payload"`
	Topic     string    `json:"topic"`
}

func notificationSequence(tx fdb.Transaction, key fdb.Key) (int64, error) {
	data, err := tx.Get(key).Get()
	if err != nil || data == nil {
		return 0, err
	}
	return strconv.ParseInt(string(data), 10, 64)
}
