//go:build foundationdb

package riverfdb

import (
	"bytes"
	"context"
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

func (e *executor) QueueCreateOrSetUpdatedAt(ctx context.Context, params *riverdriver.QueueCreateOrSetUpdatedAtParams) (*rivertype.Queue, error) {
	return transact(ctx, e, func(tx fdb.Transaction) (*rivertype.Queue, error) {
		key := e.driver.key(params.Schema, "queue", params.Name)
		queue, err := readJSON[rivertype.Queue](tx, key)
		now := timeOrNow(params.Now)
		if errors.Is(err, rivertype.ErrNotFound) {
			queue = &rivertype.Queue{CreatedAt: now, Metadata: defaultMetadata(params.Metadata), Name: params.Name, PausedAt: params.PausedAt}
		} else if err != nil {
			return nil, err
		}
		queue.UpdatedAt = now
		if params.UpdatedAt != nil {
			queue.UpdatedAt = params.UpdatedAt.UTC()
		}
		return queue, writeJSON(tx, key, queue)
	})
}

func (e *executor) QueueDeleteExpired(ctx context.Context, params *riverdriver.QueueDeleteExpiredParams) ([]string, error) {
	return transact(ctx, e, func(tx fdb.Transaction) ([]string, error) {
		queues, err := scanJSON[rivertype.Queue](tx, e.driver.key(params.Schema, "queue"))
		if err != nil {
			return nil, err
		}
		names := make([]string, 0)
		for _, queue := range queues {
			if len(names) >= params.Max {
				break
			}
			if queue.UpdatedAt.Before(params.UpdatedAtHorizon) {
				tx.Clear(e.driver.key(params.Schema, "queue", queue.Name))
				names = append(names, queue.Name)
			}
		}
		return names, nil
	})
}

func (e *executor) QueueGet(ctx context.Context, params *riverdriver.QueueGetParams) (*rivertype.Queue, error) {
	return transact(ctx, e, func(tx fdb.Transaction) (*rivertype.Queue, error) {
		return readJSON[rivertype.Queue](tx, e.driver.key(params.Schema, "queue", params.Name))
	})
}

func (e *executor) QueueList(ctx context.Context, params *riverdriver.QueueListParams) ([]*rivertype.Queue, error) {
	return transact(ctx, e, func(tx fdb.Transaction) ([]*rivertype.Queue, error) {
		queues, err := scanJSON[rivertype.Queue](tx, e.driver.key(params.Schema, "queue"))
		if err != nil {
			return nil, err
		}
		return queues[:min(len(queues), max(0, params.Max))], nil
	})
}

func (e *executor) QueueNameList(ctx context.Context, params *riverdriver.QueueNameListParams) ([]string, error) {
	return transact(ctx, e, func(tx fdb.Transaction) ([]string, error) {
		queues, err := scanJSON[rivertype.Queue](tx, e.driver.key(params.Schema, "queue"))
		if err != nil {
			return nil, err
		}
		names := make([]string, 0, len(queues))
		for _, queue := range queues {
			names = append(names, queue.Name)
		}
		return filterNames(names, params.After, params.Match, params.Exclude, params.Max), nil
	})
}

func (e *executor) QueuePause(ctx context.Context, params *riverdriver.QueuePauseParams) error {
	return e.updateQueues(ctx, params.Schema, params.Name, func(queue *rivertype.Queue) {
		if queue.PausedAt == nil {
			queue.PausedAt = new(timeOrNow(params.Now))
			queue.UpdatedAt = *queue.PausedAt
		}
	})
}

func (e *executor) QueueResume(ctx context.Context, params *riverdriver.QueueResumeParams) error {
	return e.updateQueues(ctx, params.Schema, params.Name, func(queue *rivertype.Queue) {
		if queue.PausedAt != nil {
			queue.PausedAt = nil
			queue.UpdatedAt = timeOrNow(params.Now)
		}
	})
}

func (e *executor) QueueUpdate(ctx context.Context, params *riverdriver.QueueUpdateParams) (*rivertype.Queue, error) {
	return transact(ctx, e, func(tx fdb.Transaction) (*rivertype.Queue, error) {
		key := e.driver.key(params.Schema, "queue", params.Name)
		queue, err := readJSON[rivertype.Queue](tx, key)
		if err != nil {
			return nil, err
		}
		if params.MetadataDoUpdate {
			queue.Metadata = bytes.Clone(params.Metadata)
		}
		queue.UpdatedAt = timeOrNow(nil)
		return queue, writeJSON(tx, key, queue)
	})
}

func (e *executor) updateQueues(ctx context.Context, schema, name string, updateFunc func(*rivertype.Queue)) error {
	_, err := transact(ctx, e, func(tx fdb.Transaction) (struct{}, error) {
		var queues []*rivertype.Queue
		if name == riverdriver.AllQueuesString {
			var err error
			queues, err = scanJSON[rivertype.Queue](tx, e.driver.key(schema, "queue"))
			if err != nil {
				return struct{}{}, err
			}
		} else {
			queue, err := readJSON[rivertype.Queue](tx, e.driver.key(schema, "queue", name))
			if err != nil {
				return struct{}{}, err
			}
			queues = []*rivertype.Queue{queue}
		}
		for _, queue := range queues {
			updateFunc(queue)
			if err := writeJSON(tx, e.driver.key(schema, "queue", queue.Name), queue); err != nil {
				return struct{}{}, err
			}
		}
		return struct{}{}, nil
	})
	return err
}
