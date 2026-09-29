//go:build foundationdb

package riverfdb

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

func (e *executor) LeaderAttemptElect(ctx context.Context, params *riverdriver.LeaderElectParams) (*riverdriver.Leader, error) {
	return transact(ctx, e, func(tx fdb.Transaction) (*riverdriver.Leader, error) {
		key := e.driver.key(params.Schema, "leader")
		_, err := readJSON[riverdriver.Leader](tx, key)
		if err == nil {
			return nil, rivertype.ErrNotFound
		}
		if !errors.Is(err, rivertype.ErrNotFound) {
			return nil, err
		}
		now := timeOrNow(params.Now)
		leader := &riverdriver.Leader{ElectedAt: now, ExpiresAt: now.Add(params.TTL), LeaderID: params.LeaderID}
		return leader, writeJSON(tx, key, leader)
	})
}

func (e *executor) LeaderAttemptReelect(ctx context.Context, params *riverdriver.LeaderReelectParams) (*riverdriver.Leader, error) {
	return transact(ctx, e, func(tx fdb.Transaction) (*riverdriver.Leader, error) {
		key := e.driver.key(params.Schema, "leader")
		leader, err := readJSON[riverdriver.Leader](tx, key)
		if err != nil {
			return nil, err
		}
		now := timeOrNow(params.Now)
		if leader.LeaderID != params.LeaderID || !leader.ElectedAt.Equal(params.ElectedAt) || leader.ExpiresAt.Before(now) {
			return nil, rivertype.ErrNotFound
		}
		leader.ExpiresAt = now.Add(params.TTL)
		return leader, writeJSON(tx, key, leader)
	})
}

func (e *executor) LeaderDeleteExpired(ctx context.Context, params *riverdriver.LeaderDeleteExpiredParams) (int, error) {
	return transact(ctx, e, func(tx fdb.Transaction) (int, error) {
		key := e.driver.key(params.Schema, "leader")
		leader, err := readJSON[riverdriver.Leader](tx, key)
		if errors.Is(err, rivertype.ErrNotFound) {
			return 0, nil
		}
		if err != nil {
			return 0, err
		}
		if !leader.ExpiresAt.Before(timeOrNow(params.Now)) {
			return 0, nil
		}
		tx.Clear(key)
		return 1, nil
	})
}

func (e *executor) LeaderGetElectedLeader(ctx context.Context, params *riverdriver.LeaderGetElectedLeaderParams) (*riverdriver.Leader, error) {
	return transact(ctx, e, func(tx fdb.Transaction) (*riverdriver.Leader, error) {
		return readJSON[riverdriver.Leader](tx, e.driver.key(params.Schema, "leader"))
	})
}

func (e *executor) LeaderInsert(ctx context.Context, params *riverdriver.LeaderInsertParams) (*riverdriver.Leader, error) {
	return transact(ctx, e, func(tx fdb.Transaction) (*riverdriver.Leader, error) {
		key := e.driver.key(params.Schema, "leader")
		_, err := readJSON[riverdriver.Leader](tx, key)
		if err == nil {
			return nil, errors.New("riverfdb: leader already exists")
		}
		if !errors.Is(err, rivertype.ErrNotFound) {
			return nil, err
		}
		now := timeOrNow(params.Now)
		leader := &riverdriver.Leader{ElectedAt: now, ExpiresAt: now.Add(params.TTL), LeaderID: params.LeaderID}
		if params.ElectedAt != nil {
			leader.ElectedAt = params.ElectedAt.UTC()
		}
		if params.ExpiresAt != nil {
			leader.ExpiresAt = params.ExpiresAt.UTC()
		}
		return leader, writeJSON(tx, key, leader)
	})
}

func (e *executor) LeaderResign(ctx context.Context, params *riverdriver.LeaderResignParams) (bool, error) {
	return transact(ctx, e, func(tx fdb.Transaction) (bool, error) {
		key := e.driver.key(params.Schema, "leader")
		leader, err := readJSON[riverdriver.Leader](tx, key)
		if errors.Is(err, rivertype.ErrNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if leader.LeaderID != params.LeaderID || !leader.ElectedAt.Equal(params.ElectedAt) {
			return false, nil
		}
		tx.Clear(key)
		payload, err := json.Marshal(map[string]string{"action": "resigned", "leader_id": leader.LeaderID})
		if err != nil {
			return false, err
		}
		return true, e.notifyMany(tx, &riverdriver.NotifyManyParams{Payload: []string{string(payload)}, Schema: params.Schema, Topic: params.LeadershipTopic})
	})
}
