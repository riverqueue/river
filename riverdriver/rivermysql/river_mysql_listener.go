package rivermysql

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/rivermysql/internal/dbsqlc"
	"github.com/riverqueue/river/rivershared/sqlctemplate"
)

const (
	notificationPollIntervalDefault = 50 * time.Millisecond
)

// Listener receives MySQL notifications from the river_notification outbox
// table. MySQL doesn't have a native LISTEN/NOTIFY equivalent, so NotifyMany
// appends rows to river_notification and this listener polls for rows with IDs
// greater than its remembered lastID. Polls use a locking read because InnoDB
// may commit auto-increment IDs out of order across concurrent transactions; a
// normal consistent read could otherwise skip a lower uncommitted notification.
type Listener struct {
	afterConnectExec string // should only ever be used in testing
	dbPool           *sql.DB
	isConnected      bool
	lastID           int64
	mu               sync.Mutex
	pollInterval     time.Duration
	replacer         *sqlctemplate.Replacer
	schema           string
	topics           map[string]struct{}
}

func (l *Listener) Close(context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.isConnected = false
	return nil
}

func (l *Listener) Connect(ctx context.Context) error {
	var (
		afterConnectExec string
		dbPool           *sql.DB
		replacer         *sqlctemplate.Replacer
		schema           string
	)

	l.mu.Lock()
	if l.isConnected {
		l.mu.Unlock()
		return errors.New("connection already established")
	}
	afterConnectExec = l.afterConnectExec
	dbPool = l.dbPool
	replacer = l.replacer
	schema = l.schema
	l.mu.Unlock()

	if dbPool == nil {
		return errors.New("database pool is nil")
	}
	if replacer == nil {
		replacer = &sqlctemplate.Replacer{UnnumberedPlaceholders: true}
	}

	if afterConnectExec != "" {
		if _, err := dbPool.ExecContext(ctx, afterConnectExec); err != nil {
			return err
		}
	}

	lastID, err := dbsqlc.New().NotificationGetLastID(schemaTemplateParam(ctx, schema), templateReplaceWrapper{dbPool, replacer})
	if err != nil {
		return err
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.isConnected {
		return errors.New("connection already established")
	}

	l.isConnected = true
	l.lastID = lastID

	return nil
}

func (l *Listener) Listen(_ context.Context, topic string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.isConnected {
		return errors.New("listener is not connected")
	}

	if l.topics == nil {
		l.topics = make(map[string]struct{})
	}

	l.topics[topic] = struct{}{}
	return nil
}

func (l *Listener) Ping(ctx context.Context) error {
	dbPool, err := l.stateDBPool()
	if err != nil {
		return err
	}
	return dbPool.PingContext(ctx)
}

func (l *Listener) Schema() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.schema
}

func (l *Listener) SetAfterConnectExec(sql string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.afterConnectExec = sql
}

func (l *Listener) Unlisten(_ context.Context, topic string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.isConnected {
		return errors.New("listener is not connected")
	}

	delete(l.topics, topic)
	return nil
}

func (l *Listener) WaitForNotification(ctx context.Context) (*riverdriver.Notification, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		notification, found, err := l.waitForNotificationOnce(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			if err := l.waitForNextPoll(ctx); err != nil {
				return nil, err
			}
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, err
		}
		if found {
			return notification, nil
		}
	}
}

func (l *Listener) stateDBPool() (*sql.DB, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.isConnected {
		return nil, errors.New("listener is not connected")
	}
	if l.dbPool == nil {
		return nil, errors.New("database pool is nil")
	}

	return l.dbPool, nil
}

func (l *Listener) waitForNextPoll(ctx context.Context) error {
	l.mu.Lock()
	pollInterval := l.pollInterval
	l.mu.Unlock()

	if pollInterval <= 0 {
		pollInterval = notificationPollIntervalDefault
	}

	timer := time.NewTimer(pollInterval)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (l *Listener) waitForNotificationOnce(ctx context.Context) (*riverdriver.Notification, bool, error) {
	var (
		after    int64
		dbPool   *sql.DB
		replacer *sqlctemplate.Replacer
		schema   string
	)

	l.mu.Lock()
	if !l.isConnected {
		l.mu.Unlock()
		return nil, false, errors.New("listener is not connected")
	}
	after = l.lastID
	dbPool = l.dbPool
	replacer = l.replacer
	schema = l.schema
	l.mu.Unlock()

	if dbPool == nil {
		return nil, false, errors.New("database pool is nil")
	}

	tx, err := dbPool.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback()

	notification, err := dbsqlc.New().NotificationGetAfterForUpdate(
		schemaTemplateParam(ctx, schema),
		notificationDBTX(tx, replacer),
		after,
	)
	if err != nil {
		return nil, false, err
	}

	if err := tx.Commit(); err != nil {
		return nil, false, err
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if notification.ID > l.lastID {
		l.lastID = notification.ID
	}

	if _, ok := l.topics[notification.Topic]; !ok {
		return nil, false, nil
	}

	return &riverdriver.Notification{
		Payload: notification.Payload,
		Topic:   notification.Topic,
	}, true, nil
}

func notificationDBTX(dbtx dbsqlc.DBTX, replacer *sqlctemplate.Replacer) templateReplaceWrapper {
	if replacer == nil {
		replacer = &sqlctemplate.Replacer{UnnumberedPlaceholders: true}
	}
	return templateReplaceWrapper{dbtx, replacer}
}
