package notifylimiter

import (
	"sync"
	"time"

	"github.com/riverqueue/river/internal/baseservice"
)

type NotifyFunc func(name string)

type Limiter struct {
	baseservice.BaseService

	waitDuration time.Duration

	mu              sync.Mutex // protects lastSentByTopic
	lastSentByTopic map[string]time.Time
}

// NewLimiter creates a new Limiter, calling the NotifyFunc no more than once per waitDuration.
// The function must be fast as it is called within a mutex.
//
// TODO NOW NEXT: need to rework this so that it's not calling a _function_, but
// rather returning a bool IF it's time for that topic to notify. That way the
// caller can decide if it needs to piggy-back the pg_notify onto the current
// query.
func NewLimiter(archetype *baseservice.Archetype, waitDuration time.Duration) *Limiter {
	return baseservice.Init(archetype, &Limiter{
		lastSentByTopic: make(map[string]time.Time),
		waitDuration:    waitDuration,
	})
}

func (l *Limiter) ShouldTrigger(topic string) bool {
	// Calculate this beforehand to reduce mutex duration.
	now := l.TimeNowUTC()
	lastSentHorizon := now.Add(-l.waitDuration)

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.lastSentByTopic[topic].Before(lastSentHorizon) {
		l.lastSentByTopic[topic] = now
		return true
	}

	return false
}
