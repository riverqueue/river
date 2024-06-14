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
func NewLimiter(archetype *baseservice.Archetype, waitDuration time.Duration) *Limiter {
	return baseservice.Init(archetype, &Limiter{
		lastSentByTopic: make(map[string]time.Time),
		waitDuration:    waitDuration,
	})
}

func (l *Limiter) ShouldTrigger(topic string) bool {
	// Calculate this beforehand to reduce mutex duration.
	now := l.Time.NowUTC()
	lastSentHorizon := now.Add(-l.waitDuration)

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.lastSentByTopic[topic].Before(lastSentHorizon) {
		l.lastSentByTopic[topic] = now
		return true
	}

	return false
}
