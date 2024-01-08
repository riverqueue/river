package river

import "github.com/riverqueue/river/internal/notifier"

// ListenSubscription is a subscription returned from the Client's Listen method.
type ListenSubscription struct {
	*notifier.Subscription
}

// Unlisten stops listening to this particular subscription.
func (sub *ListenSubscription) Unlisten() {
	sub.Subscription.Unlisten()
}

// NotifyFunc is a function that will be called any time a Postgres notification
// payload is receiverd on the specified topic. It must not block, or messages
// will be dropped (including those important to River's internal operation).
type NotifyFunc func(topic string, payload string)
