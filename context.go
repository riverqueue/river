package river

import (
	"context"
	"errors"
)

type ctxKey int

const (
	ctxKeyClient ctxKey = iota
)

var errClientNotInContext = errors.New("river: client not found in context, can only be used in a Worker")

func withClient[TTx any](ctx context.Context, client *Client[TTx]) context.Context {
	return context.WithValue(ctx, ctxKeyClient, client)
}

// ClientFromContext returns the Client from the context. This function can
// only be used within a Worker's Work() method because that is the only place
// River sets the Client on the context.
//
// It panics if the context does not contain a Client, which will never happen
// from the context provided to a Worker's Work() method.
func ClientFromContext[TTx any](ctx context.Context) *Client[TTx] {
	client, err := ClientFromContextSafely[TTx](ctx)
	if err != nil {
		panic(err)
	}
	return client
}

// ClientFromContext returns the Client from the context. This function can
// only be used within a Worker's Work() method because that is the only place
// River sets the Client on the context.
//
// It returns an error if the context does not contain a Client, which will
// never happen from the context provided to a Worker's Work() method.
func ClientFromContextSafely[TTx any](ctx context.Context) (*Client[TTx], error) {
	client, exists := ctx.Value(ctxKeyClient).(*Client[TTx])
	if !exists || client == nil {
		return nil, errClientNotInContext
	}
	return client, nil
}
