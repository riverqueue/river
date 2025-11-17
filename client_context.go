package river

import (
	"context"
	"errors"

	"github.com/riverqueue/river/internal/rivercommon"
)

var errClientNotInContext = errors.New("river: client not found in context, can only be used in a Worker")

func withClient[TTx any](ctx context.Context, client *Client[TTx]) context.Context {
	return context.WithValue(ctx, rivercommon.ContextKeyClient{}, client)
}

// ClientFromContext returns the Client from the context. This function can
// only be used within a Worker's Work() method because that is the only place
// River sets the Client on the context.
//
// It panics if the context does not contain a Client, which will never happen
// from the context provided to a Worker's Work() method.
//
// When testing JobArgs.Work implementations, it might be useful to use
// rivertest.WorkContext to initialize a context that has an available client.
//
// The type parameter TTx is the transaction type used by the [Client],
// pgx.Tx for the pgx driver, and *sql.Tx for the [database/sql] driver.
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
//
// When testing JobArgs.Work implementations, it might be useful to use
// rivertest.WorkContext to initialize a context that has an available client.
//
// See the examples for [ClientFromContext] to understand how to use this
// function.
func ClientFromContextSafely[TTx any](ctx context.Context) (*Client[TTx], error) {
	client, exists := ctx.Value(rivercommon.ContextKeyClient{}).(*Client[TTx])
	if !exists || client == nil {
		return nil, errClientNotInContext
	}
	return client, nil
}
