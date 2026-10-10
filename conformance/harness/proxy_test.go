package harness

import (
	"context"
	"io"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// faultProxy forwards TCP connections to Postgres and can make the
// database unavailable to the adapters behind it: it resets established
// connections and refuses new ones until restored. Unlike terminating
// backends, this keeps the database down for those adapters alone.
type faultProxy struct {
	down     atomic.Bool
	mu       sync.Mutex
	open     map[net.Conn]struct{}
	rejected atomic.Int64
	url      string
}

// startFaultProxy starts a proxy in front of databaseURL's server and
// returns it, with url pointing at the proxy.
func startFaultProxy(t *testing.T, databaseURL string) *faultProxy {
	t.Helper()

	parsed, err := url.Parse(databaseURL)
	require.NoError(t, err)
	upstream := parsed.Host
	if parsed.Port() == "" {
		upstream = net.JoinHostPort(parsed.Hostname(), "5432")
	}
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	proxied := *parsed
	proxied.Host = listener.Addr().String()
	proxy := &faultProxy{open: make(map[net.Conn]struct{}), url: proxied.String()}
	t.Cleanup(func() {
		_ = listener.Close()
		proxy.closeAll()
	})

	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			if proxy.down.Load() {
				proxy.rejected.Add(1)
				_ = client.Close()
				continue
			}
			go proxy.forward(client, upstream)
		}
	}()
	return proxy
}

func (p *faultProxy) forward(client net.Conn, upstream string) {
	server, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(context.Background(), "tcp", upstream)
	if err != nil {
		_ = client.Close()
		return
	}
	if !p.track(client, server) {
		return
	}
	done := make(chan struct{}, 2)
	pipe := func(destination, source net.Conn) {
		_, _ = io.Copy(destination, source)
		done <- struct{}{}
	}
	go pipe(server, client)
	go pipe(client, server)
	<-done
	p.untrack(client, server)
}

func (p *faultProxy) track(connections ...net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.down.Load() {
		for _, connection := range connections {
			_ = connection.Close()
		}
		return false
	}
	for _, connection := range connections {
		p.open[connection] = struct{}{}
	}
	return true
}

func (p *faultProxy) untrack(connections ...net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, connection := range connections {
		_ = connection.Close()
		delete(p.open, connection)
	}
}

func (p *faultProxy) closeAll() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for connection := range p.open {
		_ = connection.Close()
		delete(p.open, connection)
	}
}

// takeDown makes the database unavailable through the proxy.
func (p *faultProxy) takeDown() {
	p.down.Store(true)
	p.closeAll()
}

// restore makes the database available through the proxy again.
func (p *faultProxy) restore() {
	p.down.Store(false)
}

// waitForRejections waits until the adapters behind the proxy have tried to
// reconnect count times while it's down, proving they noticed the outage.
func (p *faultProxy) waitForRejections(t *testing.T, count int64) {
	t.Helper()

	WaitFor(t, "reconnection attempts", time.Minute, func() bool { return p.rejected.Load() >= count })
}
