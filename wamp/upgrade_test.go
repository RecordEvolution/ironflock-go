package wamp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// A router behind a load balancer whose upstream is gone accepts TCP (and
// TLS) but never answers the WebSocket upgrade. gorilla/websocket bounds the
// upgrade only by the attempt's deadline, the 15s connect timeout; Start and
// Stop must not wait for it. The tests keep connectTimeout far above their
// bounds, so a wait for it fails them.
const stalledConnectTimeout = 30 * time.Second

// stallingServer accepts connections and reads the WebSocket upgrade
// request, but never answers it.
type stallingServer struct {
	url      string
	tls      *tls.Config      // trusts the server's certificate (wss)
	requests chan struct{}    // receives a value for every upgrade request
	srv      *httptest.Server // closed at cleanup
	release  chan struct{}    // closed at cleanup: ends the stalled handlers
}

func newStallingServer(t *testing.T, useTLS bool) *stallingServer {
	t.Helper()
	s := &stallingServer{requests: make(chan struct{}, 16), release: make(chan struct{})}
	s.srv = httptest.NewUnstartedServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case s.requests <- struct{}{}:
		default:
		}
		select {
		case <-r.Context().Done(): // the client closed the connection
		case <-s.release:
		}
	}))
	if useTLS {
		s.srv.StartTLS()
		pool := x509.NewCertPool()
		pool.AddCert(s.srv.Certificate())
		s.tls = &tls.Config{RootCAs: pool}
		s.url = "wss" + strings.TrimPrefix(s.srv.URL, "https") + "/ws-ua-usr"
	} else {
		s.srv.Start()
		s.url = "ws" + strings.TrimPrefix(s.srv.URL, "http") + "/ws-ua-usr"
	}
	t.Cleanup(func() {
		close(s.release)
		s.srv.Close()
	})
	return s
}

// newStalledConn returns a configured connection to srv whose connect
// timeout is far longer than any bound the tests assert.
func newStalledConn(t *testing.T, srv *stallingServer) *Connection {
	t.Helper()
	c := NewConnection()
	fastTunables(c)
	c.t.connectTimeout = stalledConnectTimeout
	if err := c.Configure(Config{
		SwarmKey:     1,
		AppKey:       2,
		URL:          srv.url,
		SerialNumber: testSerial,
		AuthID:       testAuthID,
		AuthSecret:   testSecret,
		TLSConfig:    srv.tls,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Stop(ctxTimeout(t, 5*time.Second)); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	return c
}

func forEachScheme(t *testing.T, test func(t *testing.T, useTLS bool)) {
	t.Run("ws", func(t *testing.T) { test(t, false) })
	t.Run("wss", func(t *testing.T) { test(t, true) })
}

// Start returns as soon as its ctx ends, even while the upgrade is stalled,
// with the supervisor already gone — and the connection can be started
// again.
func TestStartReturnsAtCtxEndDuringStalledUpgrade(t *testing.T) {
	forEachScheme(t, func(t *testing.T, useTLS bool) {
		srv := newStallingServer(t, useTLS)
		c := newStalledConn(t, srv)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cancelled := make(chan time.Time, 1)
		go func() {
			select {
			case <-srv.requests: // the attempt is in its upgrade
			case <-time.After(5 * time.Second):
			}
			cancelled <- time.Now()
			cancel()
		}()
		err := c.Start(ctx)
		returned := time.Now()
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Start = %v, want context canceled", err)
		}
		if el := returned.Sub(<-cancelled); el > time.Second {
			t.Fatalf("Start returned %v after its ctx ended", el)
		}
		if !closed(c.done) {
			t.Fatal("supervisor still running after Start returned")
		}

		// Restartable: the next Start dials again (and fails the same way).
		start := time.Now()
		err = c.Start(ctxTimeout(t, 200*time.Millisecond))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("second Start = %v, want deadline exceeded", err)
		}
		if el := time.Since(start); el > 1200*time.Millisecond {
			t.Fatalf("second Start took %v with a 200ms ctx", el)
		}
		recv(t, srv.requests, "the second Start's upgrade request")
	})
}

// Stop interrupts an attempt stalled in its upgrade at once.
func TestStopInterruptsStalledUpgrade(t *testing.T) {
	forEachScheme(t, func(t *testing.T, useTLS bool) {
		srv := newStallingServer(t, useTLS)
		c := newStalledConn(t, srv)
		started := make(chan error, 1)
		go func() { started <- c.Start(context.Background()) }()
		recv(t, srv.requests, "the upgrade request")

		begin := time.Now()
		if err := c.Stop(ctxTimeout(t, 5*time.Second)); err != nil {
			t.Fatalf("Stop = %v after %v", err, time.Since(begin))
		}
		if el := time.Since(begin); el > time.Second {
			t.Fatalf("Stop took %v", el)
		}
		if !closed(c.done) {
			t.Fatal("supervisor still running after Stop returned")
		}
		if err := recv(t, started, "Start"); !errors.Is(err, ErrStopped) {
			t.Fatalf("Start = %v, want ErrStopped", err)
		}
	})
}

// The same during a reconnect: the router went away and new connections
// stall in their upgrade (a platform restart behind a load balancer). Stop
// with Run's budget returns nil long before it.
func TestStopInterruptsStalledReconnect(t *testing.T) {
	tr := newTestRouter(t, true)
	proxy := newDropProxy(t, tr.url)
	disconnects := make(chan string, 4)
	c, _ := startTestConn(t, tr, func(cfg *Config, c *Connection) {
		cfg.URL = proxy.url
		cfg.OnDisconnect = func(reason string) { disconnects <- reason }
		c.t.connectTimeout = stalledConnectTimeout
	})

	proxy.stall.Store(true)
	proxy.dropAll()
	recv(t, disconnects, "OnDisconnect")
	recv(t, proxy.stalled, "a reconnect stalled in its upgrade")

	begin := time.Now()
	if err := c.Stop(ctxTimeout(t, 10*time.Second)); err != nil {
		t.Fatalf("Stop = %v after %v", err, time.Since(begin))
	}
	if el := time.Since(begin); el > time.Second {
		t.Fatalf("Stop took %v", el)
	}
	if !closed(c.done) {
		t.Fatal("supervisor still running after Stop returned")
	}
}
