//go:build integration

package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ironflock "github.com/RecordEvolution/ironflock-go"
	"github.com/RecordEvolution/ironflock-go/wamp"
)

// startIronFlock creates and starts an IronFlock against the fake platform.
func startIronFlock(t *testing.T, ctx context.Context, opts ...ironflock.Option) *ironflock.IronFlock {
	t.Helper()
	opts = append([]ironflock.Option{ironflock.WithURL(platformURL(t))}, opts...)
	ifl, err := ironflock.New(opts...)
	if err != nil {
		t.Fatal(err)
	}
	if err := ifl.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := ifl.Stop(context.Background()); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	return ifl
}

// pythonPeer runs py_peer.py with the Python SDK, skipping when no Python
// interpreter with the ironflock package is configured.
func pythonPeer(t *testing.T, ctx context.Context, args ...string) *exec.Cmd {
	t.Helper()
	python := os.Getenv("IRONFLOCK_TEST_PYTHON")
	if python == "" {
		t.Skip("IRONFLOCK_TEST_PYTHON not set (a python with the ironflock package installed)")
	}
	cmd := exec.CommandContext(ctx, python, append([]string{"-I", "py_peer.py"}, args...)...)
	cmd.Env = os.Environ()
	return cmd
}

// waitLine reads r until a line starting with prefix and returns the rest.
func waitLine(t *testing.T, r io.Reader, prefix string) string {
	t.Helper()
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		if line := sc.Text(); strings.HasPrefix(line, prefix) {
			return strings.TrimPrefix(line, prefix)
		}
	}
	t.Fatalf("python peer exited before printing %q (err %v)", prefix, sc.Err())
	return ""
}

func TestDeviceFunctionInteropWithPython(t *testing.T) {
	deviceEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	ifl := startIronFlock(t, ctx)

	// Python calls a function the Go SDK registered.
	_, err := ifl.RegisterDeviceFunction(ctx, "go_echo", func(ctx context.Context, inv *ironflock.Invocation) (any, error) {
		return map[string]any{"args": inv.Args, "kwargs": inv.Kwargs, "from": "go"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	call := pythonPeer(t, ctx, "call", testDeviceID, "go_echo")
	stdout, _ := call.StdoutPipe()
	if err := call.Start(); err != nil {
		t.Fatal(err)
	}
	var got any
	if err := json.Unmarshal([]byte(waitLine(t, stdout, "RESULT:")), &got); err != nil {
		t.Fatal(err)
	}
	_ = call.Wait()
	want := map[string]any{"args": []any{1.0, "two"}, "kwargs": map[string]any{"k": true}, "from": "go"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("python called go_echo: got %#v, want %#v", got, want)
	}

	// The Go SDK calls a function the Python SDK registered.
	serve := pythonPeer(t, ctx, "serve", "py_echo")
	stdin, _ := serve.StdinPipe()
	stdout, _ = serve.StdoutPipe()
	if err := serve.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stdin.Close(); _ = serve.Wait() }()
	waitLine(t, stdout, "READY")
	go func() { _, _ = io.Copy(io.Discard, stdout) }()
	res, err := ifl.CallDeviceFunction(ctx, 42, "py_echo", 1, "two", ironflock.Kwargs{"k": true})
	if err != nil {
		t.Fatal(err)
	}
	wantPy := map[string]any{"args": []any{int64(1), "two"}, "kwargs": map[string]any{"k": true}, "from": "python"}
	if !reflect.DeepEqual(res.Value(), wantPy) {
		t.Fatalf("go called py_echo: got %#v, want %#v", res.Value(), wantPy)
	}
}

// collect receives n rows from ch, failing after timeout.
func collect(t *testing.T, ch <-chan ironflock.Row, n int, timeout time.Duration) []ironflock.Row {
	t.Helper()
	var rows []ironflock.Row
	deadline := time.After(timeout)
	for len(rows) < n {
		select {
		case r := <-ch:
			rows = append(rows, r)
		case <-deadline:
			t.Fatalf("received %d of %d rows: %v", len(rows), n, rows)
		}
	}
	return rows
}

func TestSubscribeToTableEndToEnd(t *testing.T) {
	deviceEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	ifl := startIronFlock(t, ctx)

	rows := make(chan ironflock.Row, 16)
	sub, err := ifl.SubscribeToTable(ctx, "e2e_table", func(ev *ironflock.Event) { rows <- ev.Row() })
	if err != nil {
		t.Fatal(err)
	}
	if err := ifl.PublishToTable(ctx, "e2e_table", ironflock.Row{"temperature": 9.5, "source": "go"}); err != nil {
		t.Fatal(err)
	}
	if err := ifl.PublishRowsToTable(ctx, "e2e_table", []ironflock.Row{
		{"temperature": 1, "source": "go-bulk"}, {"temperature": 2, "source": "go-bulk"}}); err != nil {
		t.Fatal(err)
	}
	got := collect(t, rows, 3, 10*time.Second)
	sources := map[string]int{}
	for _, r := range got {
		sources[r["source"].(string)]++
	}
	if sources["go"] != 1 || sources["go-bulk"] != 2 {
		t.Fatalf("rows %v", got)
	}

	if os.Getenv("IRONFLOCK_TEST_PYTHON") != "" {
		pub := pythonPeer(t, ctx, "publish_table", "e2e_table")
		if out, err := pub.CombinedOutput(); err != nil {
			t.Fatalf("python publish: %v\n%s", err, out)
		}
		got = collect(t, rows, 3, 10*time.Second)
		for _, r := range got {
			if !strings.HasPrefix(r["source"].(string), "python") {
				t.Fatalf("unexpected row %v", r)
			}
		}
	}

	if err := sub.Unsubscribe(ctx); err != nil {
		t.Fatal(err)
	}
	if err := ifl.PublishToTable(ctx, "e2e_table", ironflock.Row{"source": "after"}); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-rows:
		t.Fatalf("row after unsubscribe: %v", r)
	case <-time.After(time.Second):
	}
}

func TestConsumedAppRealtimeAndGuards(t *testing.T) {
	deviceEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	ifl := startIronFlock(t, ctx)

	app, err := ifl.ConnectToApp(ctx, "Weather")
	if err != nil {
		t.Fatal(err)
	}
	if app.App != "weather" || app.Stage != "dev" || len(app.Tables) != 1 || app.Tables[0].Tablename != "readings" {
		t.Fatalf("handle %+v", app)
	}
	again, err := ifl.ConnectToApp(ctx, "weather")
	if err != nil || again != app {
		t.Fatalf("cache: %v %p %p", err, again, app)
	}

	rows := make(chan ironflock.Row, 8)
	if _, err := app.SubscribeToTable(ctx, "readings", func(ev *ironflock.Event) { rows <- ev.Row() }); err != nil {
		t.Fatal(err)
	}
	if _, err := ifl.Call(ctx, "test.provider.publish", "readings", ironflock.Row{"temp": 5}); err != nil {
		t.Fatal(err)
	}
	if _, err := ifl.Call(ctx, "test.provider.publish", "readings",
		[]ironflock.Row{{"temp": 6}, {"temp": 7}}, ironflock.Kwargs{"bulk": true}); err != nil {
		t.Fatal(err)
	}
	collect(t, rows, 3, 10*time.Second)

	var cerr *ironflock.CrossAppAccessError
	if _, err := app.SubscribeToTable(ctx, "private_table", func(*ironflock.Event) {}); !errors.As(err, &cerr) || cerr.Code != ironflock.CodePrivateTable {
		t.Fatalf("private table: %v", err)
	}
	if _, err := app.GetHistory(ctx, "readings", &ironflock.TableQueryParams{Limit: 1, Columns: []string{"api_key"}}); !errors.As(err, &cerr) || cerr.Code != ironflock.CodeSecretColumn {
		t.Fatalf("secret column: %v", err)
	}
	if _, err := app.GetHistory(ctx, "readings", &ironflock.TableQueryParams{Limit: 1, FilterAnd: []ironflock.Filter{
		ironflock.Or(ironflock.Where("api_key", "=", "x"), ironflock.IsNull("temp"))}}); !errors.As(err, &cerr) || cerr.Code != ironflock.CodeSecretColumn {
		t.Fatalf("secret column inside a group: %v", err)
	}
	if _, err := app.GetSeriesHistory(ctx, "hourly", ironflock.SeriesQueryParams{Metrics: []string{"temp"}, Method: ironflock.MethodAvg, Limit: 1,
		TimeRange: &ironflock.TimeRange{}}); !errors.As(err, &cerr) || cerr.Code != ironflock.CodePrivateTable {
		t.Fatalf("series on a transform: %v", err)
	}

	// The provider realm grants a consumer read access only.
	err = app.Connection().Publish(ctx, "transformed.readings", nil, map[string]any{"x": 1},
		&wamp.PublishOptions{Acknowledge: true}, 0)
	if ironflock.WampURI(err) != wamp.URINotAuthorized {
		t.Fatalf("publish on provider realm: %v", err)
	}

	if err := app.Close(ctx); err != nil {
		t.Fatal(err)
	}
	fresh, err := ifl.ConnectToApp(ctx, "weather")
	if err != nil || fresh == app {
		t.Fatalf("reconnect after close: %v", err)
	}
}

func TestPerAppCredentialRotation(t *testing.T) {
	deviceEnv(t)
	dir := os.Getenv("IRONFLOCK_ENV_DIR")
	write := func(name, value string) {
		if err := os.WriteFile(filepath.Join(dir, name+".txt"), []byte(value+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("APP_AUTH_ID", "app-26-per-app")
	write("APP_AUTH_SECRET", "wrong-secret")

	ifl, err := ironflock.New(ironflock.WithURL(platformURL(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := ifl.Stop(context.Background()); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	started := make(chan error, 1)
	go func() { started <- ifl.Start(ctx) }()

	select {
	case err := <-started:
		t.Fatalf("joined with a wrong per-app secret: %v", err)
	case <-time.After(4 * time.Second):
	}
	// The agent rotates the credential while the app runs: the next attempt
	// must pick it up without a restart.
	write("APP_AUTH_SECRET", "per-app-secret")
	select {
	case err := <-started:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("did not join after the credential was rotated")
	}
}

func TestRouterRestartRecovery(t *testing.T) {
	restart := os.Getenv("IRONFLOCK_TEST_RESTART_ROUTER")
	if restart == "" {
		t.Skip("IRONFLOCK_TEST_RESTART_ROUTER not set (a shell command that restarts the router)")
	}
	deviceEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	ifl := startIronFlock(t, ctx, ironflock.WithReconnectWindow(90*time.Second))

	rows := make(chan ironflock.Row, 16)
	if _, err := ifl.SubscribeToTable(ctx, "restart_table", func(ev *ironflock.Event) { rows <- ev.Row() }); err != nil {
		t.Fatal(err)
	}
	if _, err := ifl.RegisterDeviceFunction(ctx, "restart_echo", func(ctx context.Context, inv *ironflock.Invocation) (any, error) {
		return "pong", nil
	}); err != nil {
		t.Fatal(err)
	}

	restarted := make(chan error, 1)
	go func() {
		out, err := exec.Command("bash", "-c", restart).CombinedOutput()
		if err != nil {
			err = errors.New(err.Error() + ": " + string(out))
		}
		restarted <- err
	}()

	// Wait for the outage, then issue a table write: it must ride out the
	// restart (reconnect window) instead of failing.
	deadline := time.Now().Add(30 * time.Second)
	for ifl.IsConnected() && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if ifl.IsConnected() {
		t.Fatal("connection never went down")
	}
	res, err := ifl.AppendToTable(ctx, "restart_table", ironflock.Row{"source": "during-outage"})
	if err != nil {
		t.Fatalf("append during the outage: %v", err)
	}
	if v, _ := res.Value().(map[string]any); v["success"] != true {
		t.Fatalf("append result %v", res.Value())
	}
	if err := <-restarted; err != nil {
		t.Fatal(err)
	}
	// The table subscription was restored: the appended row comes back on
	// transformed.restart_table.
	if r := collect(t, rows, 1, 20*time.Second); r[0]["source"] != "during-outage" {
		t.Fatalf("row %v", r)
	}
	// The device function was re-registered.
	got, err := ifl.CallDeviceFunction(ctx, 42, "restart_echo")
	if err != nil || got.Value() != "pong" {
		t.Fatalf("call after restart: %v %v", got, err)
	}
}

// freezeProxy forwards TCP connections to target until frozen; frozen, it
// stops moving bytes without closing anything — a silently half-open link,
// as an idle NAT or a router behind a load balancer produces.
type freezeProxy struct {
	ln     net.Listener
	target string
	frozen atomic.Bool
	mu     sync.Mutex
	conns  []net.Conn
}

func newFreezeProxy(t *testing.T, target string) *freezeProxy {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &freezeProxy{ln: ln, target: target}
	go p.serve()
	t.Cleanup(p.close)
	return p
}

func (p *freezeProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		if p.frozen.Load() {
			_ = c.Close()
			continue
		}
		u, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = c.Close()
			continue
		}
		p.mu.Lock()
		p.conns = append(p.conns, c, u)
		p.mu.Unlock()
		go p.pipe(c, u)
		go p.pipe(u, c)
	}
}

func (p *freezeProxy) pipe(dst, src net.Conn) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		for p.frozen.Load() {
			time.Sleep(20 * time.Millisecond) // swallow traffic: never deliver, never close
			n = 0
		}
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *freezeProxy) close() {
	_ = p.ln.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
}

func TestKeepAliveDetectsHalfOpenLink(t *testing.T) {
	url := platformURL(t)
	host := strings.SplitN(strings.TrimPrefix(url, "ws://"), "/", 2)[0]
	proxy := newFreezeProxy(t, host)

	disconnected := make(chan string, 4)
	conn := wamp.NewConnection()
	if err := conn.Configure(wamp.Config{
		SwarmKey: 2, AppKey: 26, Stage: wamp.StageDevelopment,
		URL:          "ws://" + proxy.ln.Addr().String() + "/ws-ua-usr",
		SerialNumber: testSerial, AuthID: testSerial, AuthSecret: testSerial,
		KeepAlive:    time.Second,
		OnDisconnect: func(reason string) { disconnected <- reason },
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := conn.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := conn.Stop(context.Background()); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}()

	proxy.frozen.Store(true)
	select {
	case <-disconnected:
	case <-time.After(10 * time.Second):
		t.Fatal("a silently dead link was not detected by keepalive")
	}
	proxy.frozen.Store(false)
	if err := conn.WaitSession(ctx, 20*time.Second); err != nil {
		t.Fatalf("no reconnect after the link recovered: %v", err)
	}
}
