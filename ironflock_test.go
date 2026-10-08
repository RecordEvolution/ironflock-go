package ironflock

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/RecordEvolution/ironflock-go/crossbar"
)

func TestNewReadsTheInjectedEnvironment(t *testing.T) {
	f := flock(t)
	if got := f.SerialNumber(); got != "test-serial-123" {
		t.Errorf("SerialNumber = %q", got)
	}
	if got := f.DeviceName(); got != "test-device" {
		t.Errorf("DeviceName = %q", got)
	}
	if got := f.DeviceKey(); got != "12" {
		t.Errorf("DeviceKey = %q", got)
	}
	if got := f.AppName(); got != "TestApp" {
		t.Errorf("AppName = %q", got)
	}
	if f.SwarmKey() != 10 || f.AppKey() != 20 {
		t.Errorf("keys = %d, %d", f.SwarmKey(), f.AppKey())
	}
	if f.Stage() != StageDevelopment {
		t.Errorf("Stage = %q", f.Stage())
	}
	if f.reconnectWindow != DefaultReconnectWindow {
		t.Errorf("reconnect window = %v", f.reconnectWindow)
	}
	if strings.Contains(f.logs.String(), "must be present") {
		t.Errorf("unexpected missing-variable warning: %s", f.logs)
	}
	if f.IsConnected() {
		t.Error("IsConnected before Start")
	}
	if f.Connection() != nil {
		t.Error("Connection() of a fake must be nil")
	}
}

func TestNewOptionsOverrideTheEnvironment(t *testing.T) {
	f := flock(t,
		WithSerialNumber("other-serial"), WithDeviceName("n"), WithDeviceKey("99"), WithAppName("a"),
		WithSwarmKey(1), WithAppKey(2), WithEnv("prod"), WithReconnectWindow(5*time.Second))
	if f.SerialNumber() != "other-serial" || f.DeviceName() != "n" || f.DeviceKey() != "99" || f.AppName() != "a" {
		t.Errorf("identity = %q %q %q %q", f.SerialNumber(), f.DeviceName(), f.DeviceKey(), f.AppName())
	}
	if f.SwarmKey() != 1 || f.AppKey() != 2 || f.Stage() != StageProduction || f.reconnectWindow != 5*time.Second {
		t.Errorf("keys %d %d stage %q window %v", f.SwarmKey(), f.AppKey(), f.Stage(), f.reconnectWindow)
	}
}

func TestNewWithoutSerialNumberFails(t *testing.T) {
	setIdentityEnv(t)
	unsetEnv(t, "DEVICE_SERIAL_NUMBER")
	_, err := newWithConn(&fakeConn{}, nil)
	if !errors.Is(err, ErrMissingConfig) {
		t.Fatalf("err = %v, want ErrMissingConfig", err)
	}
	mustContain(t, err.Error(), "DEVICE_SERIAL_NUMBER")

	// An explicit serial number is enough.
	if _, err := newWithConn(&fakeConn{}, nil, WithSerialNumber("custom-serial")); err != nil {
		t.Fatal(err)
	}
}

func TestNewWarnsAboutMissingVariables(t *testing.T) {
	setIdentityEnv(t)
	unsetEnv(t, "DEVICE_KEY", "APP_NAME")
	f := newTestFlock(t)
	mustContain(t, f.logs.String(), "Warning: The following environment variables must be present: DEVICE_KEY, APP_NAME")

	setIdentityEnv(t)
	unsetEnv(t, "DEVICE_KEY", "APP_NAME", "SWARM_KEY", "APP_KEY")
	f = newTestFlock(t)
	mustContain(t, f.logs.String(),
		"Warning: The following environment variables must be present: DEVICE_KEY, APP_NAME, SWARM_KEY, APP_KEY")
}

func TestNewParsesKeys(t *testing.T) {
	cases := []struct {
		raw     string
		want    int
		warning bool
	}{
		{"7", 7, false},
		{" 7 ", 7, false},
		{"", 0, false},
		{"abc", 0, true},
		{"7.5", 0, true},
	}
	for _, tc := range cases {
		setIdentityEnv(t)
		t.Setenv("SWARM_KEY", tc.raw)
		f := newTestFlock(t)
		if f.SwarmKey() != tc.want {
			t.Errorf("SWARM_KEY=%q: got %d, want %d", tc.raw, f.SwarmKey(), tc.want)
		}
		if got := strings.Contains(f.logs.String(), "is not an integer"); got != tc.warning {
			t.Errorf("SWARM_KEY=%q: parse warning %v, want %v: %s", tc.raw, got, tc.warning, f.logs)
		}
	}
}

func TestStageFromEnv(t *testing.T) {
	cases := map[string]Stage{
		"DEV": StageDevelopment, "PROD": StageProduction, "": StageDevelopment, "dev": StageDevelopment,
		"prod": StageProduction, "Prod": StageProduction, "production": StageDevelopment, "staging": StageDevelopment,
	}
	for env, want := range cases {
		setIdentityEnv(t)
		t.Setenv("ENV", env)
		if got := newTestFlock(t).Stage(); got != want {
			t.Errorf("ENV=%q: stage %q, want %q", env, got, want)
		}
	}
	setIdentityEnv(t)
	unsetEnv(t, "ENV")
	if got := newTestFlock(t).Stage(); got != StageDevelopment {
		t.Errorf("no ENV: stage %q", got)
	}
}

func TestNewCredentialsAreBothOrNeither(t *testing.T) {
	setIdentityEnv(t)
	for _, opt := range []Option{WithCredentials("id", ""), WithCredentials("", "secret")} {
		if _, err := newWithConn(&fakeConn{}, nil, opt); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("err = %v, want ErrInvalidArgument", err)
		}
	}
}

func TestNewReconnectWindow(t *testing.T) {
	for _, tc := range []struct {
		opt  time.Duration
		want time.Duration
	}{{0, 0}, {-time.Second, 0}, {90 * time.Second, 90 * time.Second}} {
		f := flock(t, WithReconnectWindow(tc.opt))
		if f.reconnectWindow != tc.want {
			t.Errorf("WithReconnectWindow(%v) = %v, want %v", tc.opt, f.reconnectWindow, tc.want)
		}
	}
}

func TestStartConfiguresTheConnection(t *testing.T) {
	f := flock(t)
	if err := f.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	cfg := f.own.config()
	if cfg.SwarmKey != 10 || cfg.AppKey != 20 || cfg.Stage != StageDevelopment {
		t.Errorf("realm config = %d %d %q", cfg.SwarmKey, cfg.AppKey, cfg.Stage)
	}
	if cfg.URL != crossbar.StudioWSURI {
		t.Errorf("URL = %q", cfg.URL)
	}
	if cfg.SerialNumber != "test-serial-123" || cfg.AuthID != "" || cfg.AuthSecret != "" {
		t.Errorf("identity = %q %q %q", cfg.SerialNumber, cfg.AuthID, cfg.AuthSecret)
	}
	if cfg.FailOnAuthError {
		t.Error("the primary connection must not fail on auth errors")
	}
	if cfg.Logger == nil {
		t.Error("logger not passed on")
	}
	if !f.IsConnected() {
		t.Error("IsConnected after Start")
	}
	if err := f.Start(context.Background()); err == nil {
		t.Error("a second Start must fail")
	}
}

func TestStartURLPrecedence(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		opts []Option
		want string
	}{
		{"default", nil, nil, crossbar.StudioWSURI},
		{"RESWARM_URL", map[string]string{"RESWARM_URL": "http://localhost:8086"}, nil, crossbar.LocalhostWSURI},
		{"WithReswarmURL", nil, []Option{WithReswarmURL("https://studio.ironflock.dev")}, crossbar.StudioDevWSURI},
		{"DEVICE_ENDPOINT_URL", map[string]string{"DEVICE_ENDPOINT_URL": "ws://10.0.0.1:18080/x", "RESWARM_URL": "http://localhost:8086"}, nil, "ws://10.0.0.1:18080/ws-ua-usr"},
		{"WithURL", map[string]string{"DEVICE_ENDPOINT_URL": "ws://10.0.0.1:18080/x"}, []Option{WithURL("ws://explicit/ws")}, "ws://explicit/ws"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setIdentityEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			f := newTestFlock(t, tc.opts...)
			if err := f.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			if got := f.own.config().URL; got != tc.want {
				t.Errorf("URL = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestStartWithUnknownStudioURLFails(t *testing.T) {
	setIdentityEnv(t)
	t.Setenv("RESWARM_URL", "https://appliance.example")
	f := newTestFlock(t)
	if err := f.Start(context.Background()); err == nil {
		t.Fatal("want an error for an unknown RESWARM_URL")
	}
	if f.own.startCount() != 0 {
		t.Error("the connection must not be started")
	}
}

func TestStartPassesExplicitCredentials(t *testing.T) {
	f := flock(t, WithCredentials("app-id", "app-secret"))
	if err := f.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if cfg := f.own.config(); cfg.AuthID != "app-id" || cfg.AuthSecret != "app-secret" {
		t.Errorf("credential = %q %q", cfg.AuthID, cfg.AuthSecret)
	}
}

func TestStartWithoutKeysFailsFast(t *testing.T) {
	setIdentityEnv(t)
	unsetEnv(t, "APP_KEY")
	f := newTestFlock(t)
	err := f.Start(context.Background())
	if !errors.Is(err, ErrMissingConfig) {
		t.Fatalf("err = %v, want ErrMissingConfig", err)
	}
	mustContain(t, err.Error(), "APP_KEY not set in environment variables!")
	if f.own.configureCount() != 0 || f.own.startCount() != 0 {
		t.Error("the connection must not be touched")
	}
}

func TestStartCanBeRetriedAfterAFailure(t *testing.T) {
	f := flock(t)
	boom := errors.New("boom")
	failures := 1
	f.own.startFn = func(ctx context.Context, cfg crossbar.Config) error {
		if failures > 0 {
			failures--
			return boom
		}
		return nil
	}
	if err := f.Start(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("first Start: %v", err)
	}
	if err := f.Start(context.Background()); err != nil {
		t.Fatalf("second Start: %v", err)
	}
	if f.own.configureCount() != 1 || f.own.startCount() != 2 {
		t.Errorf("configured %d times, started %d times", f.own.configureCount(), f.own.startCount())
	}
}

func TestStopIsIdempotent(t *testing.T) {
	f := flock(t)
	if err := f.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := f.Stop(context.Background()); err != nil {
				t.Errorf("Stop: %v", err)
			}
		}()
	}
	wg.Wait()
	if err := f.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := f.own.stopCount(); n != 1 {
		t.Errorf("connection stopped %d times", n)
	}
	if err := f.Start(context.Background()); !errors.Is(err, crossbar.ErrStopped) {
		t.Errorf("Start after Stop: %v", err)
	}
	if _, err := f.ConnectToApp(context.Background(), "weatherstation"); !errors.Is(err, crossbar.ErrStopped) {
		t.Errorf("ConnectToApp after Stop: %v", err)
	}
}

func TestStopReturnsTheConnectionError(t *testing.T) {
	f := flock(t)
	boom := errors.New("boom")
	f.own.stopErr = boom
	if err := f.Stop(context.Background()); !errors.Is(err, boom) {
		t.Errorf("Stop: %v", err)
	}
}

func TestRunRunsMainAndStops(t *testing.T) {
	checkGoroutines(t)
	f := flock(t)
	sentinel := errors.New("main failed")
	err := f.Run(context.Background(), func(ctx context.Context) error {
		if !f.IsConnected() {
			t.Error("main runs after Start")
		}
		for range 3 {
			if err := f.Publish(ctx, "test.publish.com", 1, "two", 3, Kwargs{"foo": "bar"}); err != nil {
				return err
			}
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Run = %v, want main's error", err)
	}
	if n := len(f.own.allPublishes()); n != 3 {
		t.Errorf("%d publications", n)
	}
	if f.own.startCount() != 1 || f.own.stopCount() != 1 {
		t.Errorf("started %d, stopped %d", f.own.startCount(), f.own.stopCount())
	}
}

func TestRunCancelsMainOnContextDone(t *testing.T) {
	checkGoroutines(t)
	f := flock(t)
	ctx, cancel := context.WithCancel(context.Background())
	mainCtxDone := make(chan struct{})
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	err := f.Run(ctx, func(ctx context.Context) error {
		<-ctx.Done()
		close(mainCtxDone)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-mainCtxDone:
	default:
		t.Error("main's context was not cancelled")
	}
	if f.own.stopCount() != 1 {
		t.Error("Run did not stop the connection")
	}
}

func TestRunWithoutMainRunsUntilContextDone(t *testing.T) {
	checkGoroutines(t)
	f := flock(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := f.Run(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if f.own.startCount() != 1 || f.own.stopCount() != 1 {
		t.Errorf("started %d, stopped %d", f.own.startCount(), f.own.stopCount())
	}
}

func TestRunReturnsTheStartError(t *testing.T) {
	checkGoroutines(t)
	f := flock(t)
	boom := errors.New("cannot connect")
	f.own.startFn = func(context.Context, crossbar.Config) error { return boom }
	called := false
	err := f.Run(context.Background(), func(context.Context) error { called = true; return nil })
	if !errors.Is(err, boom) {
		t.Fatalf("Run = %v", err)
	}
	if called {
		t.Error("main must not run when Start fails")
	}
}

func TestRunStopsWithAFreshContext(t *testing.T) {
	f := flock(t)
	ctx, cancel := context.WithCancel(context.Background())
	err := f.Run(ctx, func(context.Context) error {
		cancel() // the caller's context is already done when Run stops
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if f.own.stopCount() != 1 {
		t.Errorf("stopped %d times", f.own.stopCount())
	}
}

func TestFilesIsBuiltOnceAndLazily(t *testing.T) {
	f := flock(t)
	if f.files != nil {
		t.Fatal("Files built before first use")
	}
	first := filesOrSkip(t, f.IronFlock)
	var wg sync.WaitGroup
	got := make([]*FileStore, 8)
	for i := range got {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got[i] = f.Files()
		}()
	}
	wg.Wait()
	for i, fs := range got {
		if fs != first {
			t.Errorf("Files() call %d returned a different store", i)
		}
	}
}

// filesOrSkip returns f.Files(), skipping the test while filestore.New is
// still a stub.
func filesOrSkip(t *testing.T, f *IronFlock) (fs *FileStore) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			if s, ok := r.(string); ok && strings.Contains(s, "TODO") {
				t.Skip("filestore.New is not implemented yet")
			}
			panic(r)
		}
	}()
	return f.Files()
}

func TestNewUsesACrossbarConnection(t *testing.T) {
	setIdentityEnv(t)
	log, _ := newTestLogger()
	f, err := New(WithLogger(log))
	if err != nil {
		t.Fatal(err)
	}
	if f.Connection() == nil {
		t.Error("Connection() must expose the *crossbar.Connection")
	}
	if c, ok := f.newConn().(*crossbar.Connection); !ok || c == nil || c == f.Connection() {
		t.Error("consumed apps must get connections of their own")
	}

	unsetEnv(t, "DEVICE_SERIAL_NUMBER")
	if _, err := New(); !errors.Is(err, ErrMissingConfig) {
		t.Errorf("New without a serial number: %v", err)
	}
}
