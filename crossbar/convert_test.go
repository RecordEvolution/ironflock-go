package crossbar

import (
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/gammazero/nexus/v3/client"
	"github.com/gammazero/nexus/v3/wamp"
)

type namedInt int
type namedString string

func TestNormalizeValue(t *testing.T) {
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	tests := []struct {
		name string
		in   any
		want any
	}{
		{"nil", nil, nil},
		{"bool", true, true},
		{"string", "s", "s"},
		{"int", 7, int64(7)},
		{"int8", int8(-8), int64(-8)},
		{"int16", int16(16), int64(16)},
		{"int32", int32(32), int64(32)},
		{"int64", int64(64), int64(64)},
		{"uint", uint(1), int64(1)},
		{"uint8", uint8(8), int64(8)},
		{"uint16", uint16(16), int64(16)},
		{"uint32", uint32(32), int64(32)},
		{"uint64 small", uint64(64), int64(64)},
		{"uint64 max int64", uint64(math.MaxInt64), int64(math.MaxInt64)},
		{"uint64 above int64", uint64(math.MaxInt64) + 1, uint64(math.MaxInt64) + 1},
		{"float32", float32(1.5), 1.5},
		{"float64", 2.25, 2.25},
		{"bytes", []byte("raw"), []byte("raw")},
		{"named int", namedInt(3), int64(3)},
		{"named string", namedString("x"), "x"},
		{"time", ts, ts},
		{"wamp.List", wamp.List{1, "a"}, []any{int64(1), "a"}},
		{"wamp.Dict", wamp.Dict{"a": uint16(1)}, map[string]any{"a": int64(1)}},
		{"map any any", map[any]any{"k": 1, 2: "two"}, map[string]any{"k": int64(1), "2": "two"}},
		{"typed slice", []int{1, 2}, []any{int64(1), int64(2)}},
		{"typed map", map[string]int{"a": 1}, map[string]any{"a": int64(1)}},
		{"byte array", [3]byte{1, 2, 3}, []byte{1, 2, 3}},
		{"pointer", func() any { v := 5; return &v }(), int64(5)},
		{"nil pointer", (*int)(nil), nil},
		{"nil typed slice", []int(nil), nil},
		{
			"nested",
			wamp.Dict{
				"list": wamp.List{int8(1), wamp.Dict{"x": uint32(2)}, nil, []byte("b")},
				"map":  map[string]any{"inner": wamp.List{float32(0.5), true}},
			},
			map[string]any{
				"list": []any{int64(1), map[string]any{"x": int64(2)}, nil, []byte("b")},
				"map":  map[string]any{"inner": []any{0.5, true}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeValue(tt.in)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("normalizeValue(%#v) = %#v (%T), want %#v (%T)", tt.in, got, got, tt.want, tt.want)
			}
		})
	}
}

func TestNormalizeListAndDictKeepNil(t *testing.T) {
	if got := normalizeList(nil); got != nil {
		t.Fatalf("normalizeList(nil) = %#v", got)
	}
	if got := normalizeDict(nil); got != nil {
		t.Fatalf("normalizeDict(nil) = %#v", got)
	}
	in := wamp.Dict{"a": wamp.List{1}}
	out := normalizeDict(in)
	if _, ok := out["a"].([]any); !ok {
		t.Fatalf("nested wamp.List not converted: %T", out["a"])
	}
	// The input is not modified.
	if _, ok := in["a"].(wamp.List); !ok {
		t.Fatalf("input modified: %T", in["a"])
	}
}

func TestParseWampError(t *testing.T) {
	tests := []struct {
		name, text, prefix string
		want               *WampError
	}{
		{
			"bare uri", "subscribing to topic 'a.b': wamp.error.not_authorized", subscribePrefix("a.b"),
			&WampError{URI: "wamp.error.not_authorized"},
		},
		{
			"uri with args", "waiting for published message: wamp.error.not_authorized: denied, really", publishPrefix,
			&WampError{URI: "wamp.error.not_authorized", Args: []any{"denied, really"}},
		},
		{
			"register", "registering procedure 'p': wamp.error.procedure_already_exists", registerPrefix("p"),
			&WampError{URI: "wamp.error.procedure_already_exists"},
		},
		{"other prefix", "timeout waiting for reply", publishPrefix, nil},
		{"not a uri", "waiting for published message: not a uri", publishPrefix, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseWampError(tt.text, tt.prefix)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("parseWampError(%q) = %#v, want %#v", tt.text, got, tt.want)
			}
		})
	}
}

func TestRequestErrorAndCallError(t *testing.T) {
	c := NewConnection()
	werr := c.requestError("publish", publishPrefix, errors.New("waiting for published message: wamp.error.not_authorized"))
	var w *WampError
	if !errors.As(werr, &w) || w.URI != ErrURINotAuthorized {
		t.Fatalf("requestError = %v, want *WampError not_authorized", werr)
	}

	lost := c.requestError("publish", publishPrefix, client.ErrNotConn)
	if !errors.Is(lost, ErrNotConnected) {
		t.Fatalf("requestError(ErrNotConn) = %v, want ErrNotConnected", lost)
	}

	rpc := client.RPCError{Procedure: "p", Err: &wamp.Error{
		Error:       "app.error.bad",
		Arguments:   wamp.List{uint8(1), wamp.Dict{"k": int32(2)}},
		ArgumentsKw: wamp.Dict{"n": uint64(3)},
	}}
	err := c.callError("p", rpc)
	if !errors.As(err, &w) {
		t.Fatalf("callError = %T %v, want *WampError", err, err)
	}
	want := &WampError{URI: "app.error.bad", Args: []any{int64(1), map[string]any{"k": int64(2)}},
		Kwargs: map[string]any{"n": int64(3)}}
	if !reflect.DeepEqual(w, want) {
		t.Fatalf("callError = %#v, want %#v", w, want)
	}
	if got, want := w.Error(), `app.error.bad: [1,{"k":2}]`; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
}

func TestOptionBuilders(t *testing.T) {
	t.Run("register defaults force_reregister", func(t *testing.T) {
		if got := registerOptions(nil, ""); !reflect.DeepEqual(got, wamp.Dict{"force_reregister": true}) {
			t.Fatalf("got %v", got)
		}
		opts := &RegisterOptions{Invoke: "single", Extra: map[string]any{"x": 1}}
		got := registerOptions(opts, "prefix")
		want := wamp.Dict{"force_reregister": true, "invoke": "single", "match": "prefix", "x": 1}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
		if opts.ForceReregister != nil || len(opts.Extra) != 1 {
			t.Fatalf("caller's options modified: %+v", opts)
		}
	})
	t.Run("register explicit false", func(t *testing.T) {
		f := false
		got := registerOptions(&RegisterOptions{ForceReregister: &f}, "")
		if got["force_reregister"] != false {
			t.Fatalf("got %v", got)
		}
		got = registerOptions(&RegisterOptions{Extra: map[string]any{"force_reregister": false}}, "")
		if got["force_reregister"] != false {
			t.Fatalf("Extra not honoured: %v", got)
		}
	})
	t.Run("call", func(t *testing.T) {
		if got := callOptions(nil); len(got) != 0 {
			t.Fatalf("default call options must be empty (no disclose_me), got %v", got)
		}
		got := callOptions(&CallOptions{Timeout: 1500 * time.Millisecond, DiscloseMe: true, Extra: map[string]any{"a": 1}})
		want := wamp.Dict{"timeout": int64(1500), "disclose_me": true, "a": 1}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
	t.Run("publish", func(t *testing.T) {
		f := false
		got := publishOptions(&PublishOptions{Acknowledge: true, ExcludeMe: &f, Retain: true})
		want := wamp.Dict{"acknowledge": true, "exclude_me": false, "retain": true}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
		if got := publishOptions(&PublishOptions{}); len(got) != 0 {
			t.Fatalf("exclude_me must be omitted when nil, got %v", got)
		}
	})
	t.Run("subscribe", func(t *testing.T) {
		got := subscribeOptions(&SubscribeOptions{Match: "exact", GetRetained: true}, "")
		if !reflect.DeepEqual(got, wamp.Dict{"get_retained": true}) {
			t.Fatalf("got %v", got)
		}
	})
	t.Run("match policy", func(t *testing.T) {
		for in, want := range map[string]string{"": "", "exact": "", "prefix": "prefix", "wildcard": "wildcard"} {
			if got, err := matchPolicy(in, nil); err != nil || got != want {
				t.Fatalf("matchPolicy(%q) = %q, %v", in, got, err)
			}
		}
		if got, _ := matchPolicy("", map[string]any{"match": "prefix"}); got != "prefix" {
			t.Fatalf("match from Extra ignored: %q", got)
		}
		if _, err := matchPolicy("regex", nil); err == nil {
			t.Fatal("invalid match policy accepted")
		}
	})
}

func TestInvokeResult(t *testing.T) {
	tests := []struct {
		name  string
		value any
		err   error
		want  client.InvokeResult
	}{
		{"nil", nil, nil, client.InvokeResult{}},
		{"value", 42, nil, client.InvokeResult{Args: wamp.List{42}}},
		{"Result", Result{Args: []any{1}, Kwargs: map[string]any{"k": 2}},
			nil, client.InvokeResult{Args: wamp.List{1}, Kwargs: wamp.Dict{"k": 2}}},
		{"*Result", &Result{Args: []any{"a"}}, nil, client.InvokeResult{Args: wamp.List{"a"}}},
		{"nil *Result", (*Result)(nil), nil, client.InvokeResult{}},
		{"WampError", nil, &WampError{URI: "app.error.x", Args: []any{1}, Kwargs: map[string]any{"a": 1}},
			client.InvokeResult{Err: "app.error.x", Args: wamp.List{1}, Kwargs: wamp.Dict{"a": 1}}},
		{"wrapped WampError", nil, errors.Join(errors.New("ctx"), &WampError{URI: "app.error.y"}),
			client.InvokeResult{Err: "app.error.y"}},
		{"plain error", "ignored", errors.New("boom"),
			client.InvokeResult{Err: ErrURIRuntimeError, Args: wamp.List{"boom"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := invokeResult(tt.value, tt.err)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("invokeResult = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestFormatSecondsAndErrors(t *testing.T) {
	for d, want := range map[time.Duration]string{
		10 * time.Second: "10", 50 * time.Millisecond: "0.05", 50 * time.Second: "50", 1500 * time.Millisecond: "1.5",
	} {
		if got := formatSeconds(d); got != want {
			t.Fatalf("formatSeconds(%v) = %q, want %q", d, got, want)
		}
	}
	err := error(&notConnectedError{realm: "realm-1-2-dev", wait: 10 * time.Second})
	want := "Not connected to the IronFlock router (realm 'realm-1-2-dev', no session after 10s). " +
		"Ensure Start() was called and the connection is established."
	if err.Error() != want || !errors.Is(err, ErrNotConnected) {
		t.Fatalf("notConnectedError = %q", err)
	}
	err = &connectTimeoutError{realm: "realm-1-2-dev", timeout: DefaultFirstConnectTimeout}
	if err.Error() != "failed to connect to realm realm-1-2-dev within 50s" || !errors.Is(err, ErrNotConnected) {
		t.Fatalf("connectTimeoutError = %q", err)
	}
}

func TestValidateURI(t *testing.T) {
	for _, bad := range []string{"", " a.b", "a.b ", "\ta"} {
		if validateURI("topic", bad) == nil {
			t.Fatalf("validateURI(%q) accepted", bad)
		}
	}
	if err := validateURI("topic", "a.b"); err != nil {
		t.Fatal(err)
	}
}

func TestToDialURL(t *testing.T) {
	for in, want := range map[string]string{
		"ws://h:1/ws-ua-usr":    "ws://h:1/ws-ua-usr",
		"wss://h/ws-ua-usr":     "wss://h/ws-ua-usr",
		"http://h:8080/ws":      "ws://h:8080/ws",
		"https://h/ws-ua-usr?a": "wss://h/ws-ua-usr?a",
	} {
		if got, err := toDialURL(in); err != nil || got != want {
			t.Fatalf("toDialURL(%q) = %q, %v", in, got, err)
		}
	}
	for _, bad := range []string{"ftp://h/x", "ws:///nohost", "::bad"} {
		if _, err := toDialURL(bad); err == nil {
			t.Fatalf("toDialURL(%q) accepted", bad)
		}
	}
}
