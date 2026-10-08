package ironflock

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/RecordEvolution/ironflock-go/wamp"
)

// answer makes the fake connection answer every call with v.
func answer(c *fakeConn, v any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.callFn = func(fakeCall) (*wamp.Result, error) { return resultOf(v), nil }
}

// fail makes the fake connection fail every call with err.
func fail(c *fakeConn, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.callFn = func(fakeCall) (*wamp.Result, error) { return nil, err }
}

func TestGetHistoryPayloads(t *testing.T) {
	f := flock(t)
	answer(f.own, []any{map[string]any{"temperature": 21.5}})

	rows, err := f.GetHistory(bg, "sensordata", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rows, []Row{{"temperature": 21.5}}) {
		t.Errorf("rows %#v", rows)
	}
	want := fakeCall{
		Procedure: "history.transformed.sensordata",
		Args:      []any{map[string]any{"limit": int64(10), "offset": int64(0)}},
		Window:    DefaultReconnectWindow,
	}
	if got := f.own.lastCall(t); !reflect.DeepEqual(got, want) {
		t.Errorf("call %#v\nwant %#v", got, want)
	}

	q := &TableQueryParams{
		Limit:     100,
		FilterAnd: []Filter{Latest(), Where("device_key", "=", 42), Where("deleted", "=", false)},
		Columns:   []string{"temperature"},
		TimeRange: &TimeRange{Start: "2026-09-14T09:57:43Z", End: nil},
	}
	before := *q
	if _, err := f.GetHistory(bg, "sensordata", q); err != nil {
		t.Fatal(err)
	}
	wantPayload := map[string]any{
		"limit":  int64(100),
		"offset": int64(0),
		"filterAnd": []any{
			map[string]any{"latest": true},
			map[string]any{"column": "device_key", "operator": "=", "value": int64(42)},
			map[string]any{"column": "deleted", "operator": "=", "value": false},
		},
		"columns":   []any{"temperature"},
		"timeRange": []any{"2026-09-14T09:57:43Z", nil},
	}
	if got := f.own.lastCall(t).Args[0]; !reflect.DeepEqual(got, wantPayload) {
		t.Errorf("payload %#v\nwant %#v", got, wantPayload)
	}
	if !reflect.DeepEqual(*q, before) {
		t.Error("the caller's query was modified")
	}
}

func TestGetHistoryResultShapes(t *testing.T) {
	f := flock(t)
	answer(f.own, nil)
	rows, err := f.GetHistory(bg, "t", nil)
	if err != nil || rows == nil || len(rows) != 0 {
		t.Errorf("no result: %#v, %v (want an empty, non-nil slice)", rows, err)
	}
	f.own.callFn = func(fakeCall) (*wamp.Result, error) { return &wamp.Result{}, nil }
	if rows, err := f.GetHistory(bg, "t", nil); err != nil || len(rows) != 0 {
		t.Errorf("no args: %#v, %v", rows, err)
	}
	for _, bad := range []any{"call-result", map[string]any{"rows": 1}, []any{map[string]any{}, "x"}} {
		answer(f.own, bad)
		_, err := f.GetHistory(bg, "t", nil)
		var oerr *OperationError
		if !errors.As(err, &oerr) || !errors.Is(err, errUnexpectedResult) {
			t.Errorf("%#v: %v", bad, err)
			continue
		}
		mustContain(t, err.Error(), "getHistory('t') failed: unexpected result type")
	}
}

func TestGetHistoryValidation(t *testing.T) {
	f := flock(t)
	_, err := f.GetHistory(bg, "", nil)
	if !errors.Is(err, ErrInvalidArgument) || !strings.HasSuffix(err.Error(), "Tablename must not be empty!") {
		t.Errorf("empty table: %v", err)
	}
	_, err = f.GetHistory(bg, "t", &TableQueryParams{Limit: 0})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("limit 0: %v", err)
	}
	mustContain(t, err.Error(), "Invalid query parameters: limit must be between 1 and 10000")
	_, err = f.GetHistory(bg, "t", &TableQueryParams{Limit: 1, FilterAnd: []Filter{{Column: "temperature"}}})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("incomplete predicate: %v", err)
	}
	if len(f.own.allCalls()) != 0 {
		t.Error("an invalid query reached the connection")
	}
}

func TestGetHistoryExplainsAMissingProcedure(t *testing.T) {
	f := flock(t)
	fail(f.own, wampErr(wamp.URINoSuchProcedure))
	_, err := f.GetHistory(bg, "missing_table", nil)
	want := "getHistory('missing_table') failed: history procedure 'history.transformed.missing_table' is not registered. " +
		"Check that the table is declared in the app's data-template and that the app's data backend is running."
	if err == nil || err.Error() != want {
		t.Errorf("error %v\nwant %q", err, want)
	}
	if WampURI(err) != wamp.URINoSuchProcedure {
		t.Error("the WAMP error must stay reachable")
	}

	fail(f.own, errors.New("no callee registered for procedure"))
	_, err = f.GetHistory(bg, "missing_table", nil)
	mustContain(t, err.Error(), "history procedure 'history.transformed.missing_table' is not registered")

	fail(f.own, errors.New("boom"))
	_, err = f.GetHistory(bg, "mytable", nil)
	if want := "getHistory('mytable') failed: boom"; err == nil || err.Error() != want {
		t.Errorf("error %v, want %q", err, want)
	}

	fail(f.own, wampErr("wamp.error.not_authorized", "nope"))
	_, err = f.GetHistory(bg, "mytable", nil)
	if want := `getHistory('mytable') failed with WAMP error 'wamp.error.not_authorized' — ["nope"]`; err == nil || err.Error() != want {
		t.Errorf("error %v, want %q", err, want)
	}
}

func TestGetSeriesHistory(t *testing.T) {
	f := flock(t)
	answer(f.own, []any{map[string]any{"temp": 1.5}})
	q := SeriesQueryParams{
		Metrics:   []string{"temperature"},
		Method:    MethodAvg,
		Limit:     100,
		TimeRange: &TimeRange{Start: "2026-01-01T00:00:00.000Z"},
		FilterAnd: []Filter{Where("device_key", "=", 42)},
	}
	rows, err := f.GetSeriesHistory(bg, "sensordata", q)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Errorf("rows %#v", rows)
	}
	want := fakeCall{
		Procedure: "history.transformed.series.sensordata",
		Args: []any{map[string]any{
			"metrics":   []any{"temperature"},
			"method":    "AVG",
			"limit":     int64(100),
			"timeRange": []any{"2026-01-01T00:00:00.000Z", nil},
			"filterAnd": []any{map[string]any{"column": "device_key", "operator": "=", "value": int64(42)}},
		}},
		Window: DefaultReconnectWindow,
	}
	if got := f.own.lastCall(t); !reflect.DeepEqual(got, want) {
		t.Errorf("call %#v\nwant %#v", got, want)
	}

	fail(f.own, errors.New("no callee registered for procedure"))
	_, err = f.GetSeriesHistory(bg, "missing_table", q)
	mustContain(t, err.Error(), "getSeriesHistory('missing_table') failed: history procedure 'history.transformed.series.missing_table' is not registered")
}

func TestGetSeriesHistoryValidation(t *testing.T) {
	f := flock(t)
	base := SeriesQueryParams{Metrics: []string{"t"}, Method: MethodAvg, Limit: 10, TimeRange: &TimeRange{}}
	if _, err := f.GetSeriesHistory(bg, "", base); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("empty table: %v", err)
	}
	for name, mutate := range map[string]func(*SeriesQueryParams){
		"method":     func(q *SeriesQueryParams) { q.Method = "MEDIAN" },
		"lower case": func(q *SeriesQueryParams) { q.Method = "avg" },
		"limit":      func(q *SeriesQueryParams) { q.Limit = 10001 },
		"time range": func(q *SeriesQueryParams) { q.TimeRange = nil },
		"mixed":      func(q *SeriesQueryParams) { q.TimeRange = &TimeRange{Start: "2026-01-01T00:00:00Z", End: int64(1)} },
	} {
		q := base
		mutate(&q)
		_, err := f.GetSeriesHistory(bg, "t", q)
		if !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), "Invalid series query parameters") {
			t.Errorf("%s: %v", name, err)
		}
	}
	for _, filter := range []Filter{Latest(), Where("latest_flag", "=", true)} {
		q := base
		q.FilterAnd = []Filter{filter}
		_, err := f.GetSeriesHistory(bg, "t", q)
		if !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), "not supported in series queries") {
			t.Errorf("latest marker %#v: %v", filter, err)
		}
	}
	if len(f.own.allCalls()) != 0 {
		t.Error("an invalid query reached the connection")
	}
}

func TestRevealSecrets(t *testing.T) {
	f := flock(t)
	answer(f.own, []any{map[string]any{"api_token": "hunter2"}})
	rows, err := f.RevealSecrets(bg, "credentials", &TableQueryParams{Limit: 5, FilterAnd: []Filter{Latest()}})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rows, []Row{{"api_token": "hunter2"}}) {
		t.Errorf("rows %#v", rows)
	}
	want := fakeCall{
		Procedure: "secret.reveal.credentials",
		Args:      []any{map[string]any{"limit": int64(5), "offset": int64(0), "filterAnd": []any{map[string]any{"latest": true}}}},
		Window:    DefaultReconnectWindow,
	}
	if got := f.own.lastCall(t); !reflect.DeepEqual(got, want) {
		t.Errorf("call %#v\nwant %#v", got, want)
	}

	if _, err := f.RevealSecrets(bg, "credentials", nil); err != nil {
		t.Fatal(err)
	}
	if got := f.own.lastCall(t).Args[0]; !reflect.DeepEqual(got, map[string]any{"limit": int64(10), "offset": int64(0)}) {
		t.Errorf("default payload %#v", got)
	}

	calls := len(f.own.allCalls())
	_, err = f.RevealSecrets(bg, "credentials", &TableQueryParams{Limit: 101})
	if !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), "Invalid query parameters: limit must be between 1 and 100") {
		t.Errorf("limit 101: %v", err)
	}
	if _, err := f.RevealSecrets(bg, "", nil); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("empty table: %v", err)
	}
	if len(f.own.allCalls()) != calls {
		t.Error("an invalid query reached the connection")
	}

	fail(f.own, errors.New("no callee registered for procedure"))
	_, err = f.RevealSecrets(bg, "credentials", nil)
	want2 := "revealSecrets('credentials') failed: procedure 'secret.reveal.credentials' is not registered. " +
		"The app's data backend does not support secret columns yet — update it, then declare the column with `secret: true` in the app's data-template."
	if err == nil || err.Error() != want2 {
		t.Errorf("error %v\nwant %q", err, want2)
	}
}

func TestVerifySecret(t *testing.T) {
	f := flock(t)
	answer(f.own, map[string]any{"match": true, "checked": int64(1)})
	res, err := f.VerifySecret(bg, "credentials", "api_key", "s3cr3t", nil)
	if err != nil {
		t.Fatal(err)
	}
	if *res != (SecretVerifyResult{Match: true, Checked: 1}) {
		t.Errorf("result %#v", res)
	}
	want := fakeCall{
		Procedure: "secret.verify.credentials",
		Args:      []any{map[string]any{"limit": int64(1), "offset": int64(0), "column": "api_key", "candidate": "s3cr3t"}},
		Window:    DefaultReconnectWindow,
	}
	if got := f.own.lastCall(t); !reflect.DeepEqual(got, want) {
		t.Errorf("call %#v\nwant %#v", got, want)
	}

	q := &TableQueryParams{Limit: 3, FilterAnd: []Filter{Latest()}}
	if _, err := f.VerifySecret(bg, "credentials", "api_key", "", q); err != nil {
		t.Fatal(err)
	}
	wantPayload := map[string]any{
		"limit": int64(3), "offset": int64(0), "filterAnd": []any{map[string]any{"latest": true}},
		"column": "api_key", "candidate": "",
	}
	if got := f.own.lastCall(t).Args[0]; !reflect.DeepEqual(got, wantPayload) {
		t.Errorf("payload %#v", got)
	}
	if q.Limit != 3 || len(q.FilterAnd) != 1 {
		t.Error("the caller's query was modified")
	}
}

func TestVerifySecretFailsClosed(t *testing.T) {
	f := flock(t)
	cases := []struct {
		answer any
		want   SecretVerifyResult
	}{
		{map[string]any{"match": false, "checked": int64(3)}, SecretVerifyResult{Checked: 3}},
		{map[string]any{"match": false, "checked": int64(0)}, SecretVerifyResult{}},
		{map[string]any{"match": true, "checked": 2.0}, SecretVerifyResult{Match: true, Checked: 2}},
		{map[string]any{"match": true}, SecretVerifyResult{Match: true}},
		{map[string]any{"match": "true", "checked": int64(1)}, SecretVerifyResult{Checked: 1}},
		{map[string]any{"match": int64(1)}, SecretVerifyResult{}},
		{map[string]any{"checked": "many"}, SecretVerifyResult{}},
		{"call-result", SecretVerifyResult{}},
		{[]any{map[string]any{"match": true}}, SecretVerifyResult{}},
		{nil, SecretVerifyResult{}},
	}
	for _, tc := range cases {
		answer(f.own, tc.answer)
		res, err := f.VerifySecret(bg, "credentials", "api_key", "x", nil)
		if err != nil {
			t.Fatal(err)
		}
		if *res != tc.want {
			t.Errorf("answer %#v: %#v, want %#v", tc.answer, *res, tc.want)
		}
	}
}

func TestVerifySecretValidationAndErrors(t *testing.T) {
	f := flock(t)
	_, err := f.VerifySecret(bg, "credentials", "", "x", nil)
	if !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), "Invalid verify parameters") {
		t.Errorf("empty column: %v", err)
	}
	_, err = f.VerifySecret(bg, "credentials", "api_key", "x", &TableQueryParams{Limit: 500})
	if !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), "Invalid verify parameters") {
		t.Errorf("limit 500: %v", err)
	}
	if len(f.own.allCalls()) != 0 {
		t.Error("an invalid query reached the connection")
	}
	fail(f.own, wampErr(wamp.URINoSuchProcedure))
	_, err = f.VerifySecret(bg, "credentials", "api_key", "x", nil)
	mustContain(t, err.Error(), "verifySecret('credentials') failed: procedure 'secret.verify.credentials' is not registered.",
		"does not support secret columns yet")
}
