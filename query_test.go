package ironflock

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestQueryWireMinimal(t *testing.T) {
	got, err := queryWire(&TableQueryParams{Limit: 10}, MaxQueryLimit, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"limit": int64(10), "offset": int64(0)}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestQueryWireFull(t *testing.T) {
	q := &TableQueryParams{
		Limit:     500,
		Offset:    20,
		TimeRange: &TimeRange{Start: "2026-01-01T00:00:00Z", End: nil},
		FilterAnd: []Filter{
			Where("temperature", ">", 20),
			Latest(),
			Or(IsNull("deleted"), Where("deleted", "=", false)),
			Where("id", "IN", []int{1, 2}),
		},
		Columns: []string{"temperature"},
	}
	got, err := queryWire(q, MaxQueryLimit, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"limit":     int64(500),
		"offset":    int64(20),
		"timeRange": []any{"2026-01-01T00:00:00Z", nil},
		"filterAnd": []any{
			map[string]any{"column": "temperature", "operator": ">", "value": int64(20)},
			map[string]any{"latest": true},
			map[string]any{"combinator": "OR", "filters": []any{
				map[string]any{"column": "deleted", "operator": "IS NULL"},
				map[string]any{"column": "deleted", "operator": "=", "value": false},
			}},
			map[string]any{"column": "id", "operator": "IN", "value": []any{int64(1), int64(2)}},
		},
		"columns": []any{"temperature"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got  %#v\nwant %#v", got, want)
	}
}

func TestQueryWireRejects(t *testing.T) {
	cases := map[string]*TableQueryParams{
		"zero limit":         {Limit: 0},
		"limit too large":    {Limit: 10001},
		"negative offset":    {Limit: 1, Offset: -1},
		"mixed time range":   {Limit: 1, TimeRange: &TimeRange{Start: "2026-01-01T00:00:00Z", End: 5}},
		"bad iso":            {Limit: 1, TimeRange: &TimeRange{Start: "yesterday"}},
		"empty column":       {Limit: 1, FilterAnd: []Filter{{Operator: "=", Value: 1}}},
		"missing value":      {Limit: 1, FilterAnd: []Filter{Where("a", "=", nil)}},
		"IN needs list":      {Limit: 1, FilterAnd: []Filter{Where("a", "IN", 3)}},
		"list for =":         {Limit: 1, FilterAnd: []Filter{Where("a", "=", []int{1})}},
		"IS NULL with value": {Limit: 1, FilterAnd: []Filter{{Column: "a", Operator: "IS NULL", Value: 1}}},
		"bad combinator":     {Limit: 1, FilterAnd: []Filter{{Combinator: "XOR", Filters: []Filter{IsNull("a")}}}},
		"empty group":        {Limit: 1, FilterAnd: []Filter{Or()}},
	}
	for name, q := range cases {
		if _, err := queryWire(q, MaxQueryLimit, nil); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%s: want ErrInvalidArgument, got %v", name, err)
		}
	}
	if _, err := queryWire(&TableQueryParams{Limit: 101}, MaxSecretLimit, nil); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("secret limit: want ErrInvalidArgument, got %v", err)
	}
}

func TestTimeRangeForms(t *testing.T) {
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.FixedZone("x", 3600))
	got, err := timeRangeWire(Since(start))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []any{"2026-01-02T02:04:05Z", nil}) {
		t.Fatalf("got %#v", got)
	}
	got, err = timeRangeWire(&TimeRange{Start: int64(1700000000000), End: uint64(1700000001000)})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []any{int64(1700000000000), int64(1700000001000)}) {
		t.Fatalf("got %#v", got)
	}
	for _, s := range []string{"2026-01-01", "2026-01-01T10:00:00", "2026-01-01T10:00:00.123+02:00", "2026-01-01 10:00:00"} {
		if _, err := timeRangeWire(&TimeRange{Start: s}); err != nil {
			t.Errorf("%q: %v", s, err)
		}
	}
}

func TestSeriesWire(t *testing.T) {
	got, err := seriesWire(&SeriesQueryParams{
		Metrics:   []string{"temperature"},
		Method:    MethodAvg,
		Limit:     500,
		TimeRange: &TimeRange{Start: "2026-01-01T00:00:00Z", End: "2026-03-01T00:00:00Z"},
		GroupBy:   []string{"device_id"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"metrics":   []any{"temperature"},
		"method":    "AVG",
		"limit":     int64(500),
		"timeRange": []any{"2026-01-01T00:00:00Z", "2026-03-01T00:00:00Z"},
		"groupBy":   []any{"device_id"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v", got)
	}
	bad := []SeriesQueryParams{
		{Metrics: []string{"a"}, Method: "MEDIAN", Limit: 1, TimeRange: &TimeRange{}},
		{Metrics: []string{"a"}, Method: MethodAvg, Limit: 1},
		{Metrics: []string{"a"}, Method: MethodAvg, Limit: 1, TimeRange: &TimeRange{}, FilterAnd: []Filter{Latest()}},
		{Metrics: []string{"a"}, Method: MethodAvg, Limit: 1, TimeRange: &TimeRange{}, FilterAnd: []Filter{Where("latest_flag", "=", true)}},
		{Metrics: []string{"a"}, Method: MethodAvg, Limit: 1, TimeRange: &TimeRange{}, FilterAnd: []Filter{Or(IsNull("a"))}},
	}
	for i, p := range bad {
		if _, err := seriesWire(&p, nil); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("case %d: want ErrInvalidArgument, got %v", i, err)
		}
	}
}

type sensorRow struct {
	Temperature float64   `json:"temperature"`
	Count       int       `json:"count"`
	At          time.Time `json:"at"`
	Skip        string    `json:"-"`
}

func TestSplitArgsAndNormalize(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	pos, kw, opts, err := splitArgs([]any{
		sensorRow{Temperature: 22.5, Count: 3, At: at},
		7,
		Kwargs{"a": 1},
		CallOptions{DiscloseMe: true},
		Kwargs{"b": []string{"x"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	wantPos := []any{
		map[string]any{"temperature": 22.5, "count": int64(3), "at": "2026-01-01T00:00:00Z"},
		int64(7),
	}
	if !reflect.DeepEqual(pos, wantPos) {
		t.Fatalf("pos %#v", pos)
	}
	if !reflect.DeepEqual(kw, map[string]any{"a": int64(1), "b": []any{"x"}}) {
		t.Fatalf("kw %#v", kw)
	}
	if opts == nil || !opts.DiscloseMe {
		t.Fatalf("opts %#v", opts)
	}
}

func TestNormalizeRows(t *testing.T) {
	rows, err := normalizeRows([]sensorRow{{Temperature: 1}, {Temperature: 2}})
	if err != nil {
		t.Fatal(err)
	}
	// Integral floats travel as integers after the JSON round trip, as they
	// do from the JavaScript SDK.
	if len(rows) != 2 || rows[1].(map[string]any)["temperature"] != int64(2) {
		t.Fatalf("rows %#v", rows)
	}
	for _, bad := range []any{nil, []Row{}, []int{1}, "x", Row{"a": 1}} {
		if _, err := normalizeRows(bad); err == nil {
			t.Errorf("%#v: want error", bad)
		}
	}
	if _, err := normalizeRows([]map[string]any{{"a": 1}}); err != nil {
		t.Fatal(err)
	}
}

func TestTimeRangeHelpers(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	cases := []struct {
		tr   *TimeRange
		want []any
	}{
		{Between(start, end), []any{"2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"}},
		{Since(start), []any{"2026-01-01T00:00:00Z", nil}},
		{Until(end), []any{nil, "2026-01-01T01:00:00Z"}},
		{&TimeRange{Start: &start, End: (*time.Time)(nil)}, []any{"2026-01-01T00:00:00Z", nil}},
		{&TimeRange{}, []any{nil, nil}},
	}
	for i, tc := range cases {
		got, err := timeRangeWire(tc.tr)
		if err != nil || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("case %d: %#v, %v", i, got, err)
		}
	}
	for _, bad := range []any{true, []string{"2026-01-01"}, struct{}{}} {
		if _, err := timeRangeWire(&TimeRange{Start: bad}); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%#v: %v", bad, err)
		}
	}
}
