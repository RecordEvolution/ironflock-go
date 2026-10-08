package ironflock

import (
	"errors"
	"reflect"
	"strings"
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

// isoTimeCase is a TimeRange string the data backend reads, with the time
// value (ms since the epoch) it reads it as.
type isoTimeCase struct {
	s  string
	ms int64
}

// The data backend (fleetdb) turns each string bound of a TimeRange into a
// time with JavaScript's new Date(s).getTime(). It runs on Bun 1.3.13 (the
// oven/bun:1.3.13 image of fleetdb-service's Dockerfile), which reads every
// string below exactly as V8 does: the values were recorded with
// new Date(s).getTime() in the oven/bun:1.3.13, node:20 and node:22 images,
// in the time zone the data backend runs in, UTC.
var (
	// acceptedISOTimes are ISO 8601 dates and date-times in extended format
	// that the data backend reads correctly, with the time value it reads.
	acceptedISOTimes = []isoTimeCase{
		{"2026", 1767225600000},
		{"2026-01", 1767225600000},
		{"2026-02", 1769904000000},
		{"2026-12", 1796083200000},
		{"2026-01-01", 1767225600000},
		{"2026-06-30", 1782777600000},
		{"0000", -62167219200000},
		{"0000-01", -62167219200000},
		{"0000-01-01", -62167219200000},
		{"9999-12-31", 253402214400000},
		{"2026-01-01T10:00", 1767261600000},
		{"2026-01-01T10:00Z", 1767261600000},
		{"2026-01-01T10:00+02:00", 1767254400000},
		{"2026-01-01T10:00+0200", 1767254400000},
		{"2026-01-01T10:00:00", 1767261600000},
		{"2026-01-01T10:00:00Z", 1767261600000},
		{"2026-01-01T00:00:00.000Z", 1767225600000},
		{"1969-12-31T23:59:59.999Z", -1},
		{"1970-01-01T00:00:00.000+00:00", 0},
		{"0000-01-01T00:00:00Z", -62167219200000},
		{"0001-01-01T00:00:00Z", -62135596800000},
		{"9999-12-31T23:59:59Z", 253402300799000},
		// Leap days, in leap years only.
		{"2028-02-29T00:00:00Z", 1835395200000},
		{"2024-02-29T10:00Z", 1709200800000},
		{"2000-02-29T00:00:00Z", 951782400000},
		// Fractions of any length; only milliseconds count.
		{"2026-01-01T10:00:00.5", 1767261600500},
		{"2026-01-01T10:00:00.5Z", 1767261600500},
		{"2026-01-01T10:00:00.1Z", 1767261600100},
		{"2026-01-01T10:00:00.12Z", 1767261600120},
		{"2026-01-01T10:00:00.123Z", 1767261600123},
		{"2026-01-01T10:00:00.1234Z", 1767261600123},
		{"2026-01-01T10:00:00.123456Z", 1767261600123},
		{"2026-01-01T10:00:00.123456789Z", 1767261600123},
		{"2026-01-01T10:00:00.1234567891Z", 1767261600123},
		{"2026-01-01T10:00:00.999999Z", 1767261600999},
		{"2026-01-01T10:00:00.9999999999Z", 1767261600999},
		{"2026-01-01T10:00:00.12345678901234567890Z", 1767261600123},
		{"2026-01-01T10:00:00.12345678901234567890", 1767261600123},
		{"2026-01-01T10:00:00.123456789012345678901234567890123456789012345678901234567890Z", 1767261600123},
		// UTC offsets, with or without a colon.
		{"2026-01-01T10:00:00+02:00", 1767254400000},
		{"2026-01-01T10:00:00-05:30", 1767281400000},
		{"2026-01-01T10:00:00+00:00", 1767261600000},
		{"2026-01-01T10:00:00-00:00", 1767261600000},
		{"2026-01-01T10:00:00+14:00", 1767211200000},
		{"2026-01-01T10:00:00-12:00", 1767304800000},
		{"2026-01-01T10:00:00+23:59", 1767175260000},
		{"2026-01-01T10:00:00.123+02:00", 1767254400123},
		{"2026-01-01T10:00:00.123456789-05:30", 1767281400123},
		{"2026-01-01T10:00:00.5+02:00", 1767254400500},
		{"2026-01-01T10:00:00+0200", 1767254400000},
		{"2026-01-01T10:00:00+0000", 1767261600000},
		{"2026-01-01T10:00:00-0530", 1767281400000},
		{"2026-01-01T10:00:00+0130", 1767256200000},
		{"2026-01-01T10:00:00+2359", 1767175260000},
		{"2026-01-01T10:00:00-2359", 1767347940000},
		{"2026-01-01T10:00:00.123+0200", 1767254400123},
		{"2026-01-01T10:00:00.5+0200", 1767254400500},
		{"2026-01-01T10:00:00.5-0000", 1767261600500},
		// A space or a lower-case t between date and time, a lower-case z.
		{"2026-01-01 10:00", 1767261600000},
		{"2026-01-01 10:00:00", 1767261600000},
		{"2026-01-01 10:00:00Z", 1767261600000},
		{"2026-01-01 10:00:00+02:00", 1767254400000},
		{"2026-01-01 10:00:00+0200", 1767254400000},
		{"2026-01-01 10:00:00.123Z", 1767261600123},
		{"2026-01-01 10:00:00.5", 1767261600500},
		{"2026-01-01 10:00:00.5+02:00", 1767254400500},
		{"2026-01-01 10:00:00z", 1767261600000},
		{"2026-01-01 10:00z", 1767261600000},
		{"2026-01-01t10:00", 1767261600000},
		{"2026-01-01t10:00:00z", 1767261600000},
		{"2026-01-01t10:00:00Z", 1767261600000},
		{"2026-01-01T10:00:00z", 1767261600000},
		{"2026-01-01T10:00z", 1767261600000},
		{"2026-01-01T10:00:00.123z", 1767261600123},
		// 24:00, the end of the day.
		{"2026-01-01T24:00", 1767312000000},
		{"2026-01-01T24:00Z", 1767312000000},
		{"2026-01-01T24:00:00", 1767312000000},
		{"2026-01-01T24:00:00Z", 1767312000000},
		{"2026-01-01T24:00:00.0", 1767312000000},
		{"2026-01-01T24:00:00.000Z", 1767312000000},
		{"2026-01-01 24:00:00Z", 1767312000000},
		{"2026-12-31T24:00:00Z", 1798761600000},
		{"2026-12-31T24:00:00+02:00", 1798754400000},
		{"9999-12-31T24:00:00Z", 253402300800000},
		// Six-digit years with a sign, within JavaScript's Date range
		// (±8.64e15 ms).
		{"+002026", 1767225600000},
		{"+002026-01", 1767225600000},
		{"+002026-01-01", 1767225600000},
		{"+002026-01-01T00:00:00Z", 1767225600000},
		{"+002026-01-01T10:00:00", 1767261600000},
		{"+002026-01-01T10:00:00.5+02:00", 1767254400500},
		{"+000000-01-01T00:00:00Z", -62167219200000},
		{"-000001", -62198755200000},
		{"-000001-01-01T00:00:00Z", -62198755200000},
		{"+010000-01-01T00:00:00Z", 253402300800000},
		{"+275760-09-13", 8640000000000000},
		{"+275760-09-13T00:00:00", 8640000000000000},
		{"+275760-09-13T00:00:00Z", 8640000000000000},
		{"+275760-09-13T00:00:00.0001Z", 8640000000000000},
		{"+275760-09-13T00:00:00.000999Z", 8640000000000000},
		{"+275760-09-12T24:00:00Z", 8640000000000000},
		{"+275760-09-13T01:00:00+02:00", 8639999996400000},
		{"-271821-04-20", -8640000000000000},
		{"-271821-04-20T00:00:00", -8640000000000000},
		{"-271821-04-20T00:00:00Z", -8640000000000000},
		{"-271821-04-19T24:00:00", -8640000000000000},
		{"-271821-04-19T24:00:00Z", -8640000000000000},
	}

	// rejectedISOTimes are strings the data backend cannot read (new Date
	// returns an invalid date) or reads as another time than they denote,
	// and strings that are not ISO 8601 in extended format although
	// JavaScript reads them.
	rejectedISOTimes = []string{
		"", "not-a-date", " ", "2026-01-01T", "2026-01-01TZ",
		// Basic format, week and ordinal dates.
		"20260101", "20260101T100000Z", "20260101T10:00:00Z", "2026-01-01T100000Z",
		"2026-W01-1", "2026-001", "2026-032", "2026-060",
		// Hours only, a missing digit, a fraction of a minute or an hour.
		"2026-01-01T10", "2026-01-01T10Z", "2026-01-01 10", "2026-01-01 10Z",
		"2026-01-01T1:00:00Z", "2026-01-01T10:0:00Z", "2026-01-01T10.5", "2026-01-01T10:30.5",
		// Fractions without digits, with a comma, with a sign or other digits.
		"2026-01-01T10:00:00.Z", "2026-01-01T10:00:00,5Z", "2026-01-01T10:00:00.-5Z",
		"2026-01-01T10:00:00.+5Z", "2026-01-01T10:00:00.５Z",
		// Offsets of hours only, with seconds, out of range, malformed.
		"2026-01-01T10:00:00+02", "2026-01-01T10:00:00-05", "2026-01-01T10:00:00+00:00:00",
		"2026-01-01T10:00:00+24:00", "2026-01-01T10:00:00+25:00", "2026-01-01T10:00:00+02:60",
		"2026-01-01T10:00:00+0060", "2026-01-01T10:00:00+2400", "2026-01-01T10:00:00+01:3",
		"2026-01-01T10:00:00+013", "2026-01-01T10:00:00+01300", "2026-01-01T10:00:00+2:00",
		"2026-01-01T10:00:00GMT", "2026-01-01T10:00:00UTC", "2026-01-01T10:00:00+00:00Z",
		"2026-01-01T10:00:00ZZ", "2026-01-01T10:00:00Z+02:00",
		// Out-of-range fields; leap seconds.
		"2026-00", "2026-13", "2026-13-01T00:00:00Z", "2026-00-01T00:00:00Z", "2026-01-00T00:00:00Z",
		"2026-01-32T00:00:00Z", "2026-01-01T25:00:00Z", "2026-01-01T10:60:00Z", "2026-01-01T10:00:60Z",
		"2026-01-01T23:59:60Z", "2026-01-01T24:00:00.001Z", "2026-01-01T24:00:01Z", "2026-01-01T24:01:00Z",
		// Days a month does not have: JavaScript moves them into the next
		// month (2026-02-30 reads as 2026-03-02).
		"2026-02-29", "2026-02-29T10:00Z", "2026-02-29T00:00:00Z", "2026-02-30T00:00:00Z",
		"2026-02-30 00:00:00Z", "2026-04-31T00:00:00Z", "2026-06-31", "2100-02-29T00:00:00Z",
		"1900-02-29T00:00:00Z",
		// Not ISO 8601, although JavaScript reads them: an offset without a
		// time, a time after a reduced date, missing zeros, other spaces and
		// separators, other digits.
		"2026-01-01Z", "2026-01Z", "2026Z", "2026-01-01+02:00", "2026-01T10:00Z", "2026T10:00Z",
		"2026-01T10:00", "2026-1-1", "2026-01-1", "2026-01-01  10:00:00Z", "2026-01-01_10:00:00Z",
		"2026-01-01x10:00:00Z", " 2026-01-01T10:00:00Z", "2026-01-01T10:00:00Z ", "2026-01-01T10:00:00Z\n",
		"2026-01-01T10:00:00\tZ", "2026-01-01T10:00:00 Z", "2026-01-01T10:00:00 +02:00",
		"2026-01-01T10:00:00+02:00 ", "２０２６-01-01",
		// Years: four digits, or six with a sign (never -000000), within
		// JavaScript's Date range. Go's formatting of years beyond 9999 or
		// before 0000 is not ISO 8601 either.
		"-000000-01-01T00:00:00Z", "+2026-01-01T00:00:00Z", "002026-01-01T00:00:00Z",
		"+0002026-01-01T00:00:00Z", "10000-01-01T00:00:00Z", "-0001-01-01T00:00:00Z",
		"+275760-09-13T00:00:00.001Z", "+275760-09-13T00:00:00-00:01", "+275760-09-14",
		"+275760-09-12T24:00:00.5Z", "-271821-04-19T23:59:59.999Z", "-271821-04-20T00:00:00+00:01",
		"-271821-04-19",
	}
)

// A string bound of a TimeRange is accepted exactly when the data backend
// reads it correctly, and sent as given, byte for byte (as the Python and
// JavaScript SDKs send it).
func TestTimeRangeStringsAreThoseTheDataBackendReads(t *testing.T) {
	for _, c := range acceptedISOTimes {
		got, err := timeRangeWire(&TimeRange{Start: c.s, End: c.s})
		if err != nil {
			t.Errorf("%q: %v", c.s, err)
			continue
		}
		if got[0] != any(c.s) || got[1] != any(c.s) {
			t.Errorf("%q sent as %#v", c.s, got)
		}
	}
	for _, s := range rejectedISOTimes {
		_, err := timeRangeWire(&TimeRange{Start: "2026-01-01T00:00:00Z", End: s})
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%q: %v, want ErrInvalidArgument", s, err)
			continue
		}
		if want := "ironflock: invalid argument: Invalid ISO datetime format: " + s; err.Error() != want {
			t.Errorf("%q: message %q, want %q", s, err, want)
		}
	}
}

// isoTime reads every accepted string as the time JavaScript's Date reads it
// as, and rejects the others.
func TestISOTimeAgreesWithJavaScriptDate(t *testing.T) {
	for _, c := range acceptedISOTimes {
		if ms, ok := isoTime(c.s); !ok || ms != c.ms {
			t.Errorf("isoTime(%q) = %d, %v; Date reads %d", c.s, ms, ok, c.ms)
		}
	}
	for _, s := range rejectedISOTimes {
		if ms, ok := isoTime(s); ok {
			t.Errorf("isoTime(%q) = %d, want rejected", s, ms)
		}
	}
}

// daysFromCivil agrees with Go's proleptic Gregorian calendar over the whole
// range of JavaScript's Date, and isoTime with time.Time's arithmetic.
func TestISOTimeArithmetic(t *testing.T) {
	for y := int64(minDateYear); y <= maxDateYear; y += 997 {
		for _, md := range [][2]int64{{1, 1}, {2, 28}, {2, 29}, {3, 1}, {12, 31}} {
			if md[1] > daysIn(y, md[0]) {
				continue
			}
			want := time.Date(int(y), time.Month(md[0]), int(md[1]), 0, 0, 0, 0, time.UTC).Unix() / 86400
			if got := daysFromCivil(y, md[0], md[1]); got != want {
				t.Fatalf("daysFromCivil(%d, %d, %d) = %d, want %d", y, md[0], md[1], got, want)
			}
		}
	}
	for y, want := range map[int64]int64{-400: 29, -100: 28, -4: 29, -1: 28, 0: 29, 4: 29, 100: 28, 1900: 28,
		2000: 29, 2024: 29, 2026: 28, 2100: 28, 2400: 29} {
		if got := daysIn(y, 2); got != want {
			t.Errorf("daysIn(%d, 2) = %d, want %d", y, got, want)
		}
	}
	at := time.Date(-12345, 6, 7, 8, 9, 10, 987654321, time.FixedZone("", -(3*3600+30*60)))
	ms, ok := isoTime("-012345-06-07T08:09:10.987654321-03:30")
	if !ok || ms != at.UnixMilli() {
		t.Errorf("isoTime = %d, %v; want %d", ms, ok, at.UnixMilli())
	}
}

// time.Time bounds are sent as RFC 3339 in UTC; outside the years 0000-9999
// with the six-digit signed year JavaScript reads (as Date.toISOString writes
// it). A time beyond JavaScript's Date range cannot be sent.
func TestTimeRangeTimeBoundsTheDataBackendReads(t *testing.T) {
	east := time.FixedZone("east", 2*3600)
	cases := []struct {
		t    time.Time
		want string
	}{
		{time.Date(2026, 1, 1, 2, 0, 0, 500, east), "2026-01-01T00:00:00.0000005Z"},
		{time.Date(9999, 12, 31, 23, 59, 59, 999e6, time.UTC), "9999-12-31T23:59:59.999Z"},
		{time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), "0000-01-01T00:00:00Z"},
		{time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC), "+010000-01-01T00:00:00Z"},
		{time.Date(10000, 1, 1, 1, 0, 0, 0, east), "9999-12-31T23:00:00Z"}, // the year in UTC counts
		{time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC), "-000001-01-01T00:00:00Z"},
		{time.Date(-1, 6, 15, 12, 30, 0, 25e7, time.UTC), "-000001-06-15T12:30:00.25Z"},
		{time.Date(275760, 9, 13, 0, 0, 0, 0, time.UTC), "+275760-09-13T00:00:00Z"},
		{time.Date(275760, 9, 13, 0, 0, 0, 999999, time.UTC), "+275760-09-13T00:00:00.000999999Z"},
		{time.Date(-271821, 4, 20, 0, 0, 0, 0, time.UTC), "-271821-04-20T00:00:00Z"},
	}
	for _, c := range cases {
		got, err := timeRangeWire(&TimeRange{Start: c.t, End: &c.t})
		if err != nil {
			t.Errorf("%v: %v", c.t, err)
			continue
		}
		if got[0] != any(c.want) || got[1] != any(c.want) {
			t.Errorf("%v sent as %#v, want %q", c.t, got, c.want)
		}
		// The data backend reads it as the same time (to the millisecond).
		if ms, ok := isoTime(c.want); !ok || ms != c.t.UnixMilli() {
			t.Errorf("%q reads as %d, %v; want %d", c.want, ms, ok, c.t.UnixMilli())
		}
	}
	for _, tt := range []time.Time{
		time.Date(275760, 9, 13, 0, 0, 0, 1e6, time.UTC),
		time.Date(-271821, 4, 19, 23, 59, 59, 999999999, time.UTC),
		time.Date(300000, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Unix(1<<63-62135596801, 999999999), // the largest time.Time
		time.Unix(-1<<63, 0),
	} {
		if _, err := timeRangeWire(&TimeRange{Start: tt}); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%v: %v, want ErrInvalidArgument", tt, err)
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
	at := time.Date(2026, 1, 1, 2, 0, 0, 0, time.FixedZone("EET", 2*3600))
	pos, kw, opts, err := splitArgs([]any{
		sensorRow{Temperature: 22, Count: 3, At: at},
		7,
		Kwargs{"a": 1, "c": 1.0},
		CallOptions{DiscloseMe: true},
		Kwargs{"b": []string{"x"}, "c": sensorRow{Temperature: 1.5}},
	})
	if err != nil {
		t.Fatal(err)
	}
	wantPos := []any{
		map[string]any{"temperature": float64(22), "count": int64(3), "at": "2026-01-01T00:00:00Z"},
		int64(7),
	}
	if !reflect.DeepEqual(pos, wantPos) {
		t.Fatalf("pos %#v", pos)
	}
	wantKw := map[string]any{
		"a": int64(1),
		"b": []any{"x"},
		"c": map[string]any{"temperature": 1.5, "count": int64(0), "at": "0001-01-01T00:00:00Z"},
	}
	if !reflect.DeepEqual(kw, wantKw) {
		t.Fatalf("kw %#v", kw)
	}
	if opts == nil || !opts.DiscloseMe {
		t.Fatalf("opts %#v", opts)
	}
	// An empty Kwargs still sends (empty) keyword arguments.
	if _, kw, _, err := splitArgs([]any{Kwargs{}}); err != nil || kw == nil || len(kw) != 0 {
		t.Fatalf("empty kwargs %#v, %v", kw, err)
	}
}

func TestNormalizeRows(t *testing.T) {
	at := time.Date(2026, 1, 1, 1, 0, 0, 0, time.FixedZone("CET", 3600))
	rows, err := normalizeRows([]sensorRow{{Temperature: 1, At: at}, {Temperature: 2, Count: 5, Skip: "x"}})
	if err != nil {
		t.Fatal(err)
	}
	// A float field stays a float (2.0 is not sent as the integer 2), times
	// are sent in UTC, and the "-" field is left out.
	want := []any{
		map[string]any{"temperature": float64(1), "count": int64(0), "at": "2026-01-01T00:00:00Z"},
		map[string]any{"temperature": float64(2), "count": int64(5), "at": "0001-01-01T00:00:00Z"},
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("rows %#v\nwant %#v", rows, want)
	}
	if rows, err := normalizeRows([]map[string]any{{"a": 1}}); err != nil || !reflect.DeepEqual(rows, []any{map[string]any{"a": int64(1)}}) {
		t.Fatalf("map rows %#v, %v", rows, err)
	}
	for _, c := range []struct {
		rows any
		err  string // "" if the batch is valid
	}{
		{"x", "rows must be a non-empty list of rows, got string"},
		{1, "rows must be a non-empty list of rows, got int"},
		{&[]Row{{"a": 1}}, "rows must be a non-empty list of rows, got *[]map[string]interface {}"},
		{&sensorRow{}, "rows must be a non-empty list of rows, got *ironflock.sensorRow"},
		{[1]int{1}, "row at index 0 must be a map or struct, got int"},
		{[]any{Row{"a": 1}, "x"}, "row at index 1 must be a map or struct, got string"},
		{[]any{Row{"a": 1}, nil}, "row at index 1 must be a map or struct, got nil"},
		{[]Row{nil}, "row at index 0 must be a map or struct, got a nil map[string]interface {}"},
		{[]*sensorRow{nil}, "row at index 0 must be a map or struct, got a nil *ironflock.sensorRow"},
		{[]Row{{"cb": func() {}}}, "row at index 0: cb: unsupported type func()"},
		{[][]float64{{1}}, "row at index 0 must be a map or struct, got []float64"},
		{[]map[bool]string{{true: ""}}, "row at index 0: unsupported type map[bool]string"},
		{[]map[int]string{{1: "a"}}, ""}, // integer keys become strings: a row
		{[2]*sensorRow{{}, {}}, ""},
	} {
		_, err := normalizeRows(c.rows)
		if c.err == "" {
			if err != nil {
				t.Errorf("%#v: %v", c.rows, err)
			}
		} else if err == nil || err.Error() != c.err {
			t.Errorf("%#v: %v\nwant %s", c.rows, err, c.err)
		}
	}
	for _, empty := range []any{nil, []Row{}, [0]Row{}, []any(nil)} {
		if _, err := normalizeRows(empty); err == nil || err.Error() != "rows must be a non-empty list of rows" {
			t.Errorf("%#v: %v", empty, err)
		}
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

// SQLOperators hands out a copy: changing it cannot change which operators
// the SDK treats as known.
func TestSQLOperatorsIsACopy(t *testing.T) {
	want := []string{
		"=", "!=", "<>", ">", "<", ">=", "<=",
		"LIKE", "ILIKE", "NOT LIKE", "NOT ILIKE",
		"IN", "NOT IN", "IS NULL", "IS NOT NULL",
	}
	ops := SQLOperators()
	if !reflect.DeepEqual(ops, want) {
		t.Fatalf("SQLOperators() = %q, want %q", ops, want)
	}
	ops[0] = "REGEXP"
	if got := SQLOperators(); !reflect.DeepEqual(got, want) {
		t.Fatalf("the vocabulary changed through a returned slice: %q", got)
	}
	log, logs := newTestLogger()
	q := &TableQueryParams{Limit: 1, FilterAnd: []Filter{Where("a", "REGEXP", "x"), Where("b", "=", 1)}}
	if _, err := queryWire(q, MaxQueryLimit, log); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(logs.String(), "is not in standard list"); n != 1 {
		t.Fatalf("%d operator warnings, want 1 (for REGEXP):\n%s", n, logs)
	}
}
