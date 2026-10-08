package ironflock

import (
	"fmt"
	"log/slog"
	"math"
	"reflect"
	"slices"
	"strings"
	"time"
)

// SecretPlaceholder is what a secret column reads as on every normal read
// path (GetHistory, SubscribeToTable, cross-app reads): the data backend
// substitutes it for a non-null secret value. Nil stays nil, so "set but
// hidden" remains distinguishable from "never written".
const SecretPlaceholder = "__secret__"

// Query limits enforced by the data backend.
const (
	MaxQueryLimit  = 10000
	MaxSecretLimit = 100
	maxUint32      = math.MaxUint32
)

// sqlOperators is the vocabulary SQLOperators returns.
var sqlOperators = []string{
	"=", "!=", "<>", ">", "<", ">=", "<=",
	"LIKE", "ILIKE", "NOT LIKE", "NOT ILIKE",
	"IN", "NOT IN", "IS NULL", "IS NOT NULL",
}

// SQLOperators returns the data backend's filter operator vocabulary. An
// operator outside it is logged as a warning, not rejected: the server is
// the authority and may accept operators this release predates. The slice
// is a copy; changing it does not change the SDK's check.
func SQLOperators() []string { return slices.Clone(sqlOperators) }

// DownSampleMethod is the aggregation a series query applies per time bucket.
type DownSampleMethod string

// Down-sampling methods.
const (
	MethodAvg   DownSampleMethod = "AVG"
	MethodSum   DownSampleMethod = "SUM"
	MethodCount DownSampleMethod = "COUNT"
	MethodMin   DownSampleMethod = "MIN"
	MethodMax   DownSampleMethod = "MAX"
	MethodFirst DownSampleMethod = "FIRST"
	MethodLast  DownSampleMethod = "LAST"
)

var downSampleMethods = []DownSampleMethod{
	MethodAvg, MethodSum, MethodCount, MethodMin, MethodMax, MethodFirst, MethodLast,
}

// TimeRange is the [start, end] range of a query. Each bound is a date-time
// string, a time.Time, an epoch-milliseconds number, or nil for an open end.
// Strings and times cannot be mixed with numbers in one range.
//
// A string is sent exactly as given. The data backend reads it with
// JavaScript's Date, so it must be an ISO 8601 date or date-time in extended
// format that Date reads as the time it denotes: a date (2026, 2026-07 or
// 2026-07-01), or a date and a time of day (2026-07-01T12:30,
// 2026-07-01T12:30:15 or 2026-07-01T12:30:15.250, the fraction of any length)
// separated by T, t or a space and optionally followed by Z or a UTC offset
// (+02:00 or +0200). A date-time without one is read as UTC. Years outside
// 0000-9999 take six digits and a sign (+010000). Strings Date would read as
// another time or not at all are rejected with ErrInvalidArgument, among them
// basic format (20260701T123000Z), hours without minutes, offsets of hours
// only and days a month does not have (2026-02-30). Fractions of a
// millisecond are dropped.
//
// A time.Time is sent as RFC 3339 in UTC, with the six-digit year outside
// 0000-9999 (as JavaScript's Date.toISOString writes it); one outside the
// range of Date (-271821-04-20 to +275760-09-13) is rejected.
type TimeRange struct {
	Start any
	End   any
}

// Between returns the closed range [start, end].
func Between(start, end time.Time) *TimeRange { return &TimeRange{Start: start, End: end} }

// Since returns the range from start with an open end.
func Since(start time.Time) *TimeRange { return &TimeRange{Start: start} }

// Until returns the range with an open start, ending at end.
func Until(end time.Time) *TimeRange { return &TimeRange{End: end} }

// Filter is one entry of a query's FilterAnd list: a WHERE predicate, the
// latest-row marker, or a group of filters combined with AND or OR. Build
// filters with Where, IsNull, IsNotNull, Latest, And and Or.
type Filter struct {
	// Predicate: Column Operator Value. Value is omitted by IS NULL and
	// IS NOT NULL; IN and NOT IN take a slice (or a comma-joined string).
	Column   string
	Operator string
	Value    any

	// Latest marks the entry as the latest-per-entity mode switch instead of
	// a predicate: the data backend returns only the latest row per entity
	// (the table's maintainLatestFlagFor key; the single most recent row
	// without one). Other entries then narrow or filter those rows.
	Latest bool

	// Combinator ("AND" or "OR") makes the entry a group of Filters. A data
	// backend that predates groups rejects them rather than applying part of
	// the filter.
	Combinator string
	Filters    []Filter
}

// Where returns the predicate "column operator value".
func Where(column, operator string, value any) Filter {
	return Filter{Column: column, Operator: operator, Value: value}
}

// IsNull returns the predicate "column IS NULL".
func IsNull(column string) Filter { return Filter{Column: column, Operator: "IS NULL"} }

// IsNotNull returns the predicate "column IS NOT NULL".
func IsNotNull(column string) Filter { return Filter{Column: column, Operator: "IS NOT NULL"} }

// Latest returns the latest-row marker: only the latest row per entity.
func Latest() Filter { return Filter{Latest: true} }

// Or returns a group matching rows that satisfy any of filters.
func Or(filters ...Filter) Filter { return Filter{Combinator: "OR", Filters: filters} }

// And returns a group matching rows that satisfy all of filters.
func And(filters ...Filter) Filter { return Filter{Combinator: "AND", Filters: filters} }

// TableQueryParams selects rows of a table or transform. A nil
// *TableQueryParams means the method's default ({Limit: 10}, or {Limit: 1}
// for VerifySecret).
type TableQueryParams struct {
	// Limit is the maximum number of rows: 1-10000 (1-100 for the secret
	// procedures, which reject a larger value instead of clamping it).
	Limit int
	// Offset skips rows for pagination.
	Offset int
	// TimeRange restricts the rows' tsp; nil for all time.
	TimeRange *TimeRange
	// FilterAnd entries are AND-ed.
	FilterAnd []Filter
	// Columns to return (tsp, device_key and authid are always included);
	// nil for all columns.
	Columns []string
}

// SeriesQueryParams queries down-sampled time series: numeric columns
// aggregated into time buckets.
type SeriesQueryParams struct {
	// Metrics are the numeric columns to down-sample.
	Metrics []string
	// Method is the aggregation per bucket.
	Method DownSampleMethod
	// Limit is the maximum number of buckets: 1-10000.
	Limit int
	// TimeRange is required.
	TimeRange *TimeRange
	// GroupBy columns split the series.
	GroupBy []string
	// FilterAnd holds WHERE predicates only: the Latest marker and groups
	// are not supported in series queries.
	FilterAnd []Filter
}

// SecretVerifyResult is the answer of VerifySecret.
type SecretVerifyResult struct {
	// Match is true when any selected row's column decrypts to the
	// candidate.
	Match bool
	// Checked is how many rows the selection actually compared.
	Checked int
}

// queryWire validates q and returns the payload sent to the history and
// secret procedures. maxLimit is MaxQueryLimit or MaxSecretLimit. Optional
// parts are omitted rather than sent as null: the data backend rejects an
// explicit null for an optional field.
//
// Filter operators outside SQLOperators are logged to log as a warning.
func queryWire(q *TableQueryParams, maxLimit int, log *slog.Logger) (map[string]any, error) {
	if q == nil {
		return nil, invalidf("query parameters are required")
	}
	if q.Limit < 1 || q.Limit > maxLimit {
		return nil, invalidf("limit must be between 1 and %d, got %d", maxLimit, q.Limit)
	}
	// int64: maxUint32 overflows int on 32-bit platforms (e.g. GOARCH=arm).
	if q.Offset < 0 || int64(q.Offset) > maxUint32 {
		return nil, invalidf("offset must be between 0 and %d, got %d", int64(maxUint32), q.Offset)
	}
	out := map[string]any{
		"limit":  int64(q.Limit),
		"offset": int64(q.Offset),
	}
	if q.TimeRange != nil {
		tr, err := timeRangeWire(q.TimeRange)
		if err != nil {
			return nil, err
		}
		out["timeRange"] = tr
	}
	if len(q.FilterAnd) > 0 {
		filters, err := filtersWire(q.FilterAnd, true, log)
		if err != nil {
			return nil, err
		}
		out["filterAnd"] = filters
	}
	if q.Columns != nil {
		cols := make([]any, len(q.Columns))
		for i, c := range q.Columns {
			if strings.TrimSpace(c) == "" {
				return nil, invalidf("columns[%d] must be a non-empty string", i)
			}
			cols[i] = c
		}
		out["columns"] = cols
	}
	return out, nil
}

// seriesWire validates p and returns the payload of the series procedure.
// Filter operators outside SQLOperators are logged to log as a warning.
func seriesWire(p *SeriesQueryParams, log *slog.Logger) (map[string]any, error) {
	if p == nil {
		return nil, invalidf("series query parameters are required")
	}
	for _, f := range p.FilterAnd {
		if containsLatest(f) {
			return nil, invalidf("the 'latest' marker (and the legacy latest_flag filter) is not supported in series queries — use GetHistory with FilterAnd: []Filter{ironflock.Latest()} to read current values")
		}
	}
	valid := false
	for _, m := range downSampleMethods {
		if p.Method == m {
			valid = true
			break
		}
	}
	if !valid {
		return nil, invalidf("method must be one of AVG, SUM, COUNT, MIN, MAX, FIRST, LAST, got %q", p.Method)
	}
	if p.Limit < 1 || p.Limit > MaxQueryLimit {
		return nil, invalidf("limit must be between 1 and %d, got %d", MaxQueryLimit, p.Limit)
	}
	if p.TimeRange == nil {
		return nil, invalidf("timeRange is required for series queries")
	}
	tr, err := timeRangeWire(p.TimeRange)
	if err != nil {
		return nil, err
	}
	metrics := make([]any, len(p.Metrics))
	for i, m := range p.Metrics {
		metrics[i] = m
	}
	out := map[string]any{
		"metrics":   metrics,
		"method":    string(p.Method),
		"limit":     int64(p.Limit),
		"timeRange": tr,
	}
	if p.GroupBy != nil {
		groupBy := make([]any, len(p.GroupBy))
		for i, g := range p.GroupBy {
			groupBy[i] = g
		}
		out["groupBy"] = groupBy
	}
	if len(p.FilterAnd) > 0 {
		filters, err := filtersWire(p.FilterAnd, false, log)
		if err != nil {
			return nil, err
		}
		out["filterAnd"] = filters
	}
	return out, nil
}

// containsLatest reports whether f is, or contains, the latest marker or a
// legacy latest_flag predicate.
func containsLatest(f Filter) bool {
	if f.Latest || f.Column == "latest_flag" {
		return true
	}
	for _, sub := range f.Filters {
		if containsLatest(sub) {
			return true
		}
	}
	return false
}

func filtersWire(filters []Filter, allowGroups bool, log *slog.Logger) ([]any, error) {
	out := make([]any, len(filters))
	for i, f := range filters {
		w, err := filterWire(f, allowGroups, fmt.Sprintf("filterAnd[%d]", i), log)
		if err != nil {
			return nil, err
		}
		out[i] = w
	}
	return out, nil
}

func filterWire(f Filter, allowGroups bool, path string, log *slog.Logger) (map[string]any, error) {
	switch {
	case f.Latest:
		return map[string]any{"latest": true}, nil
	case f.Combinator != "" || f.Filters != nil:
		if !allowGroups {
			return nil, invalidf("%s: filter groups are not supported here; use plain predicates", path)
		}
		comb := strings.ToUpper(f.Combinator)
		if comb != "AND" && comb != "OR" {
			return nil, invalidf("%s: combinator must be \"AND\" or \"OR\", got %q", path, f.Combinator)
		}
		if len(f.Filters) == 0 {
			return nil, invalidf("%s: a filter group needs at least one filter", path)
		}
		subs := make([]any, len(f.Filters))
		for i, sub := range f.Filters {
			w, err := filterWire(sub, true, fmt.Sprintf("%s.filters[%d]", path, i), log)
			if err != nil {
				return nil, err
			}
			subs[i] = w
		}
		return map[string]any{"combinator": comb, "filters": subs}, nil
	}

	if strings.TrimSpace(f.Column) == "" {
		return nil, invalidf("%s: each filter must be the latest marker, a group, or a predicate with a non-empty column", path)
	}
	if strings.TrimSpace(f.Operator) == "" {
		return nil, invalidf("%s: predicate on column %q has no operator", path, f.Column)
	}
	op := strings.ToUpper(strings.TrimSpace(f.Operator))
	if !slices.Contains(sqlOperators, op) {
		if log == nil {
			log = slog.Default()
		}
		log.Warn(fmt.Sprintf("Operator '%s' is not in standard list: %v", f.Operator, sqlOperators))
	}
	out := map[string]any{"column": f.Column, "operator": f.Operator}

	if op == "IS NULL" || op == "IS NOT NULL" {
		if f.Value != nil {
			return nil, invalidf("%s: operator %q takes no value", path, f.Operator)
		}
		return out, nil
	}
	if f.Value == nil {
		return nil, invalidf("%s: predicate on column %q needs a value (use IsNull/IsNotNull to test for NULL)", path, f.Column)
	}
	isList := isSlice(f.Value)
	if op == "IN" || op == "NOT IN" {
		if !isList {
			if _, ok := f.Value.(string); !ok {
				return nil, invalidf("%s: operator %q requires a list of values", path, f.Operator)
			}
		}
	} else if isList {
		return nil, invalidf("%s: operator %q does not support list values", path, f.Operator)
	}
	v, err := normalize(f.Value)
	if err != nil {
		return nil, invalidf("%s: %v", path, err)
	}
	if list, ok := v.([]any); ok {
		for i, e := range list {
			switch e.(type) {
			case string, int64, uint64, float64:
			default:
				return nil, invalidf("%s: value[%d] must be a string or a number, got %T", path, i, e)
			}
		}
	} else {
		switch v.(type) {
		case string, int64, uint64, float64, bool:
		default:
			return nil, invalidf("%s: value must be a string, number or bool, got %T", path, f.Value)
		}
	}
	out["value"] = v
	return out, nil
}

func isSlice(v any) bool {
	if _, ok := v.([]byte); ok {
		return false
	}
	k := reflect.ValueOf(v).Kind()
	return k == reflect.Slice || k == reflect.Array
}

// The data backend turns each string bound of a TimeRange into a time with
// JavaScript's Date (new Date(s).getTime()), which holds times up to
// maxDateTime ms before or after the epoch (±100,000,000 days).
const (
	maxDateTime = 8_640_000_000_000_000
	// minDateYear and maxDateYear are the years of the earliest and the
	// latest time a Date holds.
	minDateYear = -271821
	maxDateYear = 275760
)

// isoTime reads s, a TimeRange string, as the data backend's JavaScript Date
// does, and returns its time value: ms since the epoch, fractions of a
// millisecond dropped. ok is false unless s is an ISO 8601 date or date-time
// in extended format that Date reads as the time it denotes:
//
//	date      = year ["-" month ["-" day]]
//	date-time = year "-" month "-" day ("T" | "t" | " ") time [zone]
//	time      = hour ":" minute [":" second ["." digit+]]
//	zone      = "Z" | "z" | ("+" | "-") hour [":"] minute
//
// A year is four digits, or six with a sign as Date.toISOString writes years
// outside 0000-9999 (but never "-000000"); the result must lie within Date's
// range. Days must exist in their month, and hours, minutes and seconds lie
// in 00-23, 00-59 and 00-59, except for 24:00, 24:00:00 and 24:00:00.0…, the
// end of the day. A time without a zone is read as UTC, the data backend's
// time zone. Date reads more than this, but not correctly: it moves a day a
// month does not have into the next month (2026-02-30 is 2026-03-02), and
// fails on hours without minutes, offsets of hours only, basic format,
// commas and week or ordinal dates. What it reads besides that (2026-1-1,
// an offset after a date) is not ISO 8601.
func isoTime(s string) (ms int64, ok bool) {
	p := isoScanner{s: s}
	year, ok := p.year()
	if !ok {
		return 0, false
	}
	month, day := int64(1), int64(1)
	var hour, minute, second, milli, offset int64
	if p.skip('-') {
		if month, ok = p.number(2, 1, 12); !ok {
			return 0, false
		}
		if p.skip('-') {
			if day, ok = p.number(2, 1, daysIn(year, month)); !ok {
				return 0, false
			}
			if p.skip('T') || p.skip('t') || p.skip(' ') {
				if hour, minute, second, milli, ok = p.clock(); !ok {
					return 0, false
				}
				if offset, ok = p.zone(); !ok {
					return 0, false
				}
			}
		}
	}
	if p.i != len(s) {
		return 0, false
	}
	ms = daysFromCivil(year, month, day)*86_400_000 +
		hour*3_600_000 + minute*60_000 + second*1000 + milli - offset*60_000
	if ms < -maxDateTime || ms > maxDateTime {
		return 0, false
	}
	return ms, true
}

// isoScanner reads the fields of an ISO 8601 string left to right.
type isoScanner struct {
	s string
	i int
}

// skip consumes c if it comes next.
func (p *isoScanner) skip(c byte) bool {
	if p.i < len(p.s) && p.s[p.i] == c {
		p.i++
		return true
	}
	return false
}

// digits consumes exactly n ASCII digits and returns their value.
func (p *isoScanner) digits(n int) (int64, bool) {
	if len(p.s)-p.i < n {
		return 0, false
	}
	var v int64
	for _, c := range []byte(p.s[p.i : p.i+n]) {
		if c < '0' || c > '9' {
			return 0, false
		}
		v = v*10 + int64(c-'0')
	}
	p.i += n
	return v, true
}

// number consumes an n-digit number within [lo, hi].
func (p *isoScanner) number(n int, lo, hi int64) (int64, bool) {
	v, ok := p.digits(n)
	return v, ok && v >= lo && v <= hi
}

// year consumes a four-digit year, or a six-digit one with a sign.
func (p *isoScanner) year() (int64, bool) {
	sign := int64(1)
	switch {
	case p.skip('+'):
	case p.skip('-'):
		sign = -1
	default:
		return p.digits(4)
	}
	y, ok := p.digits(6)
	if !ok || (sign < 0 && y == 0) {
		return 0, false
	}
	return sign * y, true
}

// clock consumes the time of day: hh:mm, hh:mm:ss or hh:mm:ss.fraction.
// Digits of the fraction beyond milliseconds are dropped, as Date drops them.
func (p *isoScanner) clock() (hour, minute, second, milli int64, ok bool) {
	if hour, ok = p.number(2, 0, 24); !ok || !p.skip(':') {
		return 0, 0, 0, 0, false
	}
	if minute, ok = p.number(2, 0, 59); !ok {
		return 0, 0, 0, 0, false
	}
	fraction := false // a fraction with a digit other than 0
	if p.skip(':') {
		if second, ok = p.number(2, 0, 59); !ok {
			return 0, 0, 0, 0, false
		}
		if p.skip('.') {
			n := 0 // digits of the fraction
			for ; p.i < len(p.s) && p.s[p.i] >= '0' && p.s[p.i] <= '9'; p.i++ {
				if n < 3 {
					milli = milli*10 + int64(p.s[p.i]-'0')
				}
				fraction = fraction || p.s[p.i] != '0'
				n++
			}
			if n == 0 {
				return 0, 0, 0, 0, false
			}
			for ; n < 3; n++ {
				milli *= 10 // ".5" is 500 ms
			}
		}
	}
	if hour == 24 && (minute != 0 || second != 0 || fraction) {
		return 0, 0, 0, 0, false // only 24:00 itself ends the day
	}
	return hour, minute, second, milli, true
}

// zone consumes an optional zone designator and returns its offset from UTC
// in minutes: Z, z, ±hh:mm or ±hhmm; none is UTC.
func (p *isoScanner) zone() (int64, bool) {
	sign := int64(1)
	switch {
	case p.skip('Z'), p.skip('z'), p.i == len(p.s):
		return 0, true
	case p.skip('+'):
	case p.skip('-'):
		sign = -1
	default:
		return 0, false
	}
	hours, ok := p.number(2, 0, 23)
	if !ok {
		return 0, false
	}
	p.skip(':')
	minutes, ok := p.number(2, 0, 59)
	if !ok {
		return 0, false
	}
	return sign * (hours*60 + minutes), true
}

// daysIn returns the number of days of month in year (proleptic Gregorian).
func daysIn(year, month int64) int64 {
	switch month {
	case 2:
		if year%4 == 0 && (year%100 != 0 || year%400 == 0) {
			return 29
		}
		return 28
	case 4, 6, 9, 11:
		return 30
	}
	return 31
}

// daysFromCivil returns the number of days from 1970-01-01 to the given date
// of the proleptic Gregorian calendar (Howard Hinnant's days_from_civil).
func daysFromCivil(year, month, day int64) int64 {
	if month <= 2 {
		year--
	}
	era := year / 400
	if year < 0 && year%400 != 0 {
		era-- // floor division
	}
	yoe := year - era*400                     // [0, 399]
	doy := (153*((month+9)%12)+2)/5 + day - 1 // [0, 365], from March 1st
	doe := yoe*365 + yoe/4 - yoe/100 + doy    // [0, 146096]
	return era*146_097 + doe - 719_468
}

// timeBound formats a time.Time bound as the data backend reads it: RFC 3339
// in UTC, and outside the years 0000-9999 with the six-digit signed year of
// JavaScript's expanded format (as Date.toISOString writes it). A time
// outside the range of JavaScript's Date is an error.
func timeBound(t time.Time) (string, error) {
	t = t.UTC()
	year := t.Year()
	if year >= 0 && year <= 9999 {
		return t.Format(time.RFC3339Nano), nil
	}
	// Checking the year first keeps UnixMilli within int64.
	if year < minDateYear || year > maxDateYear || t.UnixMilli() < -maxDateTime || t.UnixMilli() > maxDateTime {
		return "", invalidf("timeRange bound %s is outside the range of times the data backend handles "+
			"(-271821-04-20T00:00:00Z to +275760-09-13T00:00:00Z)", t.Format(time.RFC3339Nano))
	}
	sign := "+"
	if year < 0 {
		sign, year = "-", -year
	}
	return fmt.Sprintf("%s%06d%s", sign, year, t.Format("-01-02T15:04:05.999999999Z07:00")), nil
}

// timeRangeWire validates tr and returns the [start, end] pair.
func timeRangeWire(tr *TimeRange) ([]any, error) {
	bounds := [2]any{tr.Start, tr.End}
	out := make([]any, 2)
	hasStr, hasNum := false, false
	for i, b := range bounds {
		switch v := b.(type) {
		case nil:
			out[i] = nil
		case time.Time:
			s, err := timeBound(v)
			if err != nil {
				return nil, err
			}
			out[i] = s
			hasStr = true
		case *time.Time:
			if v == nil {
				out[i] = nil
				continue
			}
			s, err := timeBound(*v)
			if err != nil {
				return nil, err
			}
			out[i] = s
			hasStr = true
		case string:
			if _, ok := isoTime(v); !ok {
				return nil, invalidf("Invalid ISO datetime format: %s", v)
			}
			out[i] = v // as given, byte for byte
			hasStr = true
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
			n, _ := normalize(v)
			out[i] = n
			hasNum = true
		default:
			return nil, invalidf("timeRange bounds must be ISO date-time strings, time.Time, epoch-ms numbers or nil, got %T", b)
		}
	}
	if hasStr && hasNum {
		return nil, invalidf("timeRange entries must be all ISO date-time strings or all epoch-ms numbers")
	}
	return out, nil
}
