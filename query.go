package ironflock

import (
	"fmt"
	"log/slog"
	"math"
	"reflect"
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

// SQLOperators is the data backend's filter operator vocabulary. An operator
// outside it is logged as a warning, not rejected: the server is the
// authority and may accept operators this release predates.
var SQLOperators = []string{
	"=", "!=", "<>", ">", "<", ">=", "<=",
	"LIKE", "ILIKE", "NOT LIKE", "NOT ILIKE",
	"IN", "NOT IN", "IS NULL", "IS NOT NULL",
}

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

// TimeRange is the [start, end] range of a query. Each bound is an ISO-8601
// date-time string, a time.Time (sent as an RFC 3339 UTC string), an integer
// epoch-milliseconds number, or nil for an open end. Strings and numbers
// cannot be mixed in one range.
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
	known := false
	for _, k := range SQLOperators {
		if op == k {
			known = true
			break
		}
	}
	if !known {
		if log == nil {
			log = slog.Default()
		}
		log.Warn(fmt.Sprintf("Operator '%s' is not in standard list: %v", f.Operator, SQLOperators))
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

// isoLayouts are the date-time forms accepted in a TimeRange, mirroring
// Python's datetime.fromisoformat.
var isoLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04Z07:00",
	"2006-01-02T15:04",
	"2006-01-02",
}

func parseISO(s string) bool {
	for _, layout := range isoLayouts {
		if _, err := time.Parse(layout, s); err == nil {
			return true
		}
	}
	return false
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
			out[i] = v.UTC().Format(time.RFC3339Nano)
			hasStr = true
		case *time.Time:
			if v == nil {
				out[i] = nil
				continue
			}
			out[i] = v.UTC().Format(time.RFC3339Nano)
			hasStr = true
		case string:
			if !parseISO(v) {
				return nil, invalidf("Invalid ISO datetime format: %s", v)
			}
			out[i] = v
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
