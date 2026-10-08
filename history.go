package ironflock

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
)

// validateReadTable checks the name of a table or transform to read: non-empty
// and without leading or trailing whitespace.
func validateReadTable(table string) error {
	if strings.TrimSpace(table) == "" {
		return invalidf("Tablename must not be empty!")
	}
	if strings.TrimSpace(table) != table {
		return invalidf("Tablename %q cannot have leading/trailing whitespace", table)
	}
	return nil
}

// decodeRows converts the result of a history-style procedure into rows: no
// value reads as no rows, a list of row objects as rows. Anything else is an
// error of operation op.
func decodeRows(op string, res *Result) ([]Row, error) {
	switch v := res.Value().(type) {
	case nil:
		return []Row{}, nil
	case []any:
		rows := make([]Row, len(v))
		for i, e := range v {
			row, ok := e.(map[string]any)
			if !ok {
				return nil, &OperationError{Op: op, Err: fmt.Errorf("%w: row %d is %T, not an object", errUnexpectedResult, i, e)}
			}
			rows[i] = row
		}
		return rows, nil
	default:
		return nil, &OperationError{Op: op, Err: fmt.Errorf("%w %T, want a list of rows", errUnexpectedResult, v)}
	}
}

// GetHistory reads rows of a table or transform via
// history.transformed.<table>. A nil q reads the 10 most recent rows. Secret
// columns come back as SecretPlaceholder.
func (f *IronFlock) GetHistory(ctx context.Context, table string, q *TableQueryParams) ([]Row, error) {
	if err := validateReadTable(table); err != nil {
		return nil, err
	}
	if q == nil {
		q = &TableQueryParams{Limit: 10}
	}
	wire, err := queryWire(q, MaxQueryLimit, f.log)
	if err != nil {
		return nil, invalidParams("query", err)
	}
	op := fmt.Sprintf("getHistory('%s')", table)
	topic := "history.transformed." + table
	res, err := f.call(ctx, topic, []any{wire}, nil, nil, f.reconnectWindow)
	if err != nil {
		return nil, historyFailed(op, topic, err, false)
	}
	return decodeRows(op, res)
}

// GetSeriesHistory reads down-sampled time series of a table via
// history.transformed.series.<table>.
func (f *IronFlock) GetSeriesHistory(ctx context.Context, table string, q SeriesQueryParams) ([]Row, error) {
	if err := validateReadTable(table); err != nil {
		return nil, err
	}
	wire, err := seriesWire(&q, f.log)
	if err != nil {
		return nil, invalidParams("series query", err)
	}
	op := fmt.Sprintf("getSeriesHistory('%s')", table)
	topic := "history.transformed.series." + table
	res, err := f.call(ctx, topic, []any{wire}, nil, nil, f.reconnectWindow)
	if err != nil {
		return nil, historyFailed(op, topic, err, false)
	}
	return decodeRows(op, res)
}

// RevealSecrets reads rows of an own table with its secret columns
// decrypted, via secret.reveal.<table>. A nil q reads the 10 most recent
// rows; Limit must be 1-100. Only the app's own containers may call it.
func (f *IronFlock) RevealSecrets(ctx context.Context, table string, q *TableQueryParams) ([]Row, error) {
	if err := validateReadTable(table); err != nil {
		return nil, err
	}
	if q == nil {
		q = &TableQueryParams{Limit: 10}
	}
	wire, err := queryWire(q, MaxSecretLimit, f.log)
	if err != nil {
		return nil, invalidParams("query", err)
	}
	op := fmt.Sprintf("revealSecrets('%s')", table)
	topic := "secret.reveal." + table
	res, err := f.call(ctx, topic, []any{wire}, nil, nil, f.reconnectWindow)
	if err != nil {
		return nil, historyFailed(op, topic, err, true)
	}
	return decodeRows(op, res)
}

// VerifySecret checks candidate against the secret column of the selected
// rows without reading it back (secret.verify.<table>); the comparison runs
// in constant time inside the data backend. A nil q checks the most recent
// row; Limit must be 1-100. A response of an unexpected shape never reads
// as a match.
func (f *IronFlock) VerifySecret(ctx context.Context, table, column, candidate string, q *TableQueryParams) (*SecretVerifyResult, error) {
	if err := validateReadTable(table); err != nil {
		return nil, err
	}
	if strings.TrimSpace(column) == "" {
		return nil, invalidf("Invalid verify parameters: column must be a non-empty string")
	}
	if q == nil {
		q = &TableQueryParams{Limit: 1}
	}
	wire, err := queryWire(q, MaxSecretLimit, f.log)
	if err != nil {
		return nil, invalidParams("verify", err)
	}
	// column and candidate travel in the same object as the row selection.
	wire["column"] = column
	wire["candidate"] = candidate

	op := fmt.Sprintf("verifySecret('%s')", table)
	topic := "secret.verify." + table
	res, err := f.call(ctx, topic, []any{wire}, nil, nil, f.reconnectWindow)
	if err != nil {
		return nil, historyFailed(op, topic, err, true)
	}
	return verifyResult(res), nil
}

// verifyResult reads the {match, checked} answer of secret.verify. It fails
// closed: only an object whose "match" is the boolean true is a match.
func verifyResult(res *Result) *SecretVerifyResult {
	out := &SecretVerifyResult{}
	m, ok := res.Value().(map[string]any)
	if !ok {
		return out
	}
	out.Match = m["match"] == true
	switch n := m["checked"].(type) {
	case int64:
		out.Checked = int(n)
	case uint64:
		out.Checked = int(min(n, uint64(math.MaxInt)))
	case float64:
		if n >= 0 && n <= math.MaxInt {
			out.Checked = int(n)
		}
	}
	return out
}

// errUnexpectedResult is the cause of decoding failures of service results.
var errUnexpectedResult = errors.New("unexpected result type")
