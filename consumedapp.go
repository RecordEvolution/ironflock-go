package ironflock

import (
	"context"
	"time"

	"github.com/RecordEvolution/ironflock-go/crossbar"
)

// Cross-app procedures on the app's own realm.
const (
	uriAppAccessResolve = "sys.appaccess.resolve"
	uriAppAccessList    = "sys.appaccess.list"
)

// TableInfo is a table or transform a provider app shares.
type TableInfo struct {
	Tablename   string `json:"tablename"`
	Description string `json:"description,omitempty"`
	// Columns are the column definitions as declared in the provider's
	// data template (e.g. {"id": "temperature", "dataType": "numeric"}).
	Columns []map[string]any `json:"columns,omitempty"`
}

// StageCatalog is what a provider shares on one stage.
type StageCatalog struct {
	Tables     []TableInfo `json:"tables"`
	Transforms []TableInfo `json:"transforms"`
}

// ConsumedAppInfo describes a provider app (the result of
// sys.appaccess.resolve and the elements of ListConsumableApps). A stage is
// present only when the provider has a data backend for it.
type ConsumedAppInfo struct {
	App            string `json:"app"`
	ProviderAppKey int    `json:"provider_app_key"`
	Stages         struct {
		Dev  *StageCatalog `json:"dev,omitempty"`
		Prod *StageCatalog `json:"prod,omitempty"`
	} `json:"stages"`
}

// Catalog returns the catalog of stage ("dev" or "prod"), or nil.
func (i *ConsumedAppInfo) Catalog(stage string) *StageCatalog {
	switch stage {
	case "dev":
		return i.Stages.Dev
	case "prod":
		return i.Stages.Prod
	}
	return nil
}

// ConnectToAppOptions configures ConnectToApp.
type ConnectToAppOptions struct {
	// Stage is the provider stage, "dev" or "prod" (default: this app's own
	// stage).
	Stage string
	// OnError is called (on its own goroutine) when the connection is
	// fatally denied AFTER ConnectToApp returned — e.g. the grant was
	// revoked and the next reconnect was refused. Before that, the same
	// condition is returned by ConnectToApp.
	OnError func(err *CrossAppAccessError)
}

// ConnectToAllAppsOptions configures ConnectToAllApps.
type ConnectToAllAppsOptions struct {
	// Stage is the provider stage, "dev" or "prod" (default: this app's own
	// stage).
	Stage string
	// OnError is called with the failure of each provider that could not be
	// opened (unless StopOnError is set), and with a *CrossAppAccessError
	// when an opened provider connection is later fatally denied.
	OnError func(err error)
	// StopOnError makes the first provider that fails to open fail the
	// whole call. By default such a provider is reported to OnError and
	// left out of the result.
	StopOnError bool
}

// consumedEntry is one cached consumed-app connection (or attempt). done is
// closed when the attempt finishes; app and err are valid afterwards.
type consumedEntry struct {
	done chan struct{}
	app  *ConsumedApp
	err  error
}

// ConsumedApp is a read-only handle on another app's data backend, in the
// same project and fleet. It wraps a dedicated connection to the provider's
// realm, where the router allows only reading shared tables and transforms.
type ConsumedApp struct {
	// App is the provider app's name.
	App string
	// Stage is the provider stage this handle is connected to ("dev" or
	// "prod").
	Stage string
	// Tables and Transforms are what the provider shares on Stage.
	Tables     []TableInfo
	Transforms []TableInfo

	conn            wampConn
	reconnectWindow time.Duration
	onClosed        func()
}

// Connection returns the underlying connection to the provider's realm.
func (a *ConsumedApp) Connection() *crossbar.Connection {
	c, _ := a.conn.(*crossbar.Connection)
	return c
}

// IsConnected reports whether the connection to the provider is open.
func (a *ConsumedApp) IsConnected() bool { return a.conn.IsOpen() }

// GetHistory reads rows of a shared table or transform (the provider's
// history.transformed.<table>). A nil q reads the 10 most recent rows.
//
// Errors: *CrossAppAccessError with PRIVATE_TABLE for a name outside the
// shared catalog, SECRET_COLUMN when q filters on or selects a column the
// provider marks secret, NOT_AUTHORIZED when the provider denies access.
func (a *ConsumedApp) GetHistory(ctx context.Context, table string, q *TableQueryParams) ([]Row, error) {
	panic("TODO: implement")
}

// GetSeriesHistory reads down-sampled series of a shared table (tables
// only: there is no series procedure for transforms).
func (a *ConsumedApp) GetSeriesHistory(ctx context.Context, table string, q SeriesQueryParams) ([]Row, error) {
	panic("TODO: implement")
}

// SubscribeToTable subscribes handler to realtime rows of a shared table or
// transform, exactly like IronFlock.SubscribeToTable.
func (a *ConsumedApp) SubscribeToTable(ctx context.Context, table string, handler EventHandler, opts ...SubscribeOptions) (*TableSubscription, error) {
	panic("TODO: implement")
}

// Close closes the connection to the provider. IronFlock.Stop closes all
// consumed apps as well.
func (a *ConsumedApp) Close(ctx context.Context) error {
	panic("TODO: implement")
}

// ConnectToApp opens a read-only connection to another app's data backend in
// the same project and returns a handle on it. The provider must list this
// app in its data-template consumes: section and the project user must have
// granted access. Handles are cached per app and stage: a second call
// returns the same handle, and concurrent calls share one attempt.
//
// Errors: *CrossAppAccessError with NO_GRANT, PROVIDER_NOT_INSTALLED,
// UNKNOWN_APP or NOT_AUTHORIZED.
func (f *IronFlock) ConnectToApp(ctx context.Context, appName string, opts ...ConnectToAppOptions) (*ConsumedApp, error) {
	panic("TODO: implement")
}

// ListConsumableApps lists every non-private provider of this project, with
// its per-stage catalog, without opening connections (for wildcard
// consumers: consumes: [{app: "*"}]).
func (f *IronFlock) ListConsumableApps(ctx context.Context) ([]ConsumedAppInfo, error) {
	panic("TODO: implement")
}

// ConnectToAllApps opens read-only connections to every non-private
// provider that has a data backend for the stage (wildcard consumers only).
// Handles share ConnectToApp's cache.
func (f *IronFlock) ConnectToAllApps(ctx context.Context, opts ...ConnectToAllAppsOptions) ([]*ConsumedApp, error) {
	panic("TODO: implement")
}
