package filestore

import (
	"context"
	"errors"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCatalogDecoding(t *testing.T) {
	ctx := context.Background()
	fs, _ := newStore(t, map[string]any{URINamespaces: ok(catalogPayload(map[string]any{
		"quota_bytes":           int64(53687091200),
		"suggested_quota_bytes": int64(10737418240),
		"cloud_base_url":        "https://i5-files.app.ironflock.com",
		"presign_available":     true,
		"presign_max_bytes":     int64(5 << 30),
		"unknown_field":         "ignored",
	}))})
	c, err := fs.Catalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := &Catalog{
		Namespaces: []NamespaceInfo{{
			Name:           "frames",
			Description:    "Camera frames",
			Private:        false,
			ContentTypes:   []string{"image/jpeg"},
			MaxObjectBytes: 1048576,
		}},
		InlineMaxBytes:      6291456,
		ListMaxLimit:        1000,
		QuotaBytes:          53687091200,
		SuggestedQuotaBytes: 10737418240,
		SadKey:              3317,
		PublicBaseURL:       "https://files.ironflock.com",
		CloudBaseURL:        "https://i5-files.app.ironflock.com",
		PresignAvailable:    true,
		PresignMaxBytes:     5 << 30,
	}
	if !reflect.DeepEqual(c, want) {
		t.Errorf("catalog = %+v\nwant      %+v", c, want)
	}
}

func TestCatalogDefaults(t *testing.T) {
	ctx := context.Background()
	fs, _ := newStore(t, map[string]any{URINamespaces: ok(map[string]any{})})
	c, err := fs.Catalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := &Catalog{Namespaces: []NamespaceInfo{}, ListMaxLimit: 1000}
	if !reflect.DeepEqual(c, want) {
		t.Errorf("catalog = %+v, want %+v", c, want)
	}

	for _, tc := range []struct {
		wire any
		want int
	}{{int64(0), 1000}, {nil, 1000}, {int64(250), 250}, {250.0, 250}, {"500", 500}} {
		fs, _ := newStore(t, map[string]any{URINamespaces: ok(map[string]any{"list_max_limit": tc.wire})})
		c, err := fs.Catalog(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if c.ListMaxLimit != tc.want {
			t.Errorf("list_max_limit %v: ListMaxLimit = %d, want %d", tc.wire, c.ListMaxLimit, tc.want)
		}
	}
}

// Decoding is lenient, like the JavaScript SDK, but reads "false" as false.
func TestCatalogLenientDecoding(t *testing.T) {
	ctx := context.Background()
	fs, _ := newStore(t, map[string]any{URINamespaces: ok(map[string]any{
		"namespaces":        []any{map[string]any{"name": "a", "private": "true", "max_object_bytes": "1024"}, "junk", nil},
		"inline_max_bytes":  float64(6291456),
		"sad_key":           "3317",
		"presign_available": "false",
		"presign_max_bytes": uint64(5 << 30),
		"quota_bytes":       math.NaN(),
		"public_base_url":   nil,
	})})
	c, err := fs.Catalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if c.InlineMaxBytes != 6291456 || c.SadKey != 3317 || c.PresignAvailable || c.PresignMaxBytes != 5<<30 || c.QuotaBytes != 0 || c.PublicBaseURL != "" {
		t.Errorf("catalog = %+v", c)
	}
	if want := []NamespaceInfo{{Name: "a", Private: true, MaxObjectBytes: 1024}}; !reflect.DeepEqual(c.Namespaces, want) {
		t.Errorf("namespaces = %+v, want %+v", c.Namespaces, want)
	}
}

// Namespaces are shared unless the template says otherwise.
func TestNamespaceWithoutPrivateFlagIsShared(t *testing.T) {
	fs, _ := newStore(t, map[string]any{URINamespaces: ok(map[string]any{
		"namespaces": []any{map[string]any{"name": "b"}}, "sad_key": int64(1), "public_base_url": "",
	})})
	ns, err := fs.Namespaces(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(ns) != 1 || ns[0].Name != "b" || ns[0].Private || ns[0].ContentTypes != nil {
		t.Errorf("namespaces = %+v", ns)
	}
}

// The object store enforces one budget per bucket; a namespace has no quota
// of its own, even when the payload sends one.
func TestNamespacesCarryNoQuota(t *testing.T) {
	if _, has := reflect.TypeOf(NamespaceInfo{}).FieldByName("QuotaBytes"); has {
		t.Error("NamespaceInfo must not have a QuotaBytes field")
	}
}

// The catalog is read on the hot path of every write: ten parallel puts on
// a cold cache must fetch it once.
func TestCatalogFetchedOnceUnderConcurrency(t *testing.T) {
	ctx := context.Background()
	release := make(chan struct{})
	var fetches atomic.Int32
	fs, fc := newStore(t, map[string]any{
		URINamespaces: replyFunc(func([]any) (any, error) {
			fetches.Add(1)
			<-release
			return ok(catalogPayload(nil)), nil
		}),
		URIPut: ok(map[string]any{"namespace": "frames", "key": "k", "size": int64(1), "etag": "e", "content_type": "x"}),
	})
	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := fs.Put(ctx, "k", []byte("x"), Namespace("frames"))
			errs <- err
		}()
	}
	waitFor(t, func() bool { return fetches.Load() == 1 })
	time.Sleep(10 * time.Millisecond) // let the other puts queue up behind the fetch
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if n := fc.count(URINamespaces); n != 1 {
		t.Errorf("catalog fetched %d times, want 1", n)
	}
	if n := fc.count(URIPut); n != 10 {
		t.Errorf("put called %d times, want 10", n)
	}
}

func TestCatalogFailureIsNotCached(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("not connected")
	var n atomic.Int32
	fs, fc := newStore(t, map[string]any{URINamespaces: replyFunc(func([]any) (any, error) {
		if n.Add(1) == 1 {
			return nil, boom
		}
		return ok(catalogPayload(nil)), nil
	})})
	if _, err := fs.Catalog(ctx); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
	for range 2 {
		if _, err := fs.Catalog(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if got := fc.count(URINamespaces); got != 2 {
		t.Errorf("catalog fetched %d times, want 2 (one failure, then cached)", got)
	}
}

// A caller waiting behind another caller's fetch gives up when its own
// context ends.
func TestCatalogWaiterHonoursContext(t *testing.T) {
	release := make(chan struct{})
	var fetches atomic.Int32
	fs, _ := newStore(t, map[string]any{URINamespaces: replyFunc(func([]any) (any, error) {
		fetches.Add(1)
		<-release
		return ok(catalogPayload(nil)), nil
	})})
	first := make(chan error, 1)
	go func() {
		_, err := fs.Catalog(context.Background())
		first <- err
	}()
	waitFor(t, func() bool { return fetches.Load() == 1 })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := fs.Catalog(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiter err = %v, want deadline exceeded", err)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
}

func TestRefreshCatalog(t *testing.T) {
	ctx := context.Background()
	fs, fc := newStore(t, nil)
	if u, _ := fs.URL(ctx, "a.jpg"); u != "https://files.ironflock.com/f/3317/default/a.jpg" {
		t.Fatalf("URL = %q", u)
	}
	fc.set(URINamespaces, ok(catalogPayload(map[string]any{"public_base_url": "https://new.example", "sad_key": int64(9)})))
	if c, _ := fs.Catalog(ctx); c.PublicBaseURL != "https://files.ironflock.com" {
		t.Errorf("Catalog re-fetched without RefreshCatalog: %q", c.PublicBaseURL)
	}
	c, err := fs.RefreshCatalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if c.PublicBaseURL != "https://new.example" || c.SadKey != 9 {
		t.Errorf("refreshed catalog = %+v", c)
	}
	if u, _ := fs.URL(ctx, "a.jpg"); u != "https://new.example/f/9/default/a.jpg" {
		t.Errorf("URL after refresh = %q", u)
	}
	if n := fc.count(URINamespaces); n != 2 {
		t.Errorf("catalog fetched %d times, want 2", n)
	}

	// A failed refresh keeps the cached catalog.
	fc.set(URINamespaces, errors.New("down"))
	if _, err := fs.RefreshCatalog(ctx); err == nil {
		t.Fatal("RefreshCatalog succeeded against a failing service")
	}
	if c, err := fs.Catalog(ctx); err != nil || c.SadKey != 9 {
		t.Errorf("Catalog after a failed refresh = %+v, %v", c, err)
	}
}

func TestCatalogResultIsACopy(t *testing.T) {
	ctx := context.Background()
	fs, _ := newStore(t, nil)
	c, err := fs.Catalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c.Namespaces[0].Name = "changed"
	c.Namespaces[0].ContentTypes[0] = "changed"
	c.PublicBaseURL = "changed"
	ns, _ := fs.Namespaces(ctx)
	ns[0].Description = "changed"

	again, _ := fs.Catalog(ctx)
	if again.Namespaces[0].Name != "frames" || again.Namespaces[0].ContentTypes[0] != "image/jpeg" ||
		again.Namespaces[0].Description != "Camera frames" || again.PublicBaseURL != "https://files.ironflock.com" {
		t.Errorf("the cached catalog was modified through a returned copy: %+v", again)
	}
}

func TestUsage(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name    string
		payload map[string]any
		want    StorageUsage
	}{
		{"fields", map[string]any{"size_bytes": int64(400), "object_count": int64(12), "quota_bytes": int64(1000), "free_bytes": int64(600)},
			StorageUsage{SizeBytes: 400, ObjectCount: 12, QuotaBytes: 1000, FreeBytes: 600}},
		{"unlimited", map[string]any{"size_bytes": int64(400), "quota_bytes": int64(0), "free_bytes": int64(-1)},
			StorageUsage{SizeBytes: 400, FreeBytes: -1}},
		{"free_bytes absent", map[string]any{"size_bytes": int64(400)},
			StorageUsage{SizeBytes: 400, FreeBytes: -1}},
		{"free_bytes null", map[string]any{"free_bytes": nil},
			StorageUsage{FreeBytes: -1}},
		{"free_bytes 0 is full", map[string]any{"quota_bytes": int64(10), "free_bytes": int64(0)},
			StorageUsage{QuotaBytes: 10, FreeBytes: 0}},
		{"per namespace", map[string]any{"size_bytes": int64(400), "per_namespace": map[string]any{"frames": int64(300), "reports": 100.0}},
			StorageUsage{SizeBytes: 400, FreeBytes: -1, PerNamespace: map[string]int64{"frames": 300, "reports": 100}}},
		{"empty per namespace is kept", map[string]any{"per_namespace": map[string]any{}},
			StorageUsage{FreeBytes: -1, PerNamespace: map[string]int64{}}},
		{"null per namespace", map[string]any{"per_namespace": nil},
			StorageUsage{FreeBytes: -1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs, _ := newStore(t, map[string]any{URIUsage: ok(tc.payload)})
			u, err := fs.Usage(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(*u, tc.want) {
				t.Errorf("usage = %+v, want %+v", *u, tc.want)
			}
		})
	}
}

// The per-namespace breakdown costs one listing per namespace, so it is
// opt-in: a plain Usage sends no arguments at all.
func TestUsageDetailIsOptIn(t *testing.T) {
	ctx := context.Background()
	fs, fc := newStore(t, map[string]any{URIUsage: ok(map[string]any{
		"size_bytes": int64(400), "per_namespace": map[string]any{"frames": int64(300), "reports": int64(100)},
	})})
	if _, err := fs.Usage(ctx); err != nil {
		t.Fatal(err)
	}
	u, err := fs.Usage(ctx, Detail())
	if err != nil {
		t.Fatal(err)
	}
	calls := fc.callsTo(URIUsage)
	if len(calls[0].Args) != 0 {
		t.Errorf("plain usage args = %v, want none", calls[0].Args)
	}
	if want := []any{map[string]any{"detail": true}}; !reflect.DeepEqual(calls[1].Args, want) {
		t.Errorf("detailed usage args = %v, want %v", calls[1].Args, want)
	}
	if want := map[string]int64{"frames": 300, "reports": 100}; !reflect.DeepEqual(u.PerNamespace, want) {
		t.Errorf("per namespace = %v, want %v", u.PerNamespace, want)
	}
	if fc.count(URINamespaces) != 0 {
		t.Error("Usage fetched the catalog")
	}
}

// waitFor polls cond until it holds or the test times out.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 5s")
		}
		time.Sleep(time.Millisecond)
	}
}
