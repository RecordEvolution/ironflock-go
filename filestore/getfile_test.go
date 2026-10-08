package filestore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"testing"
)

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestGetToFileInline(t *testing.T) {
	ctx := context.Background()
	fs, fc := newStore(t, map[string]any{URIGet: ok(map[string]any{
		"namespace": "frames", "key": "a.txt", "size": int64(3), "data": "aGV5", "encoding": "base64",
	})})
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "deeper", "a.txt")
	info, err := fs.GetToFile(ctx, "a.txt", path, Namespace("frames"))
	if err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, path); string(got) != "hey" {
		t.Errorf("file = %q, want hey", got)
	}
	if names := dirEntries(t, filepath.Dir(path)); !reflect.DeepEqual(names, []string{"a.txt"}) {
		t.Errorf("directory holds %v, want only the file", names)
	}
	if info.Size != 3 || info.URL != "https://files.ironflock.com/f/3317/frames/a.txt" {
		t.Errorf("info = %+v", info)
	}
	if fc.count(URIStat) != 0 {
		t.Error("an inline get read the descriptor back")
	}
}

func TestGetToFileDirect(t *testing.T) {
	ctx := context.Background()
	data := randomBytes(t, 3<<20)
	var store *objectStore
	fs, fc, store := directStore(t, 1024, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(data) }, map[string]any{
		URIStat: replyFunc(func([]any) (any, error) {
			if len(store.requests()) != 1 {
				return nil, errors.New("stat before the download")
			}
			return ok(map[string]any{"namespace": "frames", "key": "big.bin", "size": int64(len(data))}), nil
		}),
	})
	dir := t.TempDir()
	path := filepath.Join(dir, "out", "big.bin")
	info, err := fs.GetToFile(ctx, "big.bin", path, Namespace("frames"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readFile(t, path), data) {
		t.Error("file content differs")
	}
	if names := dirEntries(t, filepath.Dir(path)); !reflect.DeepEqual(names, []string{"big.bin"}) {
		t.Errorf("directory holds %v, want only the file", names)
	}
	if info.Size != int64(len(data)) || fc.count(URIStat) != 1 {
		t.Errorf("info = %+v, stat calls = %d", info, fc.count(URIStat))
	}
}

// A failed or cancelled download never leaves a partial file, and keeps a
// previous file at the path intact.
func TestGetToFileLeavesNoPartialFile(t *testing.T) {
	const old = "previous content"
	cases := []struct {
		name    string
		respond func(started chan<- struct{}) func(w http.ResponseWriter, r *http.Request)
		cancel  bool
		check   func(t *testing.T, err error)
	}{
		{"object store refusal", func(chan<- struct{}) func(http.ResponseWriter, *http.Request) {
			return respondWith(404, "<Error><Code>NoSuchKey</Code></Error>")
		}, false, func(t *testing.T, err error) {
			wantError(t, err, CodeNoSuchObject, "the object store has no such object")
		}},
		{"connection lost mid-stream", func(chan<- struct{}) func(http.ResponseWriter, *http.Request) {
			return func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Length", strconv.Itoa(1<<20))
				_, _ = w.Write(make([]byte, 64<<10))
				w.(http.Flusher).Flush()
				panic(http.ErrAbortHandler)
			}
		}, false, func(t *testing.T, err error) {
			if !errors.Is(err, io.ErrUnexpectedEOF) || codeOf(err) != "" {
				t.Errorf("err = %v, want an interrupted-download error", err)
			}
		}},
		{"cancelled mid-stream", func(started chan<- struct{}) func(http.ResponseWriter, *http.Request) {
			return func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Length", strconv.Itoa(1<<20))
				_, _ = w.Write(make([]byte, 64<<10))
				w.(http.Flusher).Flush()
				started <- struct{}{}
				<-r.Context().Done()
			}
		}, true, func(t *testing.T, err error) {
			if !errors.Is(err, context.Canceled) {
				t.Errorf("err = %v, want context.Canceled", err)
			}
		}},
	}
	for _, tc := range cases {
		for _, existing := range []bool{false, true} {
			name := tc.name
			if existing {
				name += " over an existing file"
			}
			t.Run(name, func(t *testing.T) {
				started := make(chan struct{}, 1)
				fs, fc, _ := directStore(t, 1024, tc.respond(started), nil)
				dir := t.TempDir()
				path := filepath.Join(dir, "big.bin")
				if existing {
					if err := os.WriteFile(path, []byte(old), 0o644); err != nil {
						t.Fatal(err)
					}
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if tc.cancel {
					go func() {
						<-started
						cancel()
					}()
				}
				_, err := fs.GetToFile(ctx, "big.bin", path)
				tc.check(t, err)
				if existing {
					if got := readFile(t, path); string(got) != old {
						t.Errorf("existing file changed to %d bytes", len(got))
					}
					if names := dirEntries(t, dir); !reflect.DeepEqual(names, []string{"big.bin"}) {
						t.Errorf("directory holds %v", names)
					}
				} else if names := dirEntries(t, dir); len(names) != 0 {
					t.Errorf("directory holds %v, want nothing", names)
				}
				if fc.count(URIStat) != 0 {
					t.Error("stat after a failed download")
				}
			})
		}
	}
}

func TestGetToFileFailures(t *testing.T) {
	ctx := context.Background()
	t.Run("service refusal creates nothing", func(t *testing.T) {
		fs, _ := newStore(t, map[string]any{URIGet: fail(CodeNoSuchObject, "gone")})
		dir := t.TempDir()
		_, err := fs.GetToFile(ctx, "a", filepath.Join(dir, "sub", "a"))
		wantError(t, err, CodeNoSuchObject, "gone")
		if names := dirEntries(t, dir); len(names) != 0 {
			t.Errorf("directory holds %v", names)
		}
	})
	t.Run("invalid inline payload", func(t *testing.T) {
		fs, _ := newStore(t, map[string]any{URIGet: ok(map[string]any{"key": "a", "data": "!!", "encoding": "base64"})})
		dir := t.TempDir()
		_, err := fs.GetToFile(ctx, "a", filepath.Join(dir, "a"))
		wantError(t, err, CodeInternal, "object payload was not valid base64")
		if names := dirEntries(t, dir); len(names) != 0 {
			t.Errorf("directory holds %v", names)
		}
	})
	t.Run("target is a directory", func(t *testing.T) {
		fs, _ := newStore(t, map[string]any{URIGet: ok(map[string]any{"key": "a", "data": "aGV5"})})
		dir := t.TempDir()
		target := filepath.Join(dir, "taken")
		if err := os.Mkdir(target, 0o755); err != nil {
			t.Fatal(err)
		}
		if _, err := fs.GetToFile(ctx, "a", target); err == nil {
			t.Fatal("wrote over a directory")
		}
		if names := dirEntries(t, dir); !reflect.DeepEqual(names, []string{"taken"}) {
			t.Errorf("directory holds %v, want no temporary file left", names)
		}
	})
}

func TestGetToFileKeepsPermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permissions")
	}
	fs, _ := newStore(t, map[string]any{URIGet: ok(map[string]any{"key": "a", "data": "aGV5"})})
	path := filepath.Join(t.TempDir(), "a")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.GetToFile(context.Background(), "a", path); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 || string(readFile(t, path)) != "hey" {
		t.Errorf("mode %v, content %q", fi.Mode().Perm(), readFile(t, path))
	}
}
