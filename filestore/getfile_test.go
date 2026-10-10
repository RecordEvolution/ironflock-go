package filestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"unicode/utf8"
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

// transferPaths are the two ways GetToFile fetches the object "hello":
// inline over the router, or directly from the object store.
var transferPaths = []struct {
	name  string
	store func(t *testing.T) *FileStore
}{
	{"inline", func(t *testing.T) *FileStore {
		fs, _ := newStore(t, map[string]any{URIGet: ok(map[string]any{
			"key": "k", "size": int64(5), "data": "aGVsbG8=", "encoding": "base64",
		})})
		return fs
	}},
	{"direct", func(t *testing.T) *FileStore {
		fs, _, _ := directStore(t, 1, respondWith(http.StatusOK, "hello"), nil)
		return fs
	}},
}

// symlink creates a symbolic link, skipping the test where the OS does not
// permit one (Windows without the privilege).
func symlink(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("symbolic links not permitted: %v", err)
		}
		t.Fatal(err)
	}
}

// mkdirs creates directories under root.
func mkdirs(t *testing.T, root string, dirs ...string) {
	t.Helper()
	for _, d := range dirs {
		if err := os.MkdirAll(filepath.Join(root, filepath.FromSlash(d)), 0o755); err != nil {
			t.Fatal(err)
		}
	}
}

// treeEntry is one entry under a test directory: its type bits, and a
// symbolic link's target or a regular file's content.
type treeEntry struct {
	typ  os.FileMode
	data string
}

func (e treeEntry) String() string {
	switch {
	case e.typ == 0:
		return strconv.Quote(e.data)
	case e.typ&os.ModeSymlink != 0:
		return "link to " + e.data
	}
	return e.typ.String()
}

// tree returns every entry under root by slash-separated relative path,
// without following links.
func tree(t *testing.T, root string) map[string]treeEntry {
	t.Helper()
	out := map[string]treeEntry{}
	err := filepath.Walk(root, func(p string, fi os.FileInfo, err error) error {
		if err != nil || p == root {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		e := treeEntry{typ: fi.Mode().Type()}
		switch {
		case fi.Mode()&os.ModeSymlink != 0:
			e.data, err = os.Readlink(p)
		case fi.Mode().IsRegular():
			var b []byte
			b, err = os.ReadFile(p)
			e.data = string(b)
		}
		out[filepath.ToSlash(rel)] = e
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// stagingFiles returns the temporary files of a download under root.
func stagingFiles(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	for p := range tree(t, root) {
		if base := path.Base(p); strings.HasPrefix(base, ".") && strings.HasSuffix(base, ".tmp") {
			out = append(out, p)
		}
	}
	return out
}

// GetToFile writes through a symbolic link, as Python's open(path, "wb")
// does: the file at the end of the links receives the object (created when
// the last link dangles), and the links stay. Relative links resolve from
// the directory they are in, as the kernel resolves them.
func TestGetToFileWritesThroughSymlinks(t *testing.T) {
	cases := []struct {
		name   string
		setup  func(t *testing.T, root string)
		path   string // given to GetToFile, relative to root
		target string // receives the object, relative to root
	}{
		{"same directory", func(t *testing.T, root string) {
			mkdirs(t, root, "d")
			symlink(t, "real.txt", filepath.Join(root, "d", "link.txt"))
		}, "d/link.txt", "d/real.txt"},
		{"another directory", func(t *testing.T, root string) {
			mkdirs(t, root, "d", "e")
			symlink(t, filepath.Join(root, "e", "real.txt"), filepath.Join(root, "d", "link.txt"))
		}, "d/link.txt", "e/real.txt"},
		{"relative, out of the directory", func(t *testing.T, root string) {
			mkdirs(t, root, "d/sub", "e")
			symlink(t, filepath.Join("..", "..", "e", "real.txt"), filepath.Join(root, "d", "sub", "link.txt"))
		}, "d/sub/link.txt", "e/real.txt"},
		{"chain of links", func(t *testing.T, root string) {
			mkdirs(t, root, "d", "e")
			symlink(t, filepath.Join(root, "e", "real.txt"), filepath.Join(root, "d", "hop"))
			symlink(t, "hop", filepath.Join(root, "d", "link.txt"))
		}, "d/link.txt", "e/real.txt"},
		// "alias" links to real/x, which holds link.txt -> ../real.txt: the
		// ".." applies to real/x, not lexically to alias.
		{"relative, behind a linked directory", func(t *testing.T, root string) {
			mkdirs(t, root, "real/x")
			symlink(t, filepath.Join(root, "real", "x"), filepath.Join(root, "alias"))
			symlink(t, filepath.Join("..", "real.txt"), filepath.Join(root, "real", "x", "link.txt"))
		}, "alias/link.txt", "real/real.txt"},
	}
	for _, tp := range transferPaths {
		for _, tc := range cases {
			for _, existing := range []bool{true, false} {
				name := tp.name + "/" + tc.name
				if !existing {
					name += ", dangling"
				}
				t.Run(name, func(t *testing.T) {
					root := t.TempDir()
					tc.setup(t, root)
					target := filepath.Join(root, filepath.FromSlash(tc.target))
					if existing {
						if err := os.WriteFile(target, []byte("old"), 0o640); err != nil {
							t.Fatal(err)
						}
					}
					want := tree(t, root)
					want[tc.target] = treeEntry{data: "hello"}

					if _, err := tp.store(t).GetToFile(context.Background(), "k", filepath.Join(root, filepath.FromSlash(tc.path))); err != nil {
						t.Fatal(err)
					}
					if got := tree(t, root); !reflect.DeepEqual(got, want) {
						t.Errorf("after GetToFile:\n%v\nwant\n%v", got, want)
					}
					if existing && runtime.GOOS != "windows" {
						if fi, err := os.Stat(target); err != nil || fi.Mode().Perm() != 0o640 {
							t.Errorf("target mode = %v, %v; want the old file's 0640", fi.Mode(), err)
						}
					}
				})
			}
		}
	}
}

// A download through a link is staged next to the file it replaces, and a
// failed one leaves the link, the target and both directories as they were.
func TestGetToFileFailedDownloadThroughASymlink(t *testing.T) {
	for _, existing := range []bool{true, false} {
		name := "existing target"
		if !existing {
			name = "dangling link"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			mkdirs(t, root, "d", "e")
			symlink(t, filepath.Join(root, "e", "real.txt"), filepath.Join(root, "d", "link.txt"))
			if existing {
				if err := os.WriteFile(filepath.Join(root, "e", "real.txt"), []byte("old"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			before := tree(t, root)
			started := make(chan struct{}, 1)
			fs, fc, _ := directStore(t, 1, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Length", strconv.Itoa(1<<20))
				_, _ = w.Write(make([]byte, 64<<10))
				w.(http.Flusher).Flush()
				started <- struct{}{}
				<-r.Context().Done()
			}, nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			errc := make(chan error, 1)
			go func() {
				_, err := fs.GetToFile(ctx, "k", filepath.Join(root, "d", "link.txt"))
				errc <- err
			}()
			select {
			case <-started:
			case err := <-errc:
				t.Fatalf("GetToFile returned before the download started: %v", err)
			}
			var staged []string
			if !eventually(func() bool { staged = stagingFiles(t, root); return len(staged) > 0 }) {
				t.Fatal("no staging file appeared")
			}
			if len(staged) != 1 || path.Dir(staged[0]) != "e" || !strings.HasPrefix(path.Base(staged[0]), ".real.txt.") {
				t.Errorf("staging files %v, want one .real.txt.*.tmp in e", staged)
			}
			cancel()
			if err := <-errc; !errors.Is(err, context.Canceled) {
				t.Fatalf("err = %v, want context.Canceled", err)
			}
			if got := tree(t, root); !reflect.DeepEqual(got, before) {
				t.Errorf("after the failed download:\n%v\nwant\n%v", got, before)
			}
			if fc.count(URIStat) != 0 {
				t.Error("stat after a failed download")
			}
		})
	}
}

// A hard-linked file is replaced, not rewritten (as documented): the name
// GetToFile is given gets the object, the file's other names keep the old
// content.
func TestGetToFileReplacesAHardLinkedName(t *testing.T) {
	dir := t.TempDir()
	a, b := filepath.Join(dir, "a.txt"), filepath.Join(dir, "b.txt")
	if err := os.WriteFile(a, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(a, b); err != nil {
		t.Skipf("hard links not supported: %v", err)
	}
	if _, err := transferPaths[0].store(t).GetToFile(context.Background(), "k", b); err != nil {
		t.Fatal(err)
	}
	if got, want := tree(t, dir), map[string]treeEntry{"a.txt": {data: "old"}, "b.txt": {data: "hello"}}; !reflect.DeepEqual(got, want) {
		t.Errorf("after GetToFile: %v, want %v", got, want)
	}
}

// replaceRename makes writeFileAtomic rename with fn for the rest of the
// test.
func replaceRename(t *testing.T, fn func(oldpath, newpath string) error) {
	t.Helper()
	orig := renameFile
	renameFile = fn
	t.Cleanup(func() { renameFile = orig })
}

// A file bind-mounted into a container is a mount point: rename(2) cannot
// replace it (EBUSY), but it can be written, as Python's open(path, "wb")
// does. The finished download is copied into it.
func TestGetToFileCopiesIntoAFileThatCannotBeReplaced(t *testing.T) {
	for _, tp := range transferPaths {
		t.Run(tp.name, func(t *testing.T) {
			replaceRename(t, func(oldpath, newpath string) error {
				return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: syscall.EBUSY}
			})
			dir := t.TempDir()
			mounted := filepath.Join(dir, "mounted.txt")
			if err := os.WriteFile(mounted, []byte("old content, longer than the object"), 0o640); err != nil {
				t.Fatal(err)
			}
			before, err := os.Stat(mounted)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tp.store(t).GetToFile(context.Background(), "k", mounted); err != nil {
				t.Fatal(err)
			}
			if got, want := tree(t, dir), map[string]treeEntry{"mounted.txt": {data: "hello"}}; !reflect.DeepEqual(got, want) {
				t.Errorf("after GetToFile: %v, want %v", got, want)
			}
			after, err := os.Stat(mounted)
			if err != nil {
				t.Fatal(err)
			}
			if !os.SameFile(before, after) {
				t.Error("the file was replaced, not written in place")
			}
		})
	}
	t.Run("other rename failures are returned", func(t *testing.T) {
		replaceRename(t, func(oldpath, newpath string) error {
			return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: syscall.EXDEV}
		})
		dir := t.TempDir()
		file := filepath.Join(dir, "a.txt")
		if err := os.WriteFile(file, []byte("old"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := transferPaths[0].store(t).GetToFile(context.Background(), "k", file); !errors.Is(err, syscall.EXDEV) {
			t.Fatalf("err = %v, want the rename's error", err)
		}
		if got, want := tree(t, dir), map[string]treeEntry{"a.txt": {data: "old"}}; !reflect.DeepEqual(got, want) {
			t.Errorf("after GetToFile: %v, want %v", got, want)
		}
	})
}

// File systems cap a name at 255 bytes, and the staging file's name is
// longer than the target's: it must still fit wherever the target's does.
func TestGetToFileLongNames(t *testing.T) {
	for _, tp := range transferPaths {
		for _, name := range []string{strings.Repeat("a", 255), strings.Repeat("界", 85)} {
			t.Run(fmt.Sprintf("%s/%d bytes of %c", tp.name, len(name), []rune(name)[0]), func(t *testing.T) {
				dir := t.TempDir()
				if _, err := tp.store(t).GetToFile(context.Background(), "k", filepath.Join(dir, name)); err != nil {
					t.Fatal(err)
				}
				if got, want := tree(t, dir), map[string]treeEntry{name: {data: "hello"}}; !reflect.DeepEqual(got, want) {
					t.Errorf("after GetToFile: %v, want %v", got, want)
				}
			})
		}
	}
}

// The staging name keeps at most 200 bytes of the target's name, cut at a
// character boundary.
func TestStagingFileName(t *testing.T) {
	dir := t.TempDir()
	cases := []struct{ base, kept string }{
		{"a.txt", "a.txt"},
		{strings.Repeat("a", 200), strings.Repeat("a", 200)},
		{strings.Repeat("a", 255), strings.Repeat("a", 200)},
		{strings.Repeat("界", 85), strings.Repeat("界", 66)},
		{strings.Repeat("a", 199) + "界界", strings.Repeat("a", 199)},
		{strings.Repeat("a", 197) + "🙂🙂", strings.Repeat("a", 197)},
	}
	for _, tc := range cases {
		f, err := createTemp(dir, tc.base, 0o666)
		if err != nil {
			t.Fatalf("%d-byte base: %v", len(tc.base), err)
		}
		name := filepath.Base(f.Name())
		_ = f.Close()
		if err := os.Remove(f.Name()); err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(name, "."+tc.kept+".") || !strings.HasSuffix(name, ".tmp") ||
			len(name) > len(tc.kept)+19 || !utf8.ValidString(name) {
			t.Errorf("%d-byte base: staging name %q (%d bytes), want .%s.<random>.tmp", len(tc.base), name, len(name), tc.kept)
		}
	}
}
