//go:build unix

package filestore

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// A FIFO cannot be replaced by a rename without cutting off its reader:
// GetToFile writes into it, directly or through a link, as Python's
// open(path, "wb") does.
func TestGetToFileWritesIntoAFIFO(t *testing.T) {
	for _, tp := range transferPaths {
		for _, viaLink := range []bool{false, true} {
			name := tp.name
			if viaLink {
				name += " through a symlink"
			}
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				fifo := filepath.Join(dir, "pipe")
				if err := syscall.Mkfifo(fifo, 0o600); err != nil {
					t.Fatal(err)
				}
				want := map[string]treeEntry{"pipe": {typ: os.ModeNamedPipe}}
				target := fifo
				if viaLink {
					target = filepath.Join(dir, "link")
					symlink(t, "pipe", target)
					want["link"] = treeEntry{typ: os.ModeSymlink, data: "pipe"}
				}
				// A second name for the FIFO, outside dir: were the FIFO replaced,
				// the reader below could still be released through it.
				keep := filepath.Join(t.TempDir(), "keep")
				if err := os.Link(fifo, keep); err != nil {
					t.Fatal(err)
				}
				var received []byte
				done := make(chan struct{})
				go func() {
					defer close(done)
					// Opening blocks until a writer opens the FIFO.
					received, _ = os.ReadFile(fifo)
				}()
				t.Cleanup(func() {
					if f, err := os.OpenFile(keep, os.O_RDWR, 0); err == nil {
						_ = f.Close()
					}
					<-done
				})

				if _, err := tp.store(t).GetToFile(context.Background(), "k", target); err != nil {
					t.Fatal(err)
				}
				if got := tree(t, dir); !reflect.DeepEqual(got, want) {
					t.Fatalf("after GetToFile: %v, want %v", got, want)
				}
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					t.Fatal("the reader got no end of file")
				}
				if string(received) != "hello" {
					t.Errorf("the reader got %q, want hello", received)
				}
			})
		}
	}
}

// setUmask sets the process umask for the rest of the test (the package's
// tests do not run in parallel).
func setUmask(t *testing.T, mask int) {
	t.Helper()
	old := syscall.Umask(mask)
	t.Cleanup(func() { syscall.Umask(old) })
}

// recordStagingModes makes createTemp record the permission bits each
// staging file has when it is created, for the rest of the test.
func recordStagingModes(t *testing.T) *[]os.FileMode {
	t.Helper()
	var modes []os.FileMode
	orig := openStaging
	openStaging = func(name string, flag int, perm os.FileMode) (*os.File, error) {
		f, err := orig(name, flag, perm)
		if err == nil {
			fi, serr := f.Stat()
			if serr != nil {
				t.Errorf("stat of the new staging file: %v", serr)
			} else {
				modes = append(modes, fi.Mode().Perm())
			}
		}
		return f, err
	}
	t.Cleanup(func() { openStaging = orig })
	return &modes
}

// A replacement is staged in the target's directory, where other users may
// list it (a 0755 /data or /shared): the staging file must never be more
// permissive than the file it replaces, from its creation on, or another
// local user could open it and keep the descriptor while the download is
// written into it. It is created for its owner only: until it has the
// target's group, the target's group bits would apply to the process's
// group. A new file gets 0666 minus the umask.
func TestGetToFileStagesWithTheTargetsPermissions(t *testing.T) {
	for _, tp := range transferPaths {
		for _, perm := range []os.FileMode{0o600, 0o640, 0o400, 0o755} {
			t.Run(tp.name+"/"+perm.String(), func(t *testing.T) {
				setUmask(t, 0) // a wide creation mode must not hide behind the umask
				modes := recordStagingModes(t)
				path := filepath.Join(t.TempDir(), "secret.json")
				if err := os.WriteFile(path, []byte("old"), perm); err != nil {
					t.Fatal(err)
				}
				if _, err := tp.store(t).GetToFile(context.Background(), "k", path); err != nil {
					t.Fatal(err)
				}
				if want := perm & 0o700; len(*modes) != 1 || (*modes)[0] != want {
					t.Errorf("staging file created with %v over a %v target, want %v", *modes, perm, want)
				}
				fi, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				if fi.Mode().Perm() != perm || string(readFile(t, path)) != "hello" {
					t.Errorf("after GetToFile: mode %v, content %q", fi.Mode().Perm(), readFile(t, path))
				}
			})
		}
		for _, mask := range []int{0, 0o022, 0o077} {
			t.Run(tp.name+"/new file/umask "+strconv.FormatInt(int64(mask), 8), func(t *testing.T) {
				setUmask(t, mask)
				modes := recordStagingModes(t)
				path := filepath.Join(t.TempDir(), "new.json")
				if _, err := tp.store(t).GetToFile(context.Background(), "k", path); err != nil {
					t.Fatal(err)
				}
				want := os.FileMode(0o666 &^ mask)
				if len(*modes) != 1 || (*modes)[0] != want {
					t.Errorf("staging file created with %v, want %v", *modes, want)
				}
				if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != want {
					t.Errorf("new file mode %v, %v; want %v", fi.Mode().Perm(), err, want)
				}
			})
		}
	}
}

// ownerOf returns the owner and group of the file at path.
func ownerOf(t *testing.T, path string) (uid, gid int) {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	st := fi.Sys().(*syscall.Stat_t)
	return int(st.Uid), int(st.Gid)
}

// otherGroup returns a group of the process other than the one a new file in
// dir gets, skipping the test where there is none.
func otherGroup(t *testing.T, dir string) int {
	t.Helper()
	probe := filepath.Join(dir, ".probe")
	if err := os.WriteFile(probe, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	_, natural := ownerOf(t, probe)
	if err := os.Remove(probe); err != nil {
		t.Fatal(err)
	}
	groups, err := os.Getgroups()
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range groups {
		if g != natural {
			return g
		}
	}
	t.Skip("the process is a member of only one group")
	return 0
}

// A replaced file is a new file, the process's own. As far as the process
// may, it gets the old file's owner and group (a host user or another app's
// user keeps writing a file in /data or /shared after the app replaced it);
// a process that may not give a file away keeps at least the group, when it
// is a member of it.
func TestGetToFileKeepsTheGroup(t *testing.T) {
	for _, tp := range transferPaths {
		t.Run(tp.name, func(t *testing.T) {
			dir := t.TempDir()
			group := otherGroup(t, dir)
			path := filepath.Join(dir, "shared.txt")
			if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, 0o664); err != nil {
				t.Fatal(err)
			}
			if err := os.Chown(path, -1, group); err != nil {
				t.Fatal(err)
			}
			if _, err := tp.store(t).GetToFile(context.Background(), "k", path); err != nil {
				t.Fatal(err)
			}
			if uid, gid := ownerOf(t, path); uid != os.Geteuid() || gid != group {
				t.Errorf("owner %d:%d, want %d:%d", uid, gid, os.Geteuid(), group)
			}
			if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o664 || string(readFile(t, path)) != "hello" {
				t.Errorf("mode %v (%v), content %q", fi.Mode().Perm(), err, readFile(t, path))
			}
		})
	}
}

// As root (the usual user of an app container), the owner is kept too.
func TestGetToFileKeepsTheOwner(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("giving a file away needs root")
	}
	for _, tp := range transferPaths {
		t.Run(tp.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(path, []byte("old"), 0o640); err != nil {
				t.Fatal(err)
			}
			if err := os.Chown(path, 1000, 1001); err != nil {
				t.Fatal(err)
			}
			if _, err := tp.store(t).GetToFile(context.Background(), "k", path); err != nil {
				t.Fatal(err)
			}
			if uid, gid := ownerOf(t, path); uid != 1000 || gid != 1001 {
				t.Errorf("owner %d:%d, want 1000:1001", uid, gid)
			}
			if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o640 || string(readFile(t, path)) != "hello" {
				t.Errorf("mode %v (%v), content %q", fi.Mode().Perm(), err, readFile(t, path))
			}
		})
	}
}

// Keeping the owner is best effort: where the process may not give the file
// its old owner or group, the download still succeeds, and the file is the
// process's own.
func TestGetToFileOwnerIsBestEffort(t *testing.T) {
	orig := chownFile
	var attempts int
	chownFile = func(f *os.File, uid, gid int) error {
		attempts++
		return &os.PathError{Op: "chown", Path: f.Name(), Err: syscall.EPERM}
	}
	t.Cleanup(func() { chownFile = orig })
	dir := t.TempDir()
	group := otherGroup(t, dir)
	path := filepath.Join(dir, "shared.txt")
	if err := os.WriteFile(path, []byte("old"), 0o664); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(path, -1, group); err != nil {
		t.Fatal(err)
	}
	if _, err := transferPaths[0].store(t).GetToFile(context.Background(), "k", path); err != nil {
		t.Fatal(err)
	}
	if attempts == 0 {
		t.Error("no attempt to keep the owner")
	}
	if _, gid := ownerOf(t, path); gid == group {
		t.Error("the group was kept although chown failed")
	}
	if string(readFile(t, path)) != "hello" {
		t.Errorf("content %q", readFile(t, path))
	}
}

// A process that may not give the file its old owner (it is not root, and
// the file was another user's) keeps at least the old group, when it is a
// member of it: the full chown fails, the group-only one does not.
func TestGetToFileKeepsTheGroupWithoutTheOwner(t *testing.T) {
	orig := chownFile
	chownFile = func(f *os.File, uid, gid int) error {
		if uid != -1 {
			return &os.PathError{Op: "chown", Path: f.Name(), Err: syscall.EPERM}
		}
		return orig(f, uid, gid)
	}
	t.Cleanup(func() { chownFile = orig })
	dir := t.TempDir()
	group := otherGroup(t, dir)
	path := filepath.Join(dir, "shared.txt")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o664); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(path, -1, group); err != nil {
		t.Fatal(err)
	}
	if _, err := transferPaths[0].store(t).GetToFile(context.Background(), "k", path); err != nil {
		t.Fatal(err)
	}
	if uid, gid := ownerOf(t, path); uid != os.Geteuid() || gid != group {
		t.Errorf("owner %d:%d, want %d:%d", uid, gid, os.Geteuid(), group)
	}
	if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o664 || string(readFile(t, path)) != "hello" {
		t.Errorf("mode %v (%v), content %q", fi.Mode().Perm(), err, readFile(t, path))
	}
}

// The staging file gets the target's permission bits without its special
// bits, before anything is written into it. (A write by a process without
// CAP_FSETID makes the kernel clear set-ID bits itself, so the end result
// that TestGetToFileDropsSpecialBits checks shows a carried-over bit only
// when the test runs as root.)
func TestReplaceFileStagesWithoutSpecialBits(t *testing.T) {
	for _, special := range []os.FileMode{os.ModeSetuid, os.ModeSetgid} {
		t.Run(special.String(), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "tool")
			if err := os.WriteFile(path, []byte("old"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, 0o755|special); err != nil {
				t.Skipf("cannot set %v here: %v", special, err)
			}
			fi, err := os.Stat(path)
			if err != nil || fi.Mode()&special == 0 {
				t.Skipf("%v did not stick: %v", special, err)
			}
			var staged os.FileMode
			err = replaceFile(path, fi, func(w io.Writer) error {
				st, err := w.(*os.File).Stat()
				if err != nil {
					return err
				}
				staged = st.Mode()
				_, err = io.WriteString(w, "new")
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			if staged&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 || staged.Perm() != 0o755 {
				t.Errorf("staging file mode %v before the write, want -rwxr-xr-x", staged)
			}
		})
	}
}

// The set-user-ID and set-group-ID bits are not carried over to the new
// file: on a file whose owner could not be kept, they would apply to the
// process's own user.
func TestGetToFileDropsSpecialBits(t *testing.T) {
	for _, special := range []os.FileMode{os.ModeSetuid, os.ModeSetgid} {
		t.Run(special.String(), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "tool")
			if err := os.WriteFile(path, []byte("old"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, 0o755|special); err != nil {
				t.Skipf("cannot set %v here: %v", special, err)
			}
			if fi, err := os.Stat(path); err != nil || fi.Mode()&special == 0 {
				t.Skipf("%v did not stick: %v", special, err)
			}
			if _, err := transferPaths[0].store(t).GetToFile(context.Background(), "k", path); err != nil {
				t.Fatal(err)
			}
			fi, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if fi.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 || fi.Mode().Perm() != 0o755 {
				t.Errorf("mode %v, want -rwxr-xr-x", fi.Mode())
			}
		})
	}
}

// GetToFile reaches its path as open(2) (and Python's open(path, "wb"))
// reaches it, not by cleaning it lexically first: a ".." applies to the
// directory a symbolic link before it leads to.
func TestGetToFileResolvesDotDotAfterLinks(t *testing.T) {
	for _, tp := range transferPaths {
		for _, relative := range []bool{false, true} {
			name := tp.name + "/absolute link"
			if relative {
				name = tp.name + "/relative link"
			}
			t.Run(name, func(t *testing.T) {
				root := t.TempDir()
				mkdirs(t, root, "data", "mnt/disk/2024")
				target := filepath.Join(root, "mnt", "disk", "2024")
				if relative {
					target = filepath.Join("..", "mnt", "disk", "2024")
				}
				symlink(t, target, filepath.Join(root, "data", "current"))
				want := tree(t, root)
				want["mnt/disk/out.bin"] = treeEntry{data: "hello"}
				want["mnt/disk/sub"] = treeEntry{typ: os.ModeDir}
				want["mnt/disk/sub/out.bin"] = treeEntry{data: "hello"}

				for _, p := range []string{"data/current/../out.bin", "data/current/../sub/out.bin"} {
					if _, err := tp.store(t).GetToFile(context.Background(), "k", root+"/"+p); err != nil {
						t.Fatalf("%s: %v", p, err)
					}
				}
				if got := tree(t, root); !reflect.DeepEqual(got, want) {
					t.Errorf("after GetToFile:\n%v\nwant\n%v", got, want)
				}
			})
		}
	}
}

// A relative path is taken from the working directory as the system knows
// it, not from $PWD, which a shell sets to the path it was entered through.
func TestGetToFileRelativeToThePhysicalWorkingDirectory(t *testing.T) {
	for _, tp := range transferPaths {
		t.Run(tp.name, func(t *testing.T) {
			root := t.TempDir()
			mkdirs(t, root, "projects", "mnt/disk/app")
			symlink(t, filepath.Join(root, "mnt", "disk", "app"), filepath.Join(root, "projects", "app"))
			t.Chdir(filepath.Join(root, "projects", "app")) // sets PWD to this logical path
			want := tree(t, root)
			want["mnt/disk/out.bin"] = treeEntry{data: "hello"}
			if _, err := tp.store(t).GetToFile(context.Background(), "k", "../out.bin"); err != nil {
				t.Fatal(err)
			}
			if got := tree(t, root); !reflect.DeepEqual(got, want) {
				t.Errorf("after GetToFile:\n%v\nwant\n%v", got, want)
			}
		})
	}
}

// A path that ends in "/" (or "." or "..") names a directory, as open(2)
// takes it: GetToFile fails and writes nothing, rather than write a file
// named like the directory.
func TestGetToFileDirectoryPaths(t *testing.T) {
	for _, tp := range transferPaths {
		for _, p := range []string{"downloads/", "f.txt/", "d/.", "d/..", "d//"} {
			t.Run(tp.name+"/"+p, func(t *testing.T) {
				root := t.TempDir()
				mkdirs(t, root, "d")
				if err := os.WriteFile(filepath.Join(root, "f.txt"), []byte("old"), 0o644); err != nil {
					t.Fatal(err)
				}
				before := tree(t, root)
				_, err := tp.store(t).GetToFile(context.Background(), "k", root+"/"+p)
				if !errors.Is(err, syscall.EISDIR) {
					t.Errorf("err = %v, want EISDIR", err)
				}
				if got := tree(t, root); !reflect.DeepEqual(got, before) {
					t.Errorf("after GetToFile:\n%v\nwant\n%v", got, before)
				}
			})
		}
	}
	t.Run("empty path", func(t *testing.T) {
		if _, err := transferPaths[0].store(t).GetToFile(context.Background(), "k", ""); !errors.Is(err, syscall.ENOENT) {
			t.Errorf("err = %v, want ENOENT", err)
		}
	})
}

// A link to a device, like /dev/stdout, is written through; the link stays.
func TestGetToFileWritesThroughALinkToADevice(t *testing.T) {
	for _, tp := range transferPaths {
		t.Run(tp.name, func(t *testing.T) {
			dir := t.TempDir()
			link := filepath.Join(dir, "out")
			symlink(t, os.DevNull, link)
			if _, err := tp.store(t).GetToFile(context.Background(), "k", link); err != nil {
				t.Fatal(err)
			}
			want := map[string]treeEntry{"out": {typ: os.ModeSymlink, data: os.DevNull}}
			if got := tree(t, dir); !reflect.DeepEqual(got, want) {
				t.Errorf("after GetToFile: %v, want %v", got, want)
			}
		})
	}
}
