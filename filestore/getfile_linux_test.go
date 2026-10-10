//go:build linux

package filestore

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
)

// The case behind the EBUSY fallback for real: a single file bind-mounted
// over the target, as Docker mounts a file volume. Mounting needs
// CAP_SYS_ADMIN, so the test runs only where it can mount (as root, or in a
// privileged container).
func TestGetToFileIntoABindMountedFile(t *testing.T) {
	for _, tp := range transferPaths {
		t.Run(tp.name, func(t *testing.T) {
			dir := t.TempDir()
			host, target := filepath.Join(dir, "host.txt"), filepath.Join(dir, "target.txt")
			for _, p := range []string{host, target} {
				if err := os.WriteFile(p, []byte("old"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			if err := syscall.Mount(host, target, "", syscall.MS_BIND, ""); err != nil {
				t.Skipf("cannot bind-mount here: %v", err)
			}
			t.Cleanup(func() {
				if err := syscall.Unmount(target, 0); err != nil {
					t.Errorf("unmount: %v", err)
				}
			})
			if _, err := tp.store(t).GetToFile(context.Background(), "k", target); err != nil {
				t.Fatal(err)
			}
			want := map[string]treeEntry{"host.txt": {data: "hello"}, "target.txt": {data: "hello"}}
			if got := tree(t, dir); !reflect.DeepEqual(got, want) {
				t.Errorf("after GetToFile: %v, want %v", got, want)
			}
		})
	}
}
