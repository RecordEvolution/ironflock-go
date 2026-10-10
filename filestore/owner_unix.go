//go:build unix

package filestore

import (
	"os"
	"syscall"
)

// chownFile is (*os.File).Chown; tests replace it to simulate a process
// that may not change a file's owner or group.
var chownFile = (*os.File).Chown

// keepOwner gives the new file f, which is to replace the file fi
// describes, that file's owner and group, as far as the process may: a
// process that may not give a file away keeps at least the group, when it is
// a member of it. Failures are ignored (EPERM where the process may not, or
// EINVAL for an ID a user namespace does not map): f then stays the
// process's own.
func keepOwner(f *os.File, fi os.FileInfo) {
	want, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return
	}
	cur, err := f.Stat()
	if err != nil {
		return
	}
	have, ok := cur.Sys().(*syscall.Stat_t)
	if !ok || (have.Uid == want.Uid && have.Gid == want.Gid) {
		return
	}
	if chownFile(f, int(want.Uid), int(want.Gid)) == nil || have.Gid == want.Gid {
		return
	}
	_ = chownFile(f, -1, int(want.Gid))
}
