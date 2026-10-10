//go:build !unix

package filestore

import "os"

// keepOwner does nothing where files have no Unix owner and group.
func keepOwner(*os.File, os.FileInfo) {}
