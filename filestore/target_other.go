//go:build !unix

package filestore

import (
	"os"
	"path/filepath"
)

// prepareTarget creates the parent directories of path and returns path
// made absolute. Windows resolves ".." in a path lexically itself, so
// cleaning it first changes nothing.
func prepareTarget(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return abs, os.MkdirAll(filepath.Dir(abs), 0o777)
}
