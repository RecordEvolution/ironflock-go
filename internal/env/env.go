// Package env reads the values the IronFlock device agent injects into an app
// container.
package env

import (
	"os"
	"path/filepath"
	"strings"
)

// DefaultDir is where the device agent mirrors injected values, one file per
// name. Overridable with the IRONFLOCK_ENV_DIR environment variable.
const DefaultDir = "/data/env"

// Dir returns the directory holding the agent-maintained value files.
func Dir() string {
	if dir, ok := os.LookupEnv("IRONFLOCK_ENV_DIR"); ok {
		return dir
	}
	return DefaultDir
}

// ReadInjected returns a platform-injected value by name: the live env FILE
// first, the environment variable second.
//
// The device agent mirrors injected values to /data/env/{name}.txt (a bind
// mount) — the only channel that reaches a RUNNING container, so a rotated
// credential or a tunnel port allocated after start is picked up without a
// restart. Environment variables are the start-time snapshot and serve as the
// fallback. An empty (or whitespace-only) file counts as absent.
func ReadInjected(name string) string {
	if data, err := os.ReadFile(filepath.Join(Dir(), name+".txt")); err == nil {
		if value := strings.TrimSpace(string(data)); value != "" {
			return value
		}
	}
	return os.Getenv(name)
}
