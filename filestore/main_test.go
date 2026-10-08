package filestore

import (
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"
)

// TestMain fails the run when goroutines outlive the tests: every transfer,
// fetch and iteration must wind down once its call returns (goleak is not a
// dependency of this module, so this is a coarse stand-in).
func TestMain(m *testing.M) {
	before := runtime.NumGoroutine()
	code := m.Run()
	if code == 0 {
		deadline := time.Now().Add(5 * time.Second)
		for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if n := runtime.NumGoroutine(); n > before {
			buf := make([]byte, 1<<20)
			buf = buf[:runtime.Stack(buf, true)]
			fmt.Fprintf(os.Stderr, "goroutine leak: %d goroutines after the tests, %d before\n%s\n", n, before, buf)
			code = 1
		}
	}
	os.Exit(code)
}
