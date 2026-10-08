//go:build unix

package ironflock

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"
)

func TestRunStopsOnSIGTERM(t *testing.T) {
	checkGoroutines(t)
	f := flock(t)
	err := f.Run(context.Background(), func(ctx context.Context) error {
		// Run has installed its signal handler before main runs.
		if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(5 * time.Second):
			return errors.New("SIGTERM did not cancel main's context")
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if f.own.stopCount() != 1 {
		t.Error("Run did not stop the connection")
	}
}
