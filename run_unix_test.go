//go:build unix

package ironflock

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestRunStopsOnSIGTERM(t *testing.T) {
	checkGoroutines(t)
	f := unstartedFlock(t)
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

// signalChildEnv makes TestRunSecondSignalTerminatesTheProcess run as the
// child process: Run with a main that takes until its stdin is closed to
// wind down.
const signalChildEnv = "IRONFLOCK_TEST_SIGNAL_CHILD"

// Once shutdown has begun, Run no longer handles SIGINT and SIGTERM: a
// second signal terminates the process while main is still winding down,
// as a second Ctrl-C ends a Python app run by asyncio.run.
func TestRunSecondSignalTerminatesTheProcess(t *testing.T) {
	if os.Getenv(signalChildEnv) != "" {
		runSignalChild(t)
		return
	}
	for _, sig := range []syscall.Signal{syscall.SIGINT, syscall.SIGTERM} {
		t.Run(sig.String(), func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestRunSecondSignalTerminatesTheProcess$", "-test.v")
			// The child's temporary directories outlive it when it is
			// killed: put them where this test removes them.
			cmd.Env = append(os.Environ(), signalChildEnv+"=1", "TMPDIR="+t.TempDir())
			stdin, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = stdin.Close() }() // lets a child that survived wind down
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			lines := make(chan string, 64)
			go func() {
				defer close(lines)
				scanner := bufio.NewScanner(stdout)
				for scanner.Scan() {
					lines <- scanner.Text()
				}
			}()
			var output strings.Builder
			await := func(marker string) {
				t.Helper()
				deadline := time.After(10 * time.Second)
				for {
					select {
					case line, ok := <-lines:
						if !ok {
							t.Fatalf("the child exited before %q:\n%s", marker, output.String())
						}
						output.WriteString(line + "\n")
						if line == marker {
							return
						}
					case <-deadline:
						t.Fatalf("timed out waiting for %q:\n%s", marker, output.String())
					}
				}
			}
			signal := func() {
				if err := cmd.Process.Signal(sig); err != nil && !errors.Is(err, os.ErrProcessDone) {
					t.Fatal(err)
				}
			}

			await("CHILD READY")
			signal()
			await("CHILD WINDING DOWN")
			// Signal again until the child dies. Run stops handling signals
			// as soon as shutdown begins, but on a goroutine of its own, so
			// the first of these may come just before that.
			exited := false
			for deadline := time.Now().Add(5 * time.Second); !exited && time.Now().Before(deadline); {
				signal()
				select {
				case line, ok := <-lines:
					if !ok {
						exited = true
					} else {
						output.WriteString(line + "\n")
					}
				case <-time.After(20 * time.Millisecond):
				}
			}
			if !exited {
				_ = stdin.Close() // the bug: main winds down and Run returns
				for line := range lines {
					output.WriteString(line + "\n")
				}
			}
			err = cmd.Wait()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) {
				t.Fatalf("the child survived the second %v (exit: %v):\n%s", sig, err, output.String())
			}
			if ws, ok := exitErr.Sys().(syscall.WaitStatus); !ok || !ws.Signaled() || ws.Signal() != sig {
				t.Fatalf("the child ended with %v, want killed by %v:\n%s", err, sig, output.String())
			}
		})
	}
}

// runSignalChild is the child of TestRunSecondSignalTerminatesTheProcess.
func runSignalChild(t *testing.T) {
	f := unstartedFlock(t)
	err := f.Run(context.Background(), func(ctx context.Context) error {
		fmt.Println("CHILD READY")
		<-ctx.Done()
		fmt.Println("CHILD WINDING DOWN")
		_, err := io.Copy(io.Discard, os.Stdin) // until the parent closes it
		return err
	})
	fmt.Println("CHILD RUN RETURNED", err)
}
