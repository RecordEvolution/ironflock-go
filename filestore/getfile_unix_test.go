//go:build unix

package filestore

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
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
