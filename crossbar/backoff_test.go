package crossbar

import (
	"sync"
	"testing"
	"time"
)

type fakeClock struct{ now time.Time }

func (c *fakeClock) advance(d time.Duration) { c.now = c.now.Add(d) }

func newTestBackoff(clock *fakeClock) *backoff {
	t := defaultTunables()
	t.retryDelayJitter = 0
	t.now = func() time.Time { return clock.now }
	return newBackoff(&t)
}

func TestBackoffGrowsAndCaps(t *testing.T) {
	b := newTestBackoff(&fakeClock{now: time.Unix(1000, 0)})
	var got []time.Duration
	for range 5 {
		b.observe(reasonTransportLost)
		got = append(got, b.next())
	}
	want := []time.Duration{time.Second, 1500 * time.Millisecond, 2 * time.Second, 2 * time.Second, 2 * time.Second}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("delays = %v, want %v", got, want)
		}
	}
	b.joined()
	if d := b.next(); d != time.Second {
		t.Fatalf("delay after join = %v, want 1s", d)
	}
}

func TestBackoffJitter(t *testing.T) {
	tun := defaultTunables()
	for _, r := range []float64{0, 0.5, 0.999} {
		tun.rand = func() float64 { return r }
		d := newBackoff(&tun).next()
		if d < 900*time.Millisecond || d > 1100*time.Millisecond {
			t.Fatalf("jittered delay %v outside ±10%% of 1s (rand=%v)", d, r)
		}
	}
}

// The no_such_realm streak mirrors the Python SDK's tests: the cap widens
// to 120s once the realm has been missing for 60s (inclusive), and a join or
// any other close reason restores the fast cap and restarts the streak.
func TestBackoffNoSuchRealmStreak(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	b := newTestBackoff(clock)

	if b.observe(ErrURINoSuchRealm) { // streak starts
		t.Fatal("slowed on first refusal")
	}
	clock.advance(30 * time.Second)
	if b.observe(ErrURINoSuchRealm) || b.maxDelay() != BaseMaxRetryDelay {
		t.Fatal("slowed before 60s")
	}
	clock.advance(30 * time.Second) // exactly 60s
	if !b.observe(ErrURINoSuchRealm) || b.maxDelay() != NoSuchRealmMaxRetryDelay {
		t.Fatal("not slowed at 60s")
	}
	clock.advance(600 * time.Second)
	if b.observe(ErrURINoSuchRealm) {
		t.Fatal("slow-down reported twice")
	}
	if b.maxDelay() != NoSuchRealmMaxRetryDelay {
		t.Fatal("cap not kept while the realm stays missing")
	}

	// The delay grows gradually towards the wider cap.
	var prev time.Duration
	for range 20 {
		d := b.next()
		if d < prev || d > NoSuchRealmMaxRetryDelay {
			t.Fatalf("delay %v after %v", d, prev)
		}
		prev = d
	}
	if prev != NoSuchRealmMaxRetryDelay {
		t.Fatalf("delay never reached the wide cap: %v", prev)
	}

	// Another close reason resets the cap and the streak.
	b.observe(reasonTransportLost)
	if b.maxDelay() != BaseMaxRetryDelay || b.next() != BaseMaxRetryDelay {
		t.Fatal("other close reason did not reset the cap")
	}
	b.observe(ErrURINoSuchRealm)
	clock.advance(30 * time.Second)
	if b.observe(ErrURINoSuchRealm) {
		t.Fatal("new streak inherited the old one")
	}

	// So does a join.
	clock.advance(60 * time.Second)
	if !b.observe(ErrURINoSuchRealm) {
		t.Fatal("not slowed")
	}
	b.joined()
	if b.maxDelay() != BaseMaxRetryDelay || b.next() != time.Second {
		t.Fatal("join did not reset the policy")
	}
	b.observe(ErrURINoSuchRealm)
	clock.advance(5 * time.Second)
	if b.observe(ErrURINoSuchRealm) {
		t.Fatal("slowed right after a join")
	}
}

func TestSerialExecutorOrderAndClose(t *testing.T) {
	var e serialExecutor
	var mu sync.Mutex
	var got []int
	done := make(chan struct{})
	for i := range 100 {
		e.submit(func() {
			mu.Lock()
			got = append(got, i)
			mu.Unlock()
			if i == 99 {
				close(done)
			}
		})
	}
	<-done
	mu.Lock()
	for i, v := range got {
		if v != i {
			t.Fatalf("out of order at %d: %v", i, got)
		}
	}
	mu.Unlock()

	block := make(chan struct{})
	started := make(chan struct{})
	ran := make(chan struct{}, 1)
	e.submit(func() { close(started); <-block })
	e.submit(func() { ran <- struct{}{} })
	<-started
	e.close()
	close(block)
	if e.submit(func() {}) {
		t.Fatal("submit accepted after close")
	}
	select {
	case <-ran:
		t.Fatal("queued function ran after close")
	case <-time.After(50 * time.Millisecond):
	}
}
