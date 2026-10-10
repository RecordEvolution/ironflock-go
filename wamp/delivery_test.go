package wamp

import (
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	nxwamp "github.com/ironflock/nexus/v3/wamp"
)

// Subscriptions that share a DeliveryGroup deliver their events one at a
// time, in the order they arrive, across all of them — as SubscribeToTable
// needs for its row and bulk feeds (finding 9).
func TestDeliveryGroupDeliversOneAtATimeInArrivalOrder(t *testing.T) {
	tr := newTestRouter(t, true)
	c, _ := startTestConn(t, tr)
	ctx := ctxTimeout(t, 10*time.Second)

	const n = 200
	topics := []string{"dg.rows", "dg.bulk"}
	var active atomic.Int32
	var overlapped atomic.Bool
	// Written without a lock: under -race, any two handler calls that are
	// not ordered one after the other are reported.
	var order []string
	perTopic := map[string]int{}
	done := make(chan struct{})
	handler := func(ev *Event) {
		if active.Add(1) > 1 {
			overlapped.Store(true)
		}
		defer active.Add(-1)
		v := ev.Args[0].(int64)
		time.Sleep(time.Duration(v%3) * 50 * time.Microsecond) // a slow handler: events queue up
		order = append(order, fmt.Sprintf("%s:%d", ev.Topic, v))
		perTopic[ev.Topic]++
		if len(order) == len(topics)*n {
			close(done)
		}
	}
	group := NewDeliveryGroup()
	for _, topic := range topics {
		if _, err := c.Subscribe(ctx, topic, handler, &SubscribeOptions{Group: group}); err != nil {
			t.Fatal(err)
		}
	}

	pub := tr.local(t)
	var want []string
	for i := range n {
		for _, topic := range topics {
			if err := pub.Publish(topic, nil, nxwamp.List{i}, nil); err != nil {
				t.Fatal(err)
			}
			want = append(want, fmt.Sprintf("%s:%d", topic, i))
		}
	}
	recv(t, done, "every event")
	if overlapped.Load() {
		t.Fatal("handlers of the group ran concurrently")
	}
	if !slices.Equal(order, want) {
		for i := range order {
			if order[i] != want[i] {
				t.Fatalf("event %d is %s, want %s (not in arrival order)", i, order[i], want[i])
			}
		}
	}
	if perTopic["dg.rows"] != n || perTopic["dg.bulk"] != n {
		t.Fatalf("events per topic: %v", perTopic)
	}
}

// Events still queued in the group for a subscription that is unsubscribed
// are dropped; the group goes on with the other subscriptions.
func TestDeliveryGroupDropsQueuedEventsOfUnsubscribed(t *testing.T) {
	tr := newTestRouter(t, true)
	c, _ := startTestConn(t, tr)
	ctx := ctxTimeout(t, 10*time.Second)
	group := NewDeliveryGroup()

	blocking := make(chan struct{})
	release := make(chan struct{})
	evB := make(chan *Event, 8)
	if _, err := c.Subscribe(ctx, "dg.b", func(ev *Event) {
		if ev.Args[0] == "block" {
			close(blocking)
			<-release
		}
		evB <- ev
	}, &SubscribeOptions{Group: group}); err != nil {
		t.Fatal(err)
	}
	hA, evA := eventCollector(8)
	subA, err := c.Subscribe(ctx, "dg.a", hA, &SubscribeOptions{Group: group})
	if err != nil {
		t.Fatal(err)
	}
	hC, evC := eventCollector(8)
	if _, err := c.Subscribe(ctx, "dg.c", hC, nil); err != nil { // not in the group
		t.Fatal(err)
	}

	pub := tr.local(t)
	localPublish(t, pub, "dg.b", nxwamp.List{"block"}, nil)
	recv(t, blocking, "the group to be busy")
	localPublish(t, pub, "dg.a", nxwamp.List{1}, nil)
	localPublish(t, pub, "dg.a", nxwamp.List{2}, nil)
	// The router delivers in publish order: once dg.c's event is handled,
	// dg.a's events are queued in the group, behind the busy handler.
	localPublish(t, pub, "dg.c", nxwamp.List{"barrier"}, nil)
	recv(t, evC, "the barrier event")
	if err := subA.Unsubscribe(ctx); err != nil {
		t.Fatal(err)
	}

	close(release)
	recv(t, evB, "the blocking event")
	localPublish(t, pub, "dg.b", nxwamp.List{"after"}, nil)
	if ev := recv(t, evB, "an event after the unsubscribe"); ev.Args[0] != "after" {
		t.Fatalf("event = %+v", ev)
	}
	noRecv(t, evA, "a queued event of the unsubscribed subscription")
}

// Handlers of the same topic in one group get each event in turn, and a
// subscription outside the group keeps its own order.
func TestDeliveryGroupWithHandlersOfOneTopic(t *testing.T) {
	tr := newTestRouter(t, true)
	c, _ := startTestConn(t, tr)
	ctx := ctxTimeout(t, 10*time.Second)
	group := NewDeliveryGroup()
	got := make(chan string, 16)
	for _, name := range []string{"h1", "h2"} {
		if _, err := c.Subscribe(ctx, "dg.same", func(ev *Event) {
			got <- fmt.Sprintf("%s:%d", name, ev.Args[0])
		}, &SubscribeOptions{Group: group}); err != nil {
			t.Fatal(err)
		}
	}
	pub := tr.local(t)
	for i := range 3 {
		localPublish(t, pub, "dg.same", nxwamp.List{i}, nil)
	}
	var order []string
	for range 6 {
		order = append(order, recv(t, got, "an event"))
	}
	if want := []string{"h1:0", "h2:0", "h1:1", "h2:1", "h1:2", "h2:2"}; !slices.Equal(order, want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
}
