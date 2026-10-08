// Uses the underlying self-healing WAMP connection directly, without the
// IronFlock facade.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/RecordEvolution/ironflock-go/crossbar"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn := crossbar.NewConnection()
	err := conn.Configure(crossbar.Config{
		SwarmKey: 123,
		AppKey:   456,
		Stage:    crossbar.StageFromEnv(os.Getenv("ENV")),
		// URL, serial number and credential come from the environment when
		// left empty.
		OnConnect:    func() { log.Print("connected") },
		OnDisconnect: func(reason string) { log.Printf("disconnected: %s", reason) },
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := conn.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer conn.Stop(context.Background())

	_, err = conn.Subscribe(ctx, "com.example.status", func(ev *crossbar.Event) {
		log.Printf("status: %v %v", ev.Args, ev.Kwargs)
	}, &crossbar.SubscribeOptions{})
	if err != nil {
		log.Fatal(err)
	}
	err = conn.Publish(ctx, "com.example.status", []any{"online"}, nil,
		&crossbar.PublishOptions{Acknowledge: true}, 0)
	if err != nil {
		log.Fatal(err)
	}
	<-ctx.Done()
}
