// purge-nats clears all JetStream pipeline messages. Dev/ops tool.
//
// Usage (from the pkg module directory):
//
//	go run ./cmd/purge-nats
//
// Destructive: removes every queued/in-flight pipeline job, upload event,
// terminal event, and webhook delivery message. Consumers and streams stay.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/Ajay01103/go-mux/pkg/events"
)

func main() {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}
	js, nc, err := events.Connect(context.Background(), natsURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect to nats: %v\n", err)
		os.Exit(1)
	}
	defer nc.Close()

	streams := []string{
		events.StreamPipelineJobs,
		events.StreamAssetEvents,
		events.StreamPipelineEvents,
		events.StreamWebhookDelivery,
	}
	for _, name := range streams {
		stream, err := js.Stream(context.Background(), name)
		if err != nil {
			fmt.Fprintf(os.Stderr, "get stream %s: %v\n", name, err)
			os.Exit(1)
		}
		info, err := stream.Info(context.Background())
		if err != nil {
			fmt.Fprintf(os.Stderr, "info stream %s: %v\n", name, err)
			os.Exit(1)
		}
		if err := stream.Purge(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "purge %s: %v\n", name, err)
			os.Exit(1)
		}
		fmt.Printf("purged %-18s (had %d messages)\n", name, info.State.Msgs)
	}
	fmt.Println("all streams purged")
}
