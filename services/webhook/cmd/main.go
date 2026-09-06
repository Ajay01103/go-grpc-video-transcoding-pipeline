package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Ajay01103/go-notion/pkg/events"
	"github.com/Ajay01103/go-notion/pkg/pipelinepb"
	"github.com/Ajay01103/go-notion/webhook/config"
	"github.com/Ajay01103/go-notion/webhook/db"
	"github.com/Ajay01103/go-notion/webhook/internal/publisher"
	"github.com/Ajay01103/go-notion/webhook/internal/repository"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "webhook service exited: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	session, err := db.Connect(ctx, db.Config{
		Hosts:             cfg.ScyllaHosts,
		Port:              cfg.ScyllaPort,
		Username:          cfg.ScyllaUsername,
		Password:          cfg.ScyllaPassword,
		Consistency:       0,
		Datacenter:        cfg.ScyllaDatacenter,
		ReplicationFactor: cfg.ReplicationFactor,
	})
	cancel()
	if err != nil {
		return fmt.Errorf("connect to scylladb: %w", err)
	}
	defer session.Close()

	repo := repository.NewWebhookRepo(session)
	if err := repo.EnsureSchema(); err != nil {
		return fmt.Errorf("ensure schema: %w", err)
	}

	js, nc, err := events.Connect(context.Background(), cfg.NATSURL)
	if err != nil {
		return fmt.Errorf("connect to nats: %w", err)
	}
	defer nc.Drain()
	if err := events.EnsureStreams(context.Background(), js); err != nil {
		return fmt.Errorf("ensure streams: %w", err)
	}
	if err := events.EnsureTerminalConsumers(context.Background(), js); err != nil {
		return fmt.Errorf("ensure terminal consumers: %w", err)
	}
	if err := events.EnsureWebhookDeliveryConsumer(context.Background(), js); err != nil {
		return fmt.Errorf("ensure delivery consumer: %w", err)
	}
	webhookPublisher := publisher.NewWebhookPublisher(repo, events.NewPublisher(js))
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/webhook/subscribe", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		var reqBody struct {
			OrgID  string   `json:"org_id"`
			URL    string   `json:"url"`
			Secret string   `json:"secret"`
			Events []string `json:"events"`
		}
		if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		orgID, err := uuid.Parse(reqBody.OrgID)
		if err != nil {
			http.Error(w, "invalid org_id", http.StatusBadRequest)
			return
		}

		ep, err := repo.CreateEndpoint(r.Context(), orgID, reqBody.URL, reqBody.Secret, reqBody.Events)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ep)
	})

	mux.HandleFunc("/webhook/publish", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		var reqBody struct {
			OrgID string      `json:"org_id"`
			Type  string      `json:"type"`
			Data  interface{} `json:"data"`
		}
		if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		orgID, err := uuid.Parse(reqBody.OrgID)
		if err != nil {
			http.Error(w, "invalid org_id", http.StatusBadRequest)
			return
		}

		if err := webhookPublisher.PublishEvent(r.Context(), orgID, reqBody.Type, reqBody.Data); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	addr := fmt.Sprintf(":%s", cfg.HTTPPort)
	srv := &http.Server{Addr: addr, Handler: mux}
	consumerCtx, stopConsumer := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopConsumer()
	go consumeTerminalEvents(consumerCtx, js, webhookPublisher)
	go consumeDeliveryAttempts(consumerCtx, js, webhookPublisher)
	go func() {
		logger.Info("webhook service started", zap.String("addr", addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("listen and serve", zap.Error(err))
			os.Exit(1)
		}
	}()

	<-consumerCtx.Done()
	ctxShutdown, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	return srv.Shutdown(ctxShutdown)
}

func consumeTerminalEvents(ctx context.Context, js jetstream.JetStream, webhookPublisher *publisher.WebhookPublisher) {
	consumer, err := js.Consumer(ctx, events.StreamPipelineEvents, events.ConsumerWebhookDelivery)
	if err != nil {
		return
	}
	for ctx.Err() == nil {
		batch, fetchErr := consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
		if fetchErr != nil {
			continue
		}
		for message := range batch.Messages() {
			var orgID string
			var eventType string
			var event proto.Message
			if message.Subject() == events.SubjectPipelineRunCompleted {
				completed := new(pipelinepb.PipelineRunCompleted)
				if err := proto.Unmarshal(message.Data(), completed); err != nil {
					_ = message.Nak()
					continue
				}
				orgID, eventType, event = completed.GetOrgId(), "asset.ready", completed
			} else {
				failed := new(pipelinepb.PipelineRunFailed)
				if err := proto.Unmarshal(message.Data(), failed); err != nil {
					_ = message.Nak()
					continue
				}
				orgID, eventType, event = failed.GetOrgId(), "asset.errored", failed
			}
			orgUUID, parseErr := uuid.Parse(orgID)
			if parseErr != nil {
				_ = message.Term()
				continue
			}
			if err := webhookPublisher.IngestPipelineEvent(ctx, orgUUID, eventType, event); err != nil {
				_ = message.Nak()
				continue
			}
			_ = message.Ack()
		}
	}
}

func consumeDeliveryAttempts(ctx context.Context, js jetstream.JetStream, webhookPublisher *publisher.WebhookPublisher) {
	consumer, err := js.Consumer(ctx, events.StreamWebhookDelivery, events.ConsumerWebhookAttempts)
	if err != nil {
		return
	}
	for ctx.Err() == nil {
		batch, fetchErr := consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
		if fetchErr != nil {
			continue
		}
		for message := range batch.Messages() {
			var attempt publisher.DeliveryAttempt
			if err := json.Unmarshal(message.Data(), &attempt); err != nil {
				_ = message.Term()
				continue
			}
			if err := webhookPublisher.DeliverAttempt(ctx, attempt); err != nil {
				if attempt.Attempt >= 3 {
					_ = message.Term()
					continue
				}
				attempt.Attempt++
				payload, marshalErr := json.Marshal(attempt)
				if marshalErr == nil {
					_ = events.NewPublisher(js).PublishBytes(ctx, events.SubjectWebhookDeliveryRetry, fmt.Sprintf("delivery:%s:retry:%d", attempt.DeliveryID, attempt.Attempt), payload)
				}
				_ = message.Ack()
				continue
			}
			_ = message.Ack()
		}
	}
}
