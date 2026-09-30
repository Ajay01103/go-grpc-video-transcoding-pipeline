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

	"connectrpc.com/connect"
	"github.com/Ajay01103/go-mux/pkg/events"
	"github.com/Ajay01103/go-mux/pkg/interceptor"
	pkglogger "github.com/Ajay01103/go-mux/pkg/logger"
	"github.com/Ajay01103/go-mux/pkg/pipelinepb"
	"github.com/Ajay01103/go-mux/webhook/config"
	"github.com/Ajay01103/go-mux/webhook/db"
	"github.com/Ajay01103/go-mux/webhook/gen/pb"
	webhookconnect "github.com/Ajay01103/go-mux/webhook/gen/pb/pbconnect"
	"github.com/Ajay01103/go-mux/webhook/internal/publisher"
	"github.com/Ajay01103/go-mux/webhook/internal/repository"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
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
	logger := pkglogger.New()
	defer logger.Sync()

	undo := zap.ReplaceGlobals(logger)
	defer undo()

	logger.Info("WEBHOOK SERVICE starting")

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
		Datacenter:        cfg.ScyllaDatacenter,
		ReplicationFactor: cfg.ReplicationFactor,
	})
	cancel()
	if err != nil {
		return fmt.Errorf("connect to scylladb: %w", err)
	}
	defer session.Close()

	repo := repository.NewWebhookRepo(session)
	migrateCtx, migrateCancel := context.WithTimeout(context.Background(), 60*time.Second)
	if err := db.Migrate(migrateCtx, session); err != nil {
		migrateCancel()
		return fmt.Errorf("run migrations: %w", err)
	}
	migrateCancel()

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

	// v2: ConnectRPC/gRPC surface for subscription and delivery management.
	webhookHandler := &webhookRPCHandler{repo: repo}
	rpcPath, rpcHandler := webhookconnect.NewWebhookServiceHandler(webhookHandler, connect.WithInterceptors(interceptor.NewLoggingInterceptor(logger)))
	mux.Handle(rpcPath, rpcHandler)

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
		logger.Info("WEBHOOK SERVICE started at ConnectRPC server", zap.String("addr", addr))
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
			eventID := ""
			if message.Headers() != nil {
				eventID = message.Headers().Get(nats.MsgIdHdr)
			}
			if err := webhookPublisher.IngestPipelineEvent(ctx, orgUUID, eventType, event, eventID); err != nil {
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
				// Durable retry: requeue onto JetStream with a deterministic
				// Nats-Msg-Id. The delay itself is bounded by consumer BackOff,
				// and the retry survives a crash because it is a stream message.
				attempt.Attempt++
				payload, marshalErr := json.Marshal(attempt)
				if marshalErr == nil {
					if publishErr := events.NewPublisher(js).PublishBytes(ctx, events.SubjectWebhookDeliveryRetry, fmt.Sprintf("delivery:%s:retry:%d", attempt.DeliveryID, attempt.Attempt), payload); publishErr != nil {
						_ = message.Nak()
						continue
					}
				}
				_ = message.Ack()
				continue
			}
			_ = message.Ack()
		}
	}
}

type webhookRPCHandler struct {
	webhookconnect.UnimplementedWebhookServiceHandler
	repo *repository.WebhookRepo
}

func (h *webhookRPCHandler) CreateSubscription(ctx context.Context, req *connect.Request[pb.CreateSubscriptionRequest]) (*connect.Response[pb.CreateSubscriptionResponse], error) {
	orgID, err := uuid.Parse(req.Msg.GetOrgId())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if req.Msg.GetUrl() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("url is required"))
	}
	ep, err := h.repo.CreateEndpoint(ctx, orgID, req.Msg.GetUrl(), req.Msg.GetSecret(), req.Msg.GetEvents())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&pb.CreateSubscriptionResponse{Endpoint: toProtoEndpoint(ep)}), nil
}

func (h *webhookRPCHandler) DeleteSubscription(ctx context.Context, req *connect.Request[pb.DeleteSubscriptionRequest]) (*connect.Response[pb.DeleteSubscriptionResponse], error) {
	orgID, err := uuid.Parse(req.Msg.GetOrgId())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	endpointID, err := uuid.Parse(req.Msg.GetEndpointId())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := h.repo.DeleteEndpoint(ctx, orgID, endpointID); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&pb.DeleteSubscriptionResponse{Ok: true}), nil
}

func (h *webhookRPCHandler) ListSubscriptions(ctx context.Context, req *connect.Request[pb.ListSubscriptionsRequest]) (*connect.Response[pb.ListSubscriptionsResponse], error) {
	orgID, err := uuid.Parse(req.Msg.GetOrgId())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	endpoints, err := h.repo.GetEndpoints(ctx, orgID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	response := &pb.ListSubscriptionsResponse{Endpoints: make([]*pb.WebhookEndpoint, 0, len(endpoints))}
	for i := range endpoints {
		response.Endpoints = append(response.Endpoints, toProtoEndpoint(&endpoints[i]))
	}
	return connect.NewResponse(response), nil
}

func (h *webhookRPCHandler) GetDeliveryStatus(ctx context.Context, req *connect.Request[pb.GetDeliveryStatusRequest]) (*connect.Response[pb.GetDeliveryStatusResponse], error) {
	deliveryID, err := uuid.Parse(req.Msg.GetDeliveryId())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	delivery, err := h.repo.GetDeliveryByID(ctx, deliveryID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if delivery == nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("delivery not found"))
	}
	return connect.NewResponse(&pb.GetDeliveryStatusResponse{Delivery: &pb.DeliveryStatus{
		DeliveryId:  delivery.DeliveryID.String(),
		EndpointId:  delivery.EndpointID.String(),
		EventType:   delivery.EventType,
		StatusCode:  int32(delivery.StatusCode),
		Attempt:     int32(delivery.Attempt),
		DeliveredAt: delivery.DeliveredAt.Format(time.RFC3339Nano),
	}}), nil
}

func toProtoEndpoint(ep *repository.WebhookEndpoint) *pb.WebhookEndpoint {
	return &pb.WebhookEndpoint{
		OrgId:      ep.OrgID.String(),
		EndpointId: ep.EndpointID.String(),
		Url:        ep.URL,
		Events:     ep.Events,
		CreatedAt:  ep.CreatedAt.Format(time.RFC3339Nano),
	}
}
