package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/Ajay01103/go-notion/asset/config"
	"github.com/Ajay01103/go-notion/asset/db"
	"github.com/Ajay01103/go-notion/asset/gen/pb"
	"github.com/Ajay01103/go-notion/asset/gen/pb/pbconnect"
	"github.com/Ajay01103/go-notion/asset/internal/repository"
	"github.com/Ajay01103/go-notion/asset/internal/service"
	"github.com/Ajay01103/go-notion/pkg/events"
	"github.com/Ajay01103/go-notion/pkg/pipelinepb"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

type assetHandler struct {
	pbconnect.UnimplementedAssetServiceHandler
	service *service.AssetService
	config  config.Config
}

func (h *assetHandler) CreateAsset(ctx context.Context, req *connect.Request[pb.CreateAssetRequest]) (*connect.Response[pb.CreateAssetResponse], error) {
	orgID, err := uuid.Parse(req.Msg.GetOrgId())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	asset, err := h.service.CreateAsset(ctx, orgID, req.Msg.GetTitle())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewResponse(&pb.CreateAssetResponse{Asset: toProtoAsset(asset)}), nil
}

func (h *assetHandler) GetAsset(ctx context.Context, req *connect.Request[pb.GetAssetRequest]) (*connect.Response[pb.GetAssetResponse], error) {
	assetID, err := uuid.Parse(req.Msg.GetAssetId())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	asset, err := h.service.GetAsset(ctx, assetID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if asset == nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("asset not found"))
	}
	return connect.NewResponse(&pb.GetAssetResponse{Asset: toProtoAsset(asset)}), nil
}

func (h *assetHandler) ListAssets(ctx context.Context, req *connect.Request[pb.ListAssetsRequest]) (*connect.Response[pb.ListAssetsResponse], error) {
	orgID, err := uuid.Parse(req.Msg.GetOrgId())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	assets, err := h.service.ListAssets(ctx, orgID, int(req.Msg.GetLimit()))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	response := &pb.ListAssetsResponse{Assets: make([]*pb.Asset, 0, len(assets))}
	for index := range assets {
		response.Assets = append(response.Assets, toProtoAsset(&assets[index]))
	}
	return connect.NewResponse(response), nil
}

func (h *assetHandler) CreateUploadURL(ctx context.Context, req *connect.Request[pb.CreateUploadURLRequest]) (*connect.Response[pb.CreateUploadURLResponse], error) {
	assetID, err := uuid.Parse(req.Msg.GetAssetId())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	upload, err := h.service.CreateUploadURL(ctx, assetID, h.config.RustFSURL, h.config.RustFSBucket, h.config.UploadURLTTL)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewResponse(&pb.CreateUploadURLResponse{
		AssetId:       upload.AssetID.String(),
		UploadUrl:     upload.UploadURL,
		Bucket:        upload.Bucket,
		ObjectKey:     upload.ObjectKey,
		ExpiresAtUnix: upload.ExpiresAt.Unix(),
	}), nil
}

func (h *assetHandler) CompleteUpload(ctx context.Context, req *connect.Request[pb.CompleteUploadRequest]) (*connect.Response[pb.CompleteUploadResponse], error) {
	assetID, err := uuid.Parse(req.Msg.GetAssetId())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := h.service.CompleteUpload(ctx, assetID, req.Msg.GetSourceUri()); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&pb.CompleteUploadResponse{Ok: true}), nil
}

func toProtoAsset(asset *repository.Asset) *pb.Asset {
	if asset == nil {
		return nil
	}
	return &pb.Asset{
		AssetId:    asset.ID.String(),
		OrgId:      asset.OrgID.String(),
		Title:      asset.Title,
		Status:     asset.Status,
		SourceUri:  asset.SourceURI,
		DurationMs: asset.DurationMs,
		CreatedAt:  asset.CreatedAt.Format(time.RFC3339Nano),
		UpdatedAt:  asset.UpdatedAt.Format(time.RFC3339Nano),
	}
}

func consumePipelineEvents(ctx context.Context, js jetstream.JetStream, repo *repository.AssetRepo) {
	consumer, err := js.Consumer(ctx, events.StreamPipelineEvents, events.ConsumerAssetStatusUpdater)
	if err != nil {
		return
	}
	for {
		batch, err := consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		for message := range batch.Messages() {
			var event pipelinepb.PipelineRunCompleted
			if message.Subject() == events.SubjectPipelineRunFailed {
				var failed pipelinepb.PipelineRunFailed
				if err := proto.Unmarshal(message.Data(), &failed); err == nil {
					if assetID, parseErr := uuid.Parse(failed.GetAssetId()); parseErr == nil {
						_ = repo.UpdateStatus(ctx, assetID, "ERRORED")
					}
				}
				_ = message.Ack()
				continue
			}
			if err := proto.Unmarshal(message.Data(), &event); err == nil {
				if assetID, parseErr := uuid.Parse(event.GetAssetId()); parseErr == nil {
					_ = repo.UpdateStatus(ctx, assetID, "READY")
				}
			}
			_ = message.Ack()
		}
	}
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "asset service exited: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	session, err := db.Connect(ctx, db.Config{
		Hosts: cfg.ScyllaHosts, Port: cfg.ScyllaPort, Username: cfg.ScyllaUsername,
		Password: cfg.ScyllaPassword, Datacenter: cfg.ScyllaDatacenter,
		ReplicationFactor: cfg.ReplicationFactor,
	})
	cancel()
	if err != nil {
		return fmt.Errorf("connect to scylladb: %w", err)
	}
	defer session.Close()
	repo := repository.NewAssetRepo(session)
	if err := repo.EnsureSchema(); err != nil {
		return fmt.Errorf("ensure schema: %w", err)
	}
	js, nc, err := events.Connect(context.Background(), cfg.NATSURL)
	if err != nil {
		return fmt.Errorf("connect to nats: %w", err)
	}
	defer nc.Drain()
	if err := events.EnsureStreams(context.Background(), js); err != nil {
		return fmt.Errorf("ensure event streams: %w", err)
	}
	if err := events.EnsureTerminalConsumers(context.Background(), js); err != nil {
		return fmt.Errorf("ensure terminal consumers: %w", err)
	}
	service := service.NewAssetService(repo, events.NewPublisher(js))
	handler := &assetHandler{service: service, config: cfg}
	path, rpcHandler := pbconnect.NewAssetServiceHandler(handler)
	mux := http.NewServeMux()
	mux.Handle(path, rpcHandler)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	server := &http.Server{Addr: ":" + cfg.HTTPPort, Handler: mux}
	consumerCtx, stopConsumer := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopConsumer()
	go consumePipelineEvents(consumerCtx, js, repo)
	go reconcileAssets(consumerCtx, service, cfg.ReconcileInterval, cfg.ReconcileAge)
	go func() {
		if serveErr := server.ListenAndServe(); serveErr != nil && serveErr != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "asset HTTP server exited: %v\n", serveErr)
			stopConsumer()
		}
	}()
	<-consumerCtx.Done()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	return server.Shutdown(shutdownCtx)
}

func reconcileAssets(ctx context.Context, assetService *service.AssetService, interval, age time.Duration) {
	if interval <= 0 || age <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = assetService.ReconcileProcessingAssets(ctx, age)
		}
	}
}
