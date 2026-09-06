package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/Ajay01103/go-notion/playback/config"
	"github.com/Ajay01103/go-notion/playback/db"
	"github.com/Ajay01103/go-notion/playback/gen/pb"
	"github.com/Ajay01103/go-notion/playback/gen/pb/pbconnect"
	"github.com/Ajay01103/go-notion/playback/internal/repository"
	"github.com/Ajay01103/go-notion/playback/internal/service"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type playbackHandler struct {
	pbconnect.UnimplementedPlaybackServiceHandler
	service *service.PlaybackService
}

func (h *playbackHandler) CreatePlayback(ctx context.Context, req *connect.Request[pb.CreatePlaybackRequest]) (*connect.Response[pb.CreatePlaybackResponse], error) {
	assetID, err := uuid.Parse(req.Msg.GetAssetId())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	record, err := h.service.CreatePlayback(ctx, assetID, req.Msg.GetPolicy())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&pb.CreatePlaybackResponse{Playback: &pb.PlaybackRecord{PlaybackId: record.PlaybackID.String(), AssetId: record.AssetID.String(), Policy: record.Policy, SigningKeyId: record.SigningKeyID, Revoked: record.Revoked, CreatedAt: record.CreatedAt.Format(time.RFC3339Nano)}}), nil
}

func (h *playbackHandler) ResolvePlayback(ctx context.Context, req *connect.Request[pb.ResolvePlaybackRequest]) (*connect.Response[pb.ResolvePlaybackResponse], error) {
	playbackID, err := uuid.Parse(req.Msg.GetPlaybackId())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	record, err := h.service.ResolvePlayback(ctx, playbackID)
	if err != nil || record == nil {
		if err == nil {
			err = fmt.Errorf("playback not found")
		}
		return nil, connect.NewError(connect.CodeNotFound, err)
	}
	return connect.NewResponse(&pb.ResolvePlaybackResponse{AssetId: record.AssetID.String(), Policy: record.Policy, Revoked: record.Revoked}), nil
}

func (h *playbackHandler) RevokePlayback(ctx context.Context, req *connect.Request[pb.RevokePlaybackRequest]) (*connect.Response[pb.RevokePlaybackResponse], error) {
	playbackID, err := uuid.Parse(req.Msg.GetPlaybackId())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := h.service.RevokePlayback(ctx, playbackID); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&pb.RevokePlaybackResponse{Ok: true}), nil
}

func (h *playbackHandler) IssueToken(ctx context.Context, req *connect.Request[pb.IssueTokenRequest]) (*connect.Response[pb.IssueTokenResponse], error) {
	playbackID, err := uuid.Parse(req.Msg.GetPlaybackId())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	token, err := h.service.IssueToken(ctx, playbackID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&pb.IssueTokenResponse{Token: token}), nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "playback service exited: %v\n", err)
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

	repo := repository.NewPlaybackRepo(session)
	if err := repo.EnsureSchema(); err != nil {
		return fmt.Errorf("ensure schema: %w", err)
	}

	playbackSvc := service.NewPlaybackService(repo, time.Duration(cfg.TokenTTL)*time.Second)
	mux := http.NewServeMux()
	path, rpcHandler := pbconnect.NewPlaybackServiceHandler(&playbackHandler{service: playbackSvc})
	mux.Handle(path, rpcHandler)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/playback", func(w http.ResponseWriter, r *http.Request) {
		assetIDStr := r.URL.Query().Get("asset_id")
		if assetIDStr == "" {
			http.Error(w, "asset_id is required", http.StatusBadRequest)
			return
		}
		assetID, err := uuid.Parse(assetIDStr)
		if err != nil {
			http.Error(w, "invalid asset_id", http.StatusBadRequest)
			return
		}
		rec, err := playbackSvc.CreatePlayback(r.Context(), assetID, "signed")
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(rec)
	})
	mux.HandleFunc("/playback/token", func(w http.ResponseWriter, r *http.Request) {
		playbackIDStr := r.URL.Query().Get("playback_id")
		if playbackIDStr == "" {
			http.Error(w, "playback_id is required", http.StatusBadRequest)
			return
		}
		playbackID, err := uuid.Parse(playbackIDStr)
		if err != nil {
			http.Error(w, "invalid playback_id", http.StatusBadRequest)
			return
		}
		ttl := 5 * time.Minute
		if rawTTL := r.URL.Query().Get("ttl_seconds"); rawTTL != "" {
			if seconds, err := strconv.Atoi(rawTTL); err == nil && seconds > 0 {
				ttl = time.Duration(seconds) * time.Second
			}
		}
		token, err := playbackSvc.IssueToken(r.Context(), playbackID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"token": token, "ttl_seconds": strconv.Itoa(int(ttl.Seconds()))})
	})

	// Phase 11: GetPlayerConfig endpoint
	mux.HandleFunc("/playback/config", func(w http.ResponseWriter, r *http.Request) {
		playbackIDStr := r.URL.Query().Get("playback_id")
		if playbackIDStr == "" {
			http.Error(w, "playback_id is required", http.StatusBadRequest)
			return
		}
		playbackID, err := uuid.Parse(playbackIDStr)
		if err != nil {
			http.Error(w, "invalid playback_id", http.StatusBadRequest)
			return
		}
		cdnURL := r.URL.Query().Get("cdn_url")
		if cdnURL == "" {
			cdnURL = "https://cdn.you.com"
		}

		langs := []string{}
		if rawLangs := r.URL.Query().Get("languages"); rawLangs != "" {
			for _, l := range strings.Split(rawLangs, ",") {
				if l != "" {
					langs = append(langs, strings.TrimSpace(l))
				}
			}
		}
		if len(langs) == 0 {
			langs = []string{"en"}
		}

		config, err := playbackSvc.GetPlayerConfig(r.Context(), playbackID, cdnURL, langs)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(config)
	})

	addr := fmt.Sprintf(":%s", cfg.HTTPPort)
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		logger.Info("playback service started", zap.String("addr", addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("listen and serve", zap.Error(err))
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	ctxShutdown, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	return srv.Shutdown(ctxShutdown)
}
