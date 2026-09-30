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
	"github.com/Ajay01103/go-mux/pkg/interceptor"
	pkglogger "github.com/Ajay01103/go-mux/pkg/logger"
	"github.com/Ajay01103/go-mux/playback/config"
	"github.com/Ajay01103/go-mux/playback/db"
	"github.com/Ajay01103/go-mux/playback/gen/pb"
	"github.com/Ajay01103/go-mux/playback/gen/pb/pbconnect"
	"github.com/Ajay01103/go-mux/playback/internal/repository"
	service "github.com/Ajay01103/go-mux/playback/internal/service"
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
	record, err := h.service.CreatePlayback(ctx, assetID, req.Msg.GetPolicy(), req.Msg.GetArtifactPrefix())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&pb.CreatePlaybackResponse{Playback: &pb.PlaybackRecord{PlaybackId: record.PlaybackID.String(), AssetId: record.AssetID.String(), Policy: record.Policy, ArtifactPrefix: record.ArtifactPrefix, SigningKeyId: record.SigningKeyID, Revoked: record.Revoked, CreatedAt: record.CreatedAt.Format(time.RFC3339Nano)}}), nil
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

func (h *playbackHandler) GetPlayerConfig(ctx context.Context, req *connect.Request[pb.GetPlayerConfigRequest]) (*connect.Response[pb.GetPlayerConfigResponse], error) {
	playbackID, err := uuid.Parse(req.Msg.GetPlaybackId())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	config, err := h.service.GetPlayerConfig(ctx, playbackID, req.Msg.GetCdnUrl(), req.Msg.GetLanguages())
	if err != nil {
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "revoked") {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	subtitles := make([]*pb.PlayerSubtitleTrack, 0, len(config.Subtitles))
	for _, track := range config.Subtitles {
		subtitles = append(subtitles, &pb.PlayerSubtitleTrack{Url: track.URL, Lang: track.Lang, Label: track.Label, Auto: track.Auto})
	}
	return connect.NewResponse(&pb.GetPlayerConfigResponse{
		Src:           config.Src,
		Poster:        config.Poster,
		StoryboardSrc: config.StoryboardSrc,
		Subtitles:     subtitles,
		AssetId:       config.AssetID,
		PlaybackId:    config.PlaybackID,
	}), nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "playback service exited: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	logger := pkglogger.New()
	defer logger.Sync()

	undo := zap.ReplaceGlobals(logger)
	defer undo()

	logger.Info("PLAYBACK SERVICE starting")

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

	ctxMigrate, cancelMigrate := context.WithTimeout(context.Background(), 30*time.Second)
	if err := db.Migrate(ctxMigrate, session); err != nil {
		cancelMigrate()
		return fmt.Errorf("run migrations: %w", err)
	}
	cancelMigrate()

	repo := repository.NewPlaybackRepo(session)
	ctxSigner, cancelSigner := context.WithTimeout(context.Background(), 30*time.Second)
	playbackSigner, err := service.NewTokenSigner(ctxSigner, session)
	cancelSigner()
	if err != nil {
		return fmt.Errorf("create playback token signer: %w", err)
	}

	playbackSvc := service.NewPlaybackService(repo, playbackSigner, time.Duration(cfg.TokenTTL)*time.Second)
	mux := http.NewServeMux()
	path, rpcHandler := pbconnect.NewPlaybackServiceHandler(&playbackHandler{service: playbackSvc}, connect.WithInterceptors(interceptor.NewLoggingInterceptor(logger)))
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
		rec, err := playbackSvc.CreatePlayback(r.Context(), assetID, "signed", r.URL.Query().Get("artifact_prefix"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(rec)
	})

	// JWKS endpoint so the CDN edge can verify playback tokens locally.
	mux.HandleFunc("/playback/jwks.json", func(w http.ResponseWriter, r *http.Request) {
		jwks, err := playbackSigner.PublicKeyJWKs(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=300")
		_, _ = w.Write(jwks)
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
		// Empty cdn_url = direct public RustFS URLs (default dev mode).
		cdnURL := r.URL.Query().Get("cdn_url")

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
		logger.Info("PLAYBACK SERVICE started at ConnectRPC server", zap.String("addr", addr))
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
