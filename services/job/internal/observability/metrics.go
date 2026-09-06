package observability

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

type Metrics struct {
	mu sync.RWMutex

	// Pipeline metrics
	QueueDepth          int64
	TranscodeDuration   float64
	ThumbnailDuration   float64
	StoryboardDuration  float64
	SubtitleDuration    float64
	FFMpegFailures      int64
	PipelineStepFailures int64

	// Idempotency
	DuplicateRequests int64
}

type MetricsCollector struct {
	metrics *Metrics
	logger  *zap.Logger
}

func NewMetricsCollector(logger *zap.Logger) *MetricsCollector {
	return &MetricsCollector{
		metrics: &Metrics{},
		logger:  logger,
	}
}

func (mc *MetricsCollector) RecordTranscodeDuration(ctx context.Context, duration time.Duration) {
	mc.metrics.mu.Lock()
	defer mc.metrics.mu.Unlock()
	mc.metrics.TranscodeDuration = duration.Seconds()
	mc.logger.Info("transcode completed", zap.Float64("duration_seconds", mc.metrics.TranscodeDuration))
}

func (mc *MetricsCollector) RecordThumbnailDuration(ctx context.Context, duration time.Duration) {
	mc.metrics.mu.Lock()
	defer mc.metrics.mu.Unlock()
	mc.metrics.ThumbnailDuration = duration.Seconds()
	mc.logger.Info("thumbnail generated", zap.Float64("duration_seconds", mc.metrics.ThumbnailDuration))
}

func (mc *MetricsCollector) RecordStoryboardDuration(ctx context.Context, duration time.Duration) {
	mc.metrics.mu.Lock()
	defer mc.metrics.mu.Unlock()
	mc.metrics.StoryboardDuration = duration.Seconds()
	mc.logger.Info("storyboard generated", zap.Float64("duration_seconds", mc.metrics.StoryboardDuration))
}

func (mc *MetricsCollector) RecordFFMpegFailure(ctx context.Context, err error) {
	mc.metrics.mu.Lock()
	defer mc.metrics.mu.Unlock()
	mc.metrics.FFMpegFailures++
	mc.logger.Error("ffmpeg failure", zap.Int64("total_failures", mc.metrics.FFMpegFailures), zap.Error(err))
}

func (mc *MetricsCollector) RecordPipelineStepFailure(ctx context.Context, stepType string, err error) {
	mc.metrics.mu.Lock()
	defer mc.metrics.mu.Unlock()
	mc.metrics.PipelineStepFailures++
	mc.logger.Error("pipeline step failed", zap.String("step", stepType), zap.Error(err))
}

func (mc *MetricsCollector) RecordDuplicateRequest(ctx context.Context, idempotencyKey string) {
	mc.metrics.mu.Lock()
	defer mc.metrics.mu.Unlock()
	mc.metrics.DuplicateRequests++
	mc.logger.Info("duplicate request detected", zap.String("idempotency_key", idempotencyKey))
}

func (mc *MetricsCollector) GetMetrics() *Metrics {
	mc.metrics.mu.RLock()
	defer mc.metrics.mu.RUnlock()
	m := &Metrics{
		QueueDepth:           mc.metrics.QueueDepth,
		TranscodeDuration:    mc.metrics.TranscodeDuration,
		ThumbnailDuration:    mc.metrics.ThumbnailDuration,
		StoryboardDuration:   mc.metrics.StoryboardDuration,
		SubtitleDuration:     mc.metrics.SubtitleDuration,
		FFMpegFailures:       mc.metrics.FFMpegFailures,
		PipelineStepFailures: mc.metrics.PipelineStepFailures,
		DuplicateRequests:    mc.metrics.DuplicateRequests,
	}
	return m
}
