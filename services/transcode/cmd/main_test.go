package main

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Ajay01103/go-mux/pkg/pipelinepb"
	"github.com/Ajay01103/go-mux/pkg/storage"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestFFmpegArgsBuildScaledLadder(t *testing.T) {
	args := ffmpegArgs("source.mp4", "output", ladder)
	joined := strings.Join(args, " ")
	for index, item := range ladder {
		if !strings.Contains(joined, "[vsrc"+string(rune('0'+index))+"]scale=w=") {
			t.Fatalf("rendition %s has no scale filter: %s", item.name, joined)
		}
		if !strings.Contains(joined, "-map [v"+string(rune('0'+index))+"]") {
			t.Fatalf("rendition %s has no mapped video stream: %s", item.name, joined)
		}
		if strings.Contains(joined, item.bitrate+"k") {
			t.Fatalf("rendition %s contains a duplicated bitrate suffix: %s", item.name, joined)
		}
	}
	if !strings.Contains(joined, "-var_stream_map v:0,a:0 v:1,a:1 v:2,a:2 v:3,a:3") {
		t.Fatalf("ladder stream map is incomplete: %s", joined)
	}
}

// A 720p source must drop the 1080p rung: upscaling costs the most CPU and
// adds no quality.
func TestLadderForCapsAtSourceHeight(t *testing.T) {
	rungs := ladderFor(720)
	if len(rungs) != 3 {
		t.Fatalf("720p source should get 3 rungs, got %d: %+v", len(rungs), rungs)
	}
	for _, rung := range rungs {
		if rung.height > 720 {
			t.Fatalf("rung %s upscales past source height 720", rung.name)
		}
	}

	if got := ladderFor(1080); len(got) != 4 {
		t.Fatalf("1080p source should keep all 4 rungs, got %d", len(got))
	}

	// Tiny/odd sources still get one rung so a master playlist exists.
	tiny := ladderFor(144)
	if len(tiny) != 1 || tiny[0].name != "360p" {
		t.Fatalf("tiny source should fall back to smallest rung, got %+v", tiny)
	}
}

func TestFFmpegArgsUsePreset(t *testing.T) {
	args := ffmpegArgs("source.mp4", "output", ladderFor(720))
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "-preset "+transcodePreset) {
		t.Fatalf("args missing -preset %s: %s", transcodePreset, joined)
	}
	if strings.Contains(joined, "1920") {
		t.Fatalf("720p ladder should not contain the 1080p width: %s", joined)
	}
}

type testMsgAcker struct {
	mu              sync.Mutex
	inProgressCount int
}

func (m *testMsgAcker) Metadata() (*jetstream.MsgMetadata, error) { return nil, nil }
func (m *testMsgAcker) Nak() error                                { return nil }
func (m *testMsgAcker) Term() error                               { return nil }
func (m *testMsgAcker) Ack() error                                { return nil }
func (m *testMsgAcker) Data() []byte                              { return nil }
func (m *testMsgAcker) Subject() string                           { return "" }
func (m *testMsgAcker) Headers() nats.Header                      { return nil }
func (m *testMsgAcker) InProgress() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inProgressCount++
	return nil
}

func (m *testMsgAcker) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.inProgressCount
}

func TestTranscodeHeartbeatNoLeak(t *testing.T) {
	orig := heartbeatInterval
	heartbeatInterval = 10 * time.Millisecond
	defer func() { heartbeatInterval = orig }()

	worker := &transcodeWorker{
		store:  storage.New(storage.Config{Endpoint: "http://127.0.0.1:9999"}),
		bucket: "test-bucket",
	}
	mockMsg := &testMsgAcker{}
	req := &pipelinepb.TranscodeRequested{
		SourceUri:    "s3://bucket/video.mp4",
		OutputPrefix: "test/out",
	}

	var renditions int
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(35*time.Millisecond, cancel)
	_ = worker.transcode(ctx, mockMsg, req, &renditions)

	countAtReturn := mockMsg.count()
	time.Sleep(100 * time.Millisecond)
	countAfter100ms := mockMsg.count()

	if countAfter100ms > countAtReturn {
		t.Fatalf("heartbeat goroutine leaked: InProgress called after return (was %d, now %d)",
			countAtReturn, countAfter100ms)
	}
}

