package main

import (
	"strings"
	"testing"
)

func TestFFmpegArgsBuildScaledLadder(t *testing.T) {
	args := ffmpegArgs("source.mp4", "output")
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
