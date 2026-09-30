package main

import (
	"strings"
	"testing"
)

func TestStoryboardVTTUsesSpriteFragments(t *testing.T) {
	vtt := storyboardVTT(11, 5)
	if !strings.HasPrefix(vtt, "WEBVTT\n\n") {
		t.Fatalf("missing WebVTT header: %q", vtt)
	}
	if !strings.Contains(vtt, "00:00:00.000 --> 00:00:05.000") {
		t.Fatalf("missing first cue: %q", vtt)
	}
	if !strings.Contains(vtt, "storyboard.jpg#xywh=0,0,160,90") {
		t.Fatalf("missing first sprite fragment: %q", vtt)
	}
	if !strings.Contains(vtt, "storyboard.jpg#xywh=160,0,160,90") {
		t.Fatalf("missing second sprite fragment: %q", vtt)
	}
}

func TestStoryboardVTTCueCountMatchesDuration(t *testing.T) {
	// 45s at 5s interval = 9 cues; cue 9 wraps to the second sprite row.
	vtt := storyboardVTT(45, 5)
	if !strings.Contains(vtt, "storyboard.jpg#xywh=0,90,160,90") {
		t.Fatalf("missing second-row fragment for 9th tile: %q", vtt)
	}
}

func TestStoryboardVTTCapsAtDuration(t *testing.T) {
	vtt := storyboardVTT(10, 5)
	cueCount := strings.Count(vtt, "-->")
	if cueCount != 2 {
		t.Fatalf("expected 2 cues for a 10s video at 5s interval, got %d", cueCount)
	}
}
