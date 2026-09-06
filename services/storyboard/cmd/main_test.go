package main

import (
	"strings"
	"testing"
)

func TestStoryboardVTTUsesSpriteFragments(t *testing.T) {
	vtt := storyboardVTT(11, 5, 1280, 680)
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
