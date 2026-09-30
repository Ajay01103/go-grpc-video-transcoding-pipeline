package orchestrator

import (
	"testing"

	"github.com/Ajay01103/go-mux/pkg/events"
	"github.com/google/uuid"
)

func TestOutputPrefixAndRequestID(t *testing.T) {
	assetID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	runID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	if got, want := OutputPrefix(assetID, runID), "assets/11111111-1111-1111-1111-111111111111/runs/22222222-2222-2222-2222-222222222222"; got != want {
		t.Fatalf("output prefix = %q, want %q", got, want)
	}
	if got, want := events.RequestedMessageID(runID.String(), "subtitle:en", 1), "run:22222222-2222-2222-2222-222222222222:step:subtitle:en:attempt:1"; got != want {
		t.Fatalf("subtitle request ID = %q, want %q", got, want)
	}
}
