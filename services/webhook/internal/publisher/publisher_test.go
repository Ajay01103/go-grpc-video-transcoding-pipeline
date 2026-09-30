package publisher

import (
	"testing"

	"github.com/Ajay01103/go-mux/webhook/internal/repository"
)

func TestWebhookPayloadSignature(t *testing.T) {
	first := repository.SignWebhookPayload(`{"type":"asset.ready"}`, "secret")
	second := repository.SignWebhookPayload(`{"type":"asset.ready"}`, "secret")
	if first == "" || first != second {
		t.Fatalf("signature is not deterministic: %q %q", first, second)
	}
	if first == repository.SignWebhookPayload(`{"type":"asset.ready"}`, "other-secret") {
		t.Fatal("different secrets produced the same signature")
	}
}
