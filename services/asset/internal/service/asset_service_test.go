package service

import (
	"context"
	"testing"
	"time"
)

func TestReconcileRequiresPositiveAge(t *testing.T) {
	service := &AssetService{}
	if err := service.ReconcileProcessingAssets(context.Background(), 0); err == nil {
		t.Fatal("expected invalid reconciliation age error")
	}
	if err := service.ReconcileProcessingAssets(context.Background(), -time.Second); err == nil {
		t.Fatal("expected negative reconciliation age error")
	}
}
