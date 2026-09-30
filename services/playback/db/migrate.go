package db

import (
	"context"
	"embed"
	"fmt"

	"github.com/Ajay01103/go-mux/pkg/dbmigrate"
	"github.com/gocql/gocql"
)

//go:embed migrations/*.cql
var migrationFS embed.FS

const ServiceName = "playback"

// Migrate applies all pending playback-service migrations. Idempotent.
func Migrate(ctx context.Context, session *gocql.Session) error {
	if err := dbmigrate.EnsureTrackingTable(ctx, session); err != nil {
		return fmt.Errorf("ensure migration tracking: %w", err)
	}
	applied, err := dbmigrate.Run(ctx, session, ServiceName, "migrations", migrationFS)
	if err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	if applied > 0 {
		fmt.Printf("playback db: applied %d migration(s)\n", applied)
	}
	return nil
}
