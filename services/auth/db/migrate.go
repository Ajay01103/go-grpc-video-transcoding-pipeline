package db

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"strings"

	"github.com/Ajay01103/go-mux/pkg/dbmigrate"
	"github.com/gocql/gocql"
)

//go:embed migrations/*.cql
var migrationFS embed.FS

const ServiceName = "auth"

// Migrate applies all pending auth-service migrations via the shared
// pkg/dbmigrate runner. Idempotent.
//
// History: auth previously used gocqlx's migrate.FromFS, which tracks applied
// migrations in the gocqlx_migrate table (keyed by filename). To switch
// runners without re-applying old migrations against existing deployments,
// Migrate first backfills service_migrations from any gocqlx rows whose
// filename maps to an embedded migration version. Fresh databases skip the
// backfill entirely (no gocqlx table) and behave like every other service.
func Migrate(ctx context.Context, session *gocql.Session) error {
	if err := dbmigrate.EnsureTrackingTable(ctx, session); err != nil {
		return fmt.Errorf("ensure migration tracking: %w", err)
	}
	if err := backfillFromGocqlx(ctx, session); err != nil {
		return fmt.Errorf("backfill gocqlx migrations: %w", err)
	}
	applied, err := dbmigrate.Run(ctx, session, ServiceName, "migrations", migrationFS)
	if err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	if applied > 0 {
		fmt.Printf("auth db: applied %d migration(s)\n", applied)
	}
	return nil
}

// backfillFromGocqlx marks legacy gocqlx-managed migrations as applied in
// service_migrations. Rows are matched by filename ("N_description.cql" from
// gocqlx_migrate) to the embedded migration version. Missing/foreign rows are
// ignored. Safe to call on every startup.
func backfillFromGocqlx(ctx context.Context, session *gocql.Session) error {
	// Only when the legacy tracking table exists (i.e. this database was
	// migrated by the old runner before).
	var tableName string
	err := session.Query(
		`SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = 'gocqlx_migrate'`,
		Keyspace,
	).WithContext(ctx).Scan(&tableName)
	if err != nil {
		if err == gocql.ErrNotFound {
			return nil // fresh database, nothing to backfill
		}
		return fmt.Errorf("check gocqlx_migrate table: %w", err)
	}

	// Version -> embedded checksum, so backfilled rows carry the same checksum
	// dbmigrate would compute, keeping its duplicate-content guard consistent.
	checksums, err := embeddedChecksums("migrations", migrationFS)
	if err != nil {
		return err
	}

	applied, err := dbmigrate.LoadApplied(ctx, session, ServiceName)
	if err != nil {
		return err
	}

	iter := session.Query(`SELECT name, done FROM gocqlx_migrate`).WithContext(ctx).Iter()
	var name string
	var done int
	for iter.Scan(&name, &done) {
		// gocqlx tracks completion with a counter that increments on every
		// migration callback, so any positive value means "applied". Only
		// done == 0 (registered but never completed) is skipped.
		if done <= 0 {
			continue
		}
		version, ok := versionOfFilename(name)
		if !ok {
			continue // not one of our embedded migrations; leave it alone
		}
		if _, already := applied[version]; already {
			continue
		}
		checksum := checksums[version]
		if err := session.Query(
			`INSERT INTO service_migrations (service, version, checksum, name, applied_at) VALUES (?, ?, ?, ?, toTimestamp(now()))`,
			ServiceName, version, checksum, name,
		).WithContext(ctx).Exec(); err != nil {
			iter.Close()
			return fmt.Errorf("backfill migration %s: %w", name, err)
		}
		applied[version] = checksum
	}
	return iter.Close()
}

// versionOfFilename parses "1_initial_schema.cql" -> 1 (also accepts the
// no-extension form gocqlx stores).
func versionOfFilename(name string) (int, bool) {
	name = strings.TrimSuffix(name, ".cql")
	var version int
	var rest string
	if _, err := fmt.Sscanf(name, "%d_%s", &version, &rest); err != nil {
		return 0, false
	}
	return version, true
}

// embeddedChecksums computes the dbmigrate checksum for every embedded
// migration file, keyed by version.
func embeddedChecksums(dir string, fsys fs.FS) (map[int]string, error) {
	checksums := make(map[int]string)
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, fmt.Errorf("read migration dir %s: %w", dir, err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".cql") {
			continue
		}
		version, ok := versionOfFilename(entry.Name())
		if !ok {
			continue
		}
		content, err := fs.ReadFile(fsys, dir+"/"+entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", entry.Name(), err)
		}
		checksums[version] = dbmigrate.ChecksumOf(content)
	}
	return checksums, nil
}
