// Package dbmigrate runs embedded .cql migrations for a service against a
// ScyllaDB session. Migrations are tracked per service in the shared keyspace
// via the service_migrations table, so multiple services can each own their
// own numbered migration files without colliding (auth keeps its own
// gocqlx-managed table).
package dbmigrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"

	"github.com/gocql/gocql"
)

// EnsureTrackingTable creates the shared migration tracking table if missing.
func EnsureTrackingTable(ctx context.Context, session *gocql.Session) error {
	return session.Query(`
		CREATE TABLE IF NOT EXISTS service_migrations (
			service text,
			version int,
			checksum text,
			name text,
			applied_at timestamp,
			PRIMARY KEY (service, version)
		)`).WithContext(ctx).Exec()
}

// Run applies every embedded migration in dir (files named NNN_desc.cql) that
// has not been applied yet for the given service. It is idempotent and safe to
// call on every startup. Returns the number of newly applied migrations.
func Run(ctx context.Context, session *gocql.Session, service, dir string, migrationFS fs.FS) (int, error) {
	entries, err := fs.ReadDir(migrationFS, dir)
	if err != nil {
		return 0, fmt.Errorf("read migration dir %s: %w", dir, err)
	}

	var files []fs.DirEntry
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".cql") {
			files = append(files, entry)
		}
	}
	if len(files) == 0 {
		return 0, fmt.Errorf("no .cql migrations found in %s", dir)
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].Name() < files[j].Name()
	})

	applied, err := loadApplied(ctx, session, service)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, file := range files {
		version, name, err := parseFilename(file.Name())
		if err != nil {
			return count, err
		}
		if _, ok := applied[version]; ok {
			continue
		}

		content, err := fs.ReadFile(migrationFS, path.Join(dir, file.Name()))
		if err != nil {
			return count, fmt.Errorf("read migration %s: %w", file.Name(), err)
		}
		checksum := checksumOf(content)
		if previous, ok := appliedByChecksum(applied, checksum); ok && previous != version {
			return count, fmt.Errorf("migration %s duplicates content of version %d", file.Name(), previous)
		}

		for _, stmt := range splitStatements(string(content)) {
			if err := session.Query(stmt).WithContext(ctx).Exec(); err != nil {
				return count, fmt.Errorf("apply migration %s: %w", file.Name(), err)
			}
		}

		if err := session.Query(
			`INSERT INTO service_migrations (service, version, checksum, name, applied_at) VALUES (?, ?, ?, ?, toTimestamp(now()))`,
			service, version, checksum, name,
		).WithContext(ctx).Exec(); err != nil {
			return count, fmt.Errorf("record migration %s: %w", file.Name(), err)
		}
		applied[version] = checksum
		count++
	}
	return count, nil
}

func loadApplied(ctx context.Context, session *gocql.Session, service string) (map[int]string, error) {
	return LoadApplied(ctx, session, service)
}

// LoadApplied returns the version -> checksum map of applied migrations for a
// service. Exported so services that need to seed the tracking table (e.g.
// auth's one-time backfill from its legacy gocqlx table) can inspect it.
func LoadApplied(ctx context.Context, session *gocql.Session, service string) (map[int]string, error) {
	applied := make(map[int]string)
	iter := session.Query(
		`SELECT version, checksum FROM service_migrations WHERE service = ?`,
		service,
	).WithContext(ctx).Iter()
	var version int
	var checksum string
	for iter.Scan(&version, &checksum) {
		applied[version] = checksum
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("load applied migrations for %s: %w", service, err)
	}
	return applied, nil
}

func appliedByChecksum(applied map[int]string, checksum string) (int, bool) {
	for version, sum := range applied {
		if sum == checksum {
			return version, true
		}
	}
	return 0, false
}

func parseFilename(name string) (version int, desc string, err error) {
	var rest string
	if _, err := fmt.Sscanf(name, "%d_%s", &version, &rest); err != nil {
		return 0, "", fmt.Errorf("migration file %q must be named NNN_description.cql", name)
	}
	rest = strings.TrimSuffix(rest, ".cql")
	return version, rest, nil
}

// splitStatements breaks a CQL file into individual statements. It tolerates
// lines starting with -- comments and keeps semicolons inside quoted strings.
func splitStatements(content string) []string {
	var statements []string
	var current strings.Builder
	inString := false
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if !inString && (trimmed == "" || strings.HasPrefix(trimmed, "--")) {
			continue
		}
		for i := 0; i < len(line); i++ {
			switch line[i] {
			case '\'':
				if i == 0 || line[i-1] != '\\' {
					inString = !inString
				}
			case ';':
				if !inString {
					if stmt := strings.TrimSpace(current.String()); stmt != "" {
						statements = append(statements, stmt)
					}
					current.Reset()
					continue
				}
			}
			current.WriteByte(line[i])
		}
		current.WriteString("\n")
	}
	if stmt := strings.TrimSpace(current.String()); stmt != "" {
		statements = append(statements, stmt)
	}
	return statements
}

func checksumOf(content []byte) string {
	return ChecksumOf(content)
}

// ChecksumOf is the exported checksum function used to identify migration
// content. Exported for the auth backfill path; see services/auth/db/migrate.go.
func ChecksumOf(content []byte) string {
	h := sha256.New()
	h.Write(content)
	return hex.EncodeToString(h.Sum(nil))
}
