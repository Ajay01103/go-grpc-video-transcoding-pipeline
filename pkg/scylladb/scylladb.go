// Package scylladb is the shared ScyllaDB connector for all services: readiness
// wait, keyspace bootstrap, and session construction. Each service's db package
// wraps Connect with its own keyspace name.
//
// Bug history: gocql's zero-value Consistency (0) is gocql.Any, which ScyllaDB
// rejects for reads ("ANY ConsistencyLevel is only supported for writes"). Any
// service whose main.go left Consistency unset (or explicitly 0) failed its
// readiness ping forever. Connect now defaults an unset consistency to
// LocalQuorum, matching auth's working configuration.
package scylladb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gocql/gocql"
)

type Config struct {
	Hosts             []string
	Port              int
	Username          string
	Password          string
	Consistency       gocql.Consistency
	Datacenter        string
	ReplicationFactor int
}

// Connect waits for the cluster, bootstraps the service's keyspace if missing,
// and returns a session bound to it.
func Connect(ctx context.Context, cfg Config, keyspace string) (*gocql.Session, error) {
	if len(cfg.Hosts) == 0 {
		cfg.Hosts = []string{"localhost"}
	}
	if cfg.Consistency == gocql.Any { // 0 == unset; ANY is invalid for reads
		cfg.Consistency = gocql.LocalQuorum
	}

	if err := waitForReady(ctx, cfg); err != nil {
		return nil, fmt.Errorf("wait for scylladb: %w", err)
	}

	if err := bootstrapKeyspace(ctx, cfg, keyspace); err != nil {
		return nil, fmt.Errorf("bootstrap keyspace: %w", err)
	}

	return newSession(cfg, keyspace)
}

func waitForReady(ctx context.Context, cfg Config) error {
	const retryInterval = 2 * time.Second
	var lastErr error

	for {
		session, err := newSession(cfg, "")
		if err == nil {
			pingErr := session.Query("SELECT cluster_name FROM system.local").
				WithContext(ctx).
				RetryPolicy(&gocql.ExponentialBackoffRetryPolicy{NumRetries: 2}).
				Exec()
			session.Close()
			if pingErr == nil {
				return nil
			}
			lastErr = pingErr
		} else {
			lastErr = err
		}

		select {
		case <-ctx.Done():
			if lastErr != nil {
				return fmt.Errorf("scylladb not ready (last error: %v): %w", lastErr, ctx.Err())
			}
			return fmt.Errorf("scylladb not ready: %w", ctx.Err())
		case <-time.After(retryInterval):
		}
	}
}

func bootstrapKeyspace(ctx context.Context, cfg Config, keyspace string) error {
	session, err := newSession(cfg, "")
	if err != nil {
		return fmt.Errorf("open bootstrap session: %w", err)
	}
	defer session.Close()

	var name string
	err = session.Query(
		`SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ? LIMIT 1`,
		keyspace,
	).WithContext(ctx).Scan(&name)
	if err == nil {
		return nil
	}
	if !errors.Is(err, gocql.ErrNotFound) {
		return fmt.Errorf("check keyspace: %w", err)
	}

	rf := cfg.ReplicationFactor
	if rf < 1 {
		return fmt.Errorf("invalid replication factor %d; must be >= 1", rf)
	}
	dc := cfg.Datacenter
	if dc == "" {
		dc = "datacenter1"
	}

	q := fmt.Sprintf(
		`CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', '%s': %d}`,
		keyspace,
		dc,
		rf,
	)
	if err := session.Query(q).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("create keyspace: %w", err)
	}

	return nil
}

func newSession(cfg Config, keyspace string) (*gocql.Session, error) {
	cluster := gocql.NewCluster(cfg.Hosts...)
	cluster.Port = cfg.Port
	cluster.Keyspace = keyspace
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: cfg.Username,
		Password: cfg.Password,
	}
	cluster.Consistency = cfg.Consistency
	cluster.Timeout = 10 * time.Second
	cluster.ConnectTimeout = 10 * time.Second
	cluster.SocketKeepalive = 30 * time.Second
	cluster.MaxWaitSchemaAgreement = 30 * time.Second
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(
		gocql.RoundRobinHostPolicy(),
	)
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 0}
	// Single-node dev cluster: skip peer discovery entirely. There's no
	// topology to learn, and it removes any dependence on what address
	// Scylla broadcasts for peers.
	cluster.DisableInitialHostLookup = true

	return gocql.NewSession(*cluster)
}
