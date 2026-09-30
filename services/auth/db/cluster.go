package db

import (
	"context"

	"github.com/Ajay01103/go-mux/pkg/scylladb"
	"github.com/gocql/gocql"
)

const Keyspace = "auth_ks"

type Config = scylladb.Config

func Connect(ctx context.Context, cfg Config) (*gocql.Session, error) {
	return scylladb.Connect(ctx, cfg, Keyspace)
}
