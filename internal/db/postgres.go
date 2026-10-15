package db

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// minPoolConns: pgxpool defaults to max(4, NumCPU) connections, which on a small VPS put ~30
// crawler goroutines (a worker and a feeder per source) in line for 4 connections to a remote
// Postgres, so they spent their time waiting on each other instead of running in parallel. An
// explicit pool_max_conns in DATABASE_URL still wins.
const minPoolConns = 20

func ConnectPostgres(databaseURL string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	if !strings.Contains(databaseURL, "pool_max_conns") && cfg.MaxConns < minPoolConns {
		cfg.MaxConns = minPoolConns
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return nil, err
	}

	if err := pool.Ping(context.Background()); err != nil {
		return nil, err
	}

	return pool, nil
}
