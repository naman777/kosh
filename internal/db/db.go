package db

import (
	"context"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultURL is the compose Postgres from docker-compose.yml.
const DefaultURL = "postgres://kosh:kosh@localhost:5442/kosh?sslmode=disable"

// URLFromEnv returns KOSH_DATABASE_URL, or DefaultURL when it is unset.
func URLFromEnv() string {
	if u := os.Getenv("KOSH_DATABASE_URL"); u != "" {
		return u
	}
	return DefaultURL
}

// Open connects a pool and pings it.
func Open(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}
