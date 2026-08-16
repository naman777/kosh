// Package testdb gives each test its own freshly migrated Postgres database.
//
// The first call in a process migrates a template database named after the
// migrations' fingerprint. Every test then clones it with
// CREATE DATABASE … TEMPLATE, which takes milliseconds, and drops the clone
// when the test ends. Tests never share state, so they can run in parallel.
//
// Set KOSH_TEST_DATABASE_URL to an admin connection (any database on the
// server). The default is the compose Postgres on port 5442.
package testdb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/naman777/kosh/internal/db"
)

const defaultAdminURL = "postgres://kosh:kosh@localhost:5442/postgres?sslmode=disable"

var (
	templateOnce sync.Once
	templateName string
	templateErr  error
)

func adminURL() string {
	if u := os.Getenv("KOSH_TEST_DATABASE_URL"); u != "" {
		return u
	}
	return defaultAdminURL
}

// withDatabase swaps the database name in a postgres URL.
func withDatabase(raw, name string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	u.Path = "/" + name
	return u.String(), nil
}

func ensureTemplate(ctx context.Context) (string, error) {
	templateOnce.Do(func() {
		fp, err := db.Fingerprint()
		if err != nil {
			templateErr = err
			return
		}
		name := "kosh_tpl_" + fp
		admin, err := pgx.Connect(ctx, adminURL())
		if err != nil {
			templateErr = fmt.Errorf("testdb: connect admin (is `docker compose up -d` running?): %w", err)
			return
		}
		defer admin.Close(ctx)

		// go test runs packages as parallel processes; serialize template creation.
		if _, err := admin.Exec(ctx, `SELECT pg_advisory_lock(7406002)`); err != nil {
			templateErr = err
			return
		}
		defer admin.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock(7406002)`) //nolint:errcheck

		var exists bool
		if err := admin.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, name).Scan(&exists); err != nil {
			templateErr = err
			return
		}
		if !exists {
			building := name + "_building"
			_, _ = admin.Exec(ctx, `DROP DATABASE IF EXISTS `+pgx.Identifier{building}.Sanitize()+` WITH (FORCE)`)
			if _, err := admin.Exec(ctx, `CREATE DATABASE `+pgx.Identifier{building}.Sanitize()); err != nil {
				templateErr = err
				return
			}
			u, err := withDatabase(adminURL(), building)
			if err != nil {
				templateErr = err
				return
			}
			pool, err := db.Open(ctx, u)
			if err != nil {
				templateErr = err
				return
			}
			err = db.Migrate(ctx, pool)
			pool.Close()
			if err != nil {
				templateErr = fmt.Errorf("testdb: migrate template: %w", err)
				return
			}
			// Rename only after a successful migration, so a half-built
			// template is never cloned.
			if _, err := admin.Exec(ctx, `ALTER DATABASE `+pgx.Identifier{building}.Sanitize()+` RENAME TO `+pgx.Identifier{name}.Sanitize()); err != nil {
				templateErr = err
				return
			}
		}
		templateName = name
	})
	return templateName, templateErr
}

// New returns a pool connected to a fresh, migrated database that is dropped
// when the test ends.
func New(t testing.TB) *pgxpool.Pool {
	t.Helper()
	pool, _ := NewWithURL(t)
	return pool
}

// NewWithURL is New but also returns the connection URL, for tests that start
// subprocesses against the same database.
func NewWithURL(t testing.TB) (*pgxpool.Pool, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	tpl, err := ensureTemplate(ctx)
	if err != nil {
		t.Fatalf("testdb: %v", err)
	}

	var b [6]byte
	_, _ = rand.Read(b[:])
	name := "kosh_t_" + hex.EncodeToString(b[:])

	admin, err := pgx.Connect(ctx, adminURL())
	if err != nil {
		t.Fatalf("testdb: connect admin: %v", err)
	}
	defer admin.Close(ctx)
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+pgx.Identifier{name}.Sanitize()+` TEMPLATE `+pgx.Identifier{tpl}.Sanitize()); err != nil {
		t.Fatalf("testdb: clone template: %v", err)
	}

	u, err := withDatabase(adminURL(), name)
	if err != nil {
		t.Fatalf("testdb: %v", err)
	}
	cfg, err := pgxpool.ParseConfig(u)
	if err != nil {
		t.Fatalf("testdb: %v", err)
	}
	cfg.MaxConns = 40
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("testdb: open: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		admin, err := pgx.Connect(ctx, adminURL())
		if err != nil {
			t.Logf("testdb: cleanup connect: %v", err)
			return
		}
		defer admin.Close(ctx)
		if _, err := admin.Exec(ctx, `DROP DATABASE IF EXISTS `+pgx.Identifier{name}.Sanitize()+` WITH (FORCE)`); err != nil {
			t.Logf("testdb: drop %s: %v", name, err)
		}
	})
	return pool, u
}
