// Command api serves Kosh's merchant API.
//
//	KOSH_DATABASE_URL   Postgres URL (default: the compose database)
//	KOSH_BANK_URL       bank simulator URL (default http://localhost:8091)
//	KOSH_ADMIN_TOKEN    bearer token for /admin (default: /admin disabled)
//	KOSH_ADDR           listen address (default :8090)
//	KOSH_BANK_TIMEOUT   per-call bank timeout (default 30s)
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/naman777/kosh/internal/api"
	"github.com/naman777/kosh/internal/bank"
	"github.com/naman777/kosh/internal/db"
	"github.com/naman777/kosh/internal/env"
	"github.com/naman777/kosh/internal/payments"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("api exiting", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.Open(ctx, db.URLFromEnv())
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool); err != nil {
		return err
	}

	cfg := payments.DefaultConfig()
	cfg.BankTimeout = env.Duration("KOSH_BANK_TIMEOUT", cfg.BankTimeout)
	svc, err := payments.New(ctx, payments.Deps{
		Pool:   pool,
		Bank:   bank.NewHTTPClient(env.String("KOSH_BANK_URL", "http://localhost:8091")),
		Config: cfg,
	})
	if err != nil {
		return err
	}

	srv := &http.Server{
		Addr:              env.String("KOSH_ADDR", ":8090"),
		Handler:           api.New(svc, os.Getenv("KOSH_ADMIN_TOKEN"), log),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Info("api listening", "addr", srv.Addr)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
