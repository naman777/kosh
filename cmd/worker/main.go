// Command worker runs Kosh's background recovery: status pollers for
// payments, refunds and payouts, hold expiry, and the daily T+1 settlement.
//
//	KOSH_DATABASE_URL    Postgres URL (default: the compose database)
//	KOSH_BANK_URL        bank simulator URL (default http://localhost:8091)
//	KOSH_WORKER_INTERVAL pause between rounds (default 1s)
//	KOSH_CRASHPOINT      crash at a named point, for failure tests
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/naman777/kosh/internal/bank"
	"github.com/naman777/kosh/internal/clock"
	"github.com/naman777/kosh/internal/db"
	"github.com/naman777/kosh/internal/env"
	"github.com/naman777/kosh/internal/payments"
	"github.com/naman777/kosh/internal/worker"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("worker exiting", "err", err)
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
	svc, err := payments.New(ctx, payments.Deps{
		Pool: pool,
		Bank: bank.NewHTTPClient(env.String("KOSH_BANK_URL", "http://localhost:8091")),
	})
	if err != nil {
		return err
	}
	w := &worker.Worker{Svc: svc, Clock: clock.Real{}, Log: log}
	log.Info("worker running")
	return w.Run(ctx, env.Duration("KOSH_WORKER_INTERVAL", time.Second))
}
