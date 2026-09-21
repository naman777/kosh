// Package worker runs Kosh's background recovery: the status pollers for
// payments, refunds and payouts, hold expiry, the daily T+1 settlement, and
// reconciliation of the bank's settlement files.
// Every task is safe to run from several workers at once (claims use
// SKIP LOCKED, settlement is unique per merchant and day).
package worker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/naman777/kosh/internal/bank"
	"github.com/naman777/kosh/internal/clock"
	"github.com/naman777/kosh/internal/payments"
	"github.com/naman777/kosh/internal/recon"
)

// Worker drives the background tasks.
type Worker struct {
	Svc   *payments.Service
	Clock clock.Clock
	Log   *slog.Logger
	// Recon and Settlements enable daily reconciliation; nil skips it.
	Recon       *recon.Engine
	Settlements bank.SettlementSource

	Batch            int           // rows per poll round; default 100
	SettlementEvery  time.Duration // how often to try yesterday's settlement; default 10 min
	lastSettlement   time.Time
	lastSettlementOK string
}

// Stats counts what one round did.
type Stats struct {
	Payments, Refunds, Payouts, Expired int
	Settled                             *payments.SettlementReport
	Reconciled                          *recon.Report
}

// RunOnce runs every task once.
func (w *Worker) RunOnce(ctx context.Context) (Stats, error) {
	batch := w.Batch
	if batch == 0 {
		batch = 100
	}
	// Each task runs even if an earlier one reported errors; the errors are
	// returned together at the end.
	var (
		st   Stats
		errs []error
		err  error
	)
	st.Payments, err = w.Svc.PollPayments(ctx, batch)
	errs = append(errs, err)
	st.Refunds, err = w.Svc.PollRefunds(ctx, batch)
	errs = append(errs, err)
	st.Payouts, err = w.Svc.PollPayouts(ctx, batch)
	errs = append(errs, err)
	st.Expired, err = w.Svc.ExpireHolds(ctx, batch)
	errs = append(errs, err)

	every := w.SettlementEvery
	if every == 0 {
		every = 10 * time.Minute
	}
	now := w.Clock.Now()
	yesterday := now.In(payments.IST).AddDate(0, 0, -1)
	day := yesterday.Format("2006-01-02")
	if day != w.lastSettlementOK || now.Sub(w.lastSettlement) >= every {
		w.lastSettlement = now
		rep, err := w.Svc.RunSettlement(ctx, yesterday)
		if err != nil && !errors.Is(err, payments.ErrTooEarly) {
			errs = append(errs, err)
		}
		if err == nil {
			st.Settled = &rep
		}
		// Yesterday's bank file arrives today: reconcile it.
		ok := err == nil
		if w.Recon != nil && w.Settlements != nil {
			rrep, err := w.Recon.ReconcileDay(ctx, w.Settlements, day)
			if err != nil {
				errs = append(errs, err)
				ok = false
			} else {
				st.Reconciled = &rrep
			}
		}
		if ok {
			w.lastSettlementOK = day
		}
	}
	return st, errors.Join(errs...)
}

// Run loops until ctx ends, pausing interval between rounds. Errors are
// logged and the loop continues: every task is retried next round.
func (w *Worker) Run(ctx context.Context, interval time.Duration) error {
	log := w.Log
	if log == nil {
		log = slog.Default()
	}
	for {
		st, err := w.RunOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			log.Error("worker round failed", "err", err)
		} else if st.Payments+st.Refunds+st.Payouts+st.Expired > 0 || (st.Settled != nil && st.Settled.Payouts > 0) {
			log.Info("worker round", "payments", st.Payments, "refunds", st.Refunds, "payouts", st.Payouts,
				"expired", st.Expired, "settled", st.Settled)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}
