package metrics

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/naman777/kosh/internal/bank"
)

// Bank wraps a bank client so every call is timed by operation and outcome.
type Bank struct{ Inner bank.Client }

func observe(op string, start time.Time, r bank.Result, err error) {
	outcome := string(r.Status)
	switch {
	case errors.Is(err, bank.ErrUnknownOutcome):
		outcome = "unknown"
	case err != nil:
		outcome = "error"
	}
	BankCall.WithLabelValues(op, outcome).Observe(time.Since(start).Seconds())
}

// Authorize implements bank.Client.
func (b Bank) Authorize(ctx context.Context, req bank.AuthorizeRequest) (bank.Result, error) {
	start := time.Now()
	r, err := b.Inner.Authorize(ctx, req)
	observe("authorize", start, r, err)
	return r, err
}

// Refund implements bank.Client.
func (b Bank) Refund(ctx context.Context, req bank.RefundRequest) (bank.Result, error) {
	start := time.Now()
	r, err := b.Inner.Refund(ctx, req)
	observe("refund", start, r, err)
	return r, err
}

// Payout implements bank.Client.
func (b Bank) Payout(ctx context.Context, req bank.PayoutRequest) (bank.Result, error) {
	start := time.Now()
	r, err := b.Inner.Payout(ctx, req)
	observe("payout", start, r, err)
	return r, err
}

// Status implements bank.Client.
func (b Bank) Status(ctx context.Context, id uuid.UUID) (bank.Result, error) {
	start := time.Now()
	r, err := b.Inner.Status(ctx, id)
	observe("status", start, r, err)
	return r, err
}
