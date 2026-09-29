package payments

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/naman777/kosh/internal/bank"
	"github.com/naman777/kosh/internal/ledger"
	"github.com/naman777/kosh/internal/money"
)

// A bank answer for a payment can arrive three ways: the API's own call
// (phase C of CreatePayment), a bank callback, or the status poller. They
// race, repeat and arrive late, so all three go through applyPaymentResult,
// which locks the payment and decides from its current state:
//
//	created / pending + success  → authorized
//	created / pending + failed   → failed
//	created           + pending  → pending (poller takes over)
//	failed            + success  → late success: auto refund
//	anything else                → no change (a duplicate or stale answer)
func (s *Service) applyPaymentResult(ctx context.Context, tx pgx.Tx, id uuid.UUID, res bank.Result) (Payment, error) {
	p, err := scanPayment(tx.QueryRow(ctx, `SELECT `+paymentCols+` FROM payments WHERE id = $1 FOR UPDATE`, id))
	if err != nil {
		return Payment{}, fmt.Errorf("payments: load %s: %w", id, err)
	}
	switch p.State {
	case StateCreated, StatePending:
		switch res.Status {
		case bank.Success:
			ref := res.BankRef
			p, err := s.transition(ctx, tx, id, p.State, StateAuthorized, change{bankRef: &ref})
			if err != nil {
				return Payment{}, err
			}
			return p, s.postAuthorization(ctx, tx, p)
		case bank.Failed:
			reason := res.Reason
			if reason == "" {
				reason = "declined"
			}
			return s.transition(ctx, tx, id, p.State, StateFailed, change{failureReason: &reason})
		default:
			if p.State == StateCreated {
				return s.transition(ctx, tx, id, StateCreated, StatePending, change{})
			}
			return p, nil
		}
	case StateFailed:
		if res.Status == bank.Success {
			return s.lateSuccess(ctx, tx, p, res)
		}
	}
	return p, nil
}

// postAuthorization records that the bank collected the customer's money:
// Dr bank_receivable / Cr unallocated_receipts. Capture later moves it from
// unallocated_receipts to the merchant, fee and GST. Recognizing the
// receivable here, not at capture, is what lets reconciliation settle every
// payment the bank collected, captured or not.
func (s *Service) postAuthorization(ctx context.Context, tx pgx.Tx, p Payment) error {
	return s.ledger.Post(ctx, tx, ledger.Entry{ID: s.ids.New(), Kind: "authorization", RefID: p.ID, Postings: []ledger.Posting{
		{Account: s.chart.For(ledger.BankReceivable, p.ID), Side: ledger.Debit, Amount: p.Amount},
		{Account: s.chart.For(ledger.UnallocatedReceipts, p.ID), Side: ledger.Credit, Amount: p.Amount},
	}})
}

// lateSuccess handles the bank collecting money for a payment Kosh had
// already marked failed (it timed out, or was abandoned). The customer paid
// and the merchant was told "failed", so the money goes back:
//
//  1. record the bank reference on the payment (recon needs it)
//  2. post late_success: Dr bank_receivable / Cr customer_refund_payable
//  3. create an auto refund holding Dr customer_refund_payable / Cr bank_receivable;
//     the refund poller submits it to the bank right away
//
// It is idempotent: a payment that already has its bank reference was
// handled before.
func (s *Service) lateSuccess(ctx context.Context, tx pgx.Tx, p Payment, res bank.Result) (Payment, error) {
	if p.BankRef != nil {
		return p, nil
	}
	now := s.clock.Now()
	p, err := scanPayment(tx.QueryRow(ctx, `
		UPDATE payments SET bank_ref = $2, updated_at = $3, bank_confirmed_at = $3
		WHERE id = $1 AND bank_ref IS NULL
		RETURNING `+paymentCols, p.ID, res.BankRef, now))
	if err != nil {
		return Payment{}, fmt.Errorf("payments: record late success: %w", err)
	}
	recv, owed := s.chart.For(ledger.BankReceivable, p.ID), s.chart.One(ledger.CustomerRefundPayable)
	if err := s.ledger.Post(ctx, tx, ledger.Entry{ID: s.ids.New(), Kind: "late_success", RefID: p.ID, Postings: []ledger.Posting{
		{Account: recv, Side: ledger.Debit, Amount: p.Amount},
		{Account: owed, Side: ledger.Credit, Amount: p.Amount},
	}}); err != nil {
		return Payment{}, err
	}
	refundID, holdID := s.ids.New(), s.ids.New()
	if err := s.ledger.Hold(ctx, tx, ledger.Entry{ID: holdID, Kind: "refund", RefID: refundID, Postings: []ledger.Posting{
		{Account: owed, Side: ledger.Debit, Amount: p.Amount},
		{Account: recv, Side: ledger.Credit, Amount: p.Amount},
	}}, now.Add(s.cfg.RefundHoldTTL)); err != nil {
		return Payment{}, err
	}
	r, err := scanRefund(tx.QueryRow(ctx, `
		INSERT INTO refunds (id, payment_id, merchant_id, kind, amount, state, hold_entry_id, failure_reason, created_at, updated_at, next_poll_at)
		VALUES ($1, $2, $3, 'auto', $4, 'pending', $5, NULL, $6, $6, $6)
		RETURNING `+refundCols, refundID, p.ID, p.MerchantID, p.Amount, holdID, now))
	if err != nil {
		return Payment{}, fmt.Errorf("payments: create auto refund: %w", err)
	}
	return p, s.emitRefund(ctx, tx, r)
}

// pollDelay is the backoff before poll number attempt.
func (s *Service) pollDelay(attempt int) time.Duration {
	d := s.cfg.PollBase
	for i := 1; i < attempt && d < s.cfg.PollMax; i++ {
		d *= 2
	}
	return min(d, s.cfg.PollMax)
}

type dueItem struct {
	id        uuid.UUID
	createdAt time.Time
}

// claimDue leases up to limit unresolved rows of table whose poll is due,
// pushing their next poll out by the backoff. Other workers skip them.
func (s *Service) claimDue(ctx context.Context, table, unresolved string, limit int) ([]dueItem, error) {
	var out []dueItem
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		now := s.clock.Now()
		rows, err := tx.Query(ctx, `
			SELECT id, created_at, poll_attempts FROM `+table+`
			WHERE `+unresolved+` AND next_poll_at <= $1
			ORDER BY next_poll_at, id
			LIMIT $2
			FOR UPDATE SKIP LOCKED`, now, limit)
		if err != nil {
			return err
		}
		var attempts []int
		for rows.Next() {
			var (
				d dueItem
				n int
			)
			if err := rows.Scan(&d.id, &d.createdAt, &n); err != nil {
				rows.Close()
				return err
			}
			out = append(out, d)
			attempts = append(attempts, n+1)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for i, d := range out {
			if _, err := tx.Exec(ctx, `UPDATE `+table+` SET poll_attempts = $2, next_poll_at = $3 WHERE id = $1`,
				d.id, attempts[i], now.Add(s.pollDelay(attempts[i]))); err != nil {
				return err
			}
		}
		return nil
	})
	return out, err
}

func (s *Service) bankStatus(ctx context.Context, id uuid.UUID) (bank.Result, error) {
	bctx, cancel := context.WithTimeout(ctx, s.cfg.BankTimeout)
	defer cancel()
	return s.bank.Status(bctx, id)
}

// PollPayments asks the bank about payments stuck in created or pending and
// resolves them. A payment the bank never received is failed (nothing
// moved). One still pending after PendingTimeout is failed too; if the bank
// later says success, applyPaymentResult turns that into an auto refund.
func (s *Service) PollPayments(ctx context.Context, limit int) (int, error) {
	due, err := s.claimDue(ctx, "payments", "state IN ('created', 'pending')", limit)
	if err != nil {
		return 0, err
	}
	n := 0
	var errs []error
	for _, d := range due {
		res, err := s.bankStatus(ctx, d.id)
		if err != nil {
			continue // bank unreachable; the claim already rescheduled it
		}
		switch {
		case res.Status == bank.Failed && res.Reason == bank.ReasonUnknownID:
			res.Reason = "not_received_by_bank"
		case res.Status == bank.Pending && s.clock.Now().Sub(d.createdAt) >= s.cfg.PendingTimeout:
			res = bank.Result{Status: bank.Failed, Reason: "bank_timeout"}
		}
		if err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
			_, err := s.applyPaymentResult(ctx, tx, d.id, res)
			return err
		}); err != nil {
			// One bad item must not block the batch: it stays claimed with
			// backoff, and the rest go ahead.
			errs = append(errs, fmt.Errorf("payment %s: %w", d.id, err))
			continue
		}
		n++
	}
	return n, errors.Join(errs...)
}

// PollRefunds resolves pending refunds. A refund the bank never received
// (its request died after phase A, or it is a fresh auto refund) is
// submitted now: the refund was promised, and the bank dedupes by id.
func (s *Service) PollRefunds(ctx context.Context, limit int) (int, error) {
	due, err := s.claimDue(ctx, "refunds", "state = 'pending'", limit)
	if err != nil {
		return 0, err
	}
	n := 0
	var errs []error
	for _, d := range due {
		res, err := s.bankStatus(ctx, d.id)
		if err != nil {
			continue
		}
		if res.Status == bank.Failed && res.Reason == bank.ReasonUnknownID {
			var (
				amount  int64
				bankRef *string
			)
			if err := s.pool.QueryRow(ctx, `
				SELECT r.amount, p.bank_ref FROM refunds r JOIN payments p ON p.id = r.payment_id WHERE r.id = $1`,
				d.id).Scan(&amount, &bankRef); err != nil {
				errs = append(errs, fmt.Errorf("refund %s: %w", d.id, err))
				continue
			}
			bctx, cancel := context.WithTimeout(ctx, s.cfg.BankTimeout)
			res, err = s.bank.Refund(bctx, bank.RefundRequest{RefundID: d.id, PaymentBankRef: deref(bankRef), Amount: money.Paise(amount)})
			cancel()
			if err != nil {
				continue
			}
		}
		if res.Status == bank.Pending && s.clock.Now().Sub(d.createdAt) >= s.cfg.RefundPendingTimeout {
			res = bank.Result{Status: bank.Failed, Reason: "bank_timeout"}
		}
		if err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
			_, err := s.resolveRefund(ctx, tx, d.id, res)
			return err
		}); err != nil {
			errs = append(errs, fmt.Errorf("refund %s: %w", d.id, err))
			continue
		}
		n++
	}
	return n, errors.Join(errs...)
}

// ErrUnknownCallback means a callback named something Kosh never created.
var ErrUnknownCallback = errors.New("payments: callback for unknown id")

// HandleBankCallback applies an asynchronous answer from the bank. It is
// idempotent: banks send callbacks late, out of order, and more than once.
func (s *Service) HandleBankCallback(ctx context.Context, cb bank.Callback) error {
	if cb.Status == bank.Pending {
		return nil
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var err error
		switch cb.Kind {
		case bank.KindPayment:
			_, err = s.applyPaymentResult(ctx, tx, cb.ID, cb.Result())
		case bank.KindRefund:
			_, err = s.resolveRefund(ctx, tx, cb.ID, cb.Result())
		case bank.KindPayout:
			_, err = s.resolvePayout(ctx, tx, cb.ID, cb.Result())
		default:
			return fmt.Errorf("payments: unknown callback kind %q", cb.Kind)
		}
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrUnknownCallback
		}
		return err
	})
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
