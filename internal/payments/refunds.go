package payments

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/naman777/kosh/internal/bank"
	"github.com/naman777/kosh/internal/crash"
	"github.com/naman777/kosh/internal/idempotency"
	"github.com/naman777/kosh/internal/ledger"
	"github.com/naman777/kosh/internal/money"
	"github.com/naman777/kosh/internal/outbox"
)

// Refund states.
const (
	RefundPending   = "pending"
	RefundProcessed = "processed"
	RefundFailed    = "failed"
)

// CreateRefundInput is POST /v1/payments/{id}/refunds. A zero amount means
// "refund whatever is left".
type CreateRefundInput struct {
	Amount money.Paise `json:"amount"`
}

// Refund is a refunds row.
type Refund struct {
	ID            uuid.UUID
	PaymentID     uuid.UUID
	MerchantID    uuid.UUID
	Kind          string // merchant | auto
	Amount        money.Paise
	State         string
	HoldEntryID   uuid.UUID
	BankRef       *string
	FailureReason *string
	Version       int
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// Refund kinds.
const (
	RefundKindMerchant = "merchant" // requested by the merchant, held on merchant_payable
	RefundKindAuto     = "auto"     // Kosh returning a late success, held on customer_refund_payable
)

const refundCols = `id, payment_id, merchant_id, kind, amount, state, hold_entry_id, bank_ref, failure_reason, version, created_at, updated_at`

func scanRefund(row pgx.Row) (Refund, error) {
	var r Refund
	err := row.Scan(&r.ID, &r.PaymentID, &r.MerchantID, &r.Kind, &r.Amount, &r.State, &r.HoldEntryID,
		&r.BankRef, &r.FailureReason, &r.Version, &r.CreatedAt, &r.UpdatedAt)
	return r, err
}

func (r Refund) View() RefundView {
	return RefundView{ID: r.ID, Entity: "refund", PaymentID: r.PaymentID, Amount: int64(r.Amount),
		Currency: "INR", State: r.State, BankRef: r.BankRef, FailureReason: r.FailureReason,
		CreatedAt: r.CreatedAt.UTC(), UpdatedAt: r.UpdatedAt.UTC()}
}

const recoveryRefundCreated = "refund_created"

// CreateRefund runs in three phases, like CreatePayment:
//
//	A. (tx) reserve the amount on the payment with a guarded update, place a
//	   ledger hold on the merchant's payable, insert the refund as pending
//	B. ask the bank to refund (idempotent on the refund id)
//	C. (tx) commit or void the hold and store the response
//
// The guarded update makes concurrent refunds safe (invariant 4) and the
// hold keeps the merchant's balance from going negative (invariant 5).
func (s *Service) CreateRefund(ctx context.Context, k idempotency.Key, paymentID uuid.UUID) (Response, error) {
	return s.idempotent(ctx, k, func(rec *idempotency.Record) (Response, error) {
		if rec.RecoveryPoint == idempotency.RecoveryStarted {
			in, err := decode[CreateRefundInput](k.Body)
			if err != nil {
				return Response{}, err
			}
			if in.Amount < 0 {
				return Response{}, errorf(http.StatusBadRequest, "bad_request", "amount must be positive")
			}
			if err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
				id, err := s.reserveRefund(ctx, tx, k.MerchantID, paymentID, in.Amount)
				if err != nil {
					return err
				}
				return s.idem.Advance(ctx, tx, rec, recoveryRefundCreated, id)
			}); err != nil {
				return Response{}, err
			}
		}
		crash.Point("refunds.create.after_reserve")

		r, err := scanRefund(s.pool.QueryRow(ctx, `SELECT `+refundCols+` FROM refunds WHERE id = $1`, rec.ResourceID.UUID))
		if err != nil {
			return Response{}, err
		}
		if r.State != RefundPending {
			return s.finish(ctx, rec, http.StatusCreated, func(pgx.Tx) (any, error) { return r.View(), nil })
		}

		var bankRef string
		if err := s.pool.QueryRow(ctx, `SELECT bank_ref FROM payments WHERE id = $1`, r.PaymentID).Scan(&bankRef); err != nil {
			return Response{}, err
		}
		bctx, cancel := context.WithTimeout(ctx, s.cfg.BankTimeout)
		res, err := s.bank.Refund(bctx, bank.RefundRequest{RefundID: r.ID, PaymentBankRef: bankRef, Amount: r.Amount})
		cancel()
		if err != nil && !errors.Is(err, bank.ErrUnknownOutcome) && !errors.Is(err, context.DeadlineExceeded) {
			return Response{}, fmt.Errorf("payments: refund: %w", err)
		}
		if err != nil {
			res = bank.Result{Status: bank.Pending}
		}
		crash.Point("refunds.create.after_bank")

		return s.finish(ctx, rec, http.StatusCreated, func(tx pgx.Tx) (any, error) {
			r, err := s.resolveRefund(ctx, tx, r.ID, res)
			return r.View(), err
		})
	})
}

func (s *Service) reserveRefund(ctx context.Context, tx pgx.Tx, merchantID, paymentID uuid.UUID, amount money.Paise) (uuid.UUID, error) {
	// Lock the payment so concurrent refunds on it queue up, then reserve
	// with a guarded update that refuses to push refunded past amount even
	// if the lock were ever dropped.
	p, err := s.getPayment(ctx, tx, merchantID, paymentID, true)
	if err != nil {
		return uuid.Nil, err
	}
	reserved := amount
	if reserved == 0 {
		reserved = p.Amount - p.Refunded
	}
	if reserved <= 0 {
		return uuid.Nil, s.whyNotRefundable(ctx, tx, merchantID, paymentID, amount)
	}
	tag, err := tx.Exec(ctx, `
		UPDATE payments SET refunded = refunded + $2, updated_at = $3
		WHERE id = $1 AND state = 'captured' AND refunded + $2 <= amount`,
		paymentID, int64(reserved), s.clock.Now())
	if err != nil {
		return uuid.Nil, err
	}
	if tag.RowsAffected() == 0 {
		return uuid.Nil, s.whyNotRefundable(ctx, tx, merchantID, paymentID, reserved)
	}

	_, payable, err := s.merchantPayable(ctx, tx, merchantID)
	if err != nil {
		return uuid.Nil, err
	}
	refundID, holdID := s.ids.New(), s.ids.New()
	now := s.clock.Now()
	err = s.ledger.Hold(ctx, tx, ledger.Entry{ID: holdID, Kind: "refund", RefID: refundID, Postings: []ledger.Posting{
		{Account: payable, Side: ledger.Debit, Amount: reserved},
		{Account: s.sys[ledger.BankReceivable], Side: ledger.Credit, Amount: reserved},
	}}, now.Add(s.cfg.RefundHoldTTL))
	if errors.Is(err, ledger.ErrInsufficientFunds) {
		return uuid.Nil, errorf(http.StatusUnprocessableEntity, "insufficient_balance",
			"merchant balance cannot cover a refund of %v", reserved)
	}
	if err != nil {
		return uuid.Nil, err
	}
	r, err := scanRefund(tx.QueryRow(ctx, `
		INSERT INTO refunds (id, payment_id, merchant_id, amount, state, hold_entry_id, created_at, updated_at, next_poll_at)
		VALUES ($1, $2, $3, $4, 'pending', $5, $6, $6, $7)
		RETURNING `+refundCols,
		refundID, paymentID, merchantID, reserved, holdID, now, now.Add(s.cfg.CreatedGrace)))
	if err != nil {
		return uuid.Nil, err
	}
	return refundID, s.emitRefund(ctx, tx, r)
}

// emitRefund writes the outbox event for the refund's current version.
// A new refund's event is refund.created; later ones name the new state.
func (s *Service) emitRefund(ctx context.Context, tx pgx.Tx, r Refund) error {
	typ := "refund." + r.State
	if r.Version == 1 {
		typ = "refund.created"
	}
	_, err := outbox.Write(ctx, tx, s.clock.Now(), outbox.Event{
		AggregateType: "refund", AggregateID: r.ID, Version: r.Version, MerchantID: r.MerchantID,
		Type: typ, Payload: r.View(),
	})
	return err
}

func (s *Service) whyNotRefundable(ctx context.Context, tx pgx.Tx, merchantID, paymentID uuid.UUID, amount money.Paise) error {
	p, err := s.getPayment(ctx, tx, merchantID, paymentID, false)
	if err != nil {
		return err
	}
	switch {
	case p.State != StateCaptured && p.State != StateRefunded:
		return errorf(http.StatusConflict, "invalid_state", "payment %s is %s; only captured payments can be refunded", p.ID, p.State)
	case p.Refunded >= p.Amount:
		return errorf(http.StatusUnprocessableEntity, "fully_refunded", "payment %s is already fully refunded", p.ID)
	default:
		return errorf(http.StatusUnprocessableEntity, "refund_exceeds_amount",
			"refund of %v exceeds the %v left on payment %s", amount, p.Amount-p.Refunded, p.ID)
	}
}

// resolveRefund applies the bank's answer to a pending refund. A refund
// already resolved is returned unchanged, so this is safe to call twice.
func (s *Service) resolveRefund(ctx context.Context, tx pgx.Tx, refundID uuid.UUID, res bank.Result) (Refund, error) {
	r, err := scanRefund(tx.QueryRow(ctx, `SELECT `+refundCols+` FROM refunds WHERE id = $1 FOR UPDATE`, refundID))
	if err != nil {
		return Refund{}, err
	}
	if r.State != RefundPending {
		return r, nil
	}
	switch res.Status {
	case bank.Success:
		if err := s.ledger.CommitConfirmed(ctx, tx, r.HoldEntryID); err != nil {
			return Refund{}, fmt.Errorf("payments: commit refund hold: %w", err)
		}
		ref := res.BankRef
		r, err = s.setRefundState(ctx, tx, r.ID, RefundProcessed, &ref, nil)
		if err != nil {
			return Refund{}, err
		}
		if r.Kind == RefundKindAuto {
			return r, nil
		}
		// Fully refunded once processed refunds cover the whole amount.
		var full bool
		if err := tx.QueryRow(ctx, `
			SELECT p.amount = (SELECT COALESCE(SUM(amount), 0) FROM refunds
			                   WHERE payment_id = p.id AND kind = 'merchant' AND state = 'processed')
			FROM payments p WHERE p.id = $1`, r.PaymentID).Scan(&full); err != nil {
			return Refund{}, err
		}
		if full {
			if _, err := s.transition(ctx, tx, r.PaymentID, StateCaptured, StateRefunded, change{}); err != nil {
				return Refund{}, err
			}
		}
		return r, nil
	case bank.Failed:
		if err := s.ledger.VoidHold(ctx, tx, r.HoldEntryID); err != nil {
			return Refund{}, fmt.Errorf("payments: void refund hold: %w", err)
		}
		reason := res.Reason
		if reason == "" {
			reason = "declined"
		}
		return s.failRefund(ctx, tx, r, reason)
	default:
		return r, nil // still pending; the hold keeps the money reserved
	}
}

// failRefund marks a refund failed and, for a merchant refund, gives the
// amount back to the payment. The caller has already voided (or expired)
// the hold. A failed auto refund leaves its credit in
// customer_refund_payable, where reconciliation (phase 5) surfaces it.
func (s *Service) failRefund(ctx context.Context, tx pgx.Tx, r Refund, reason string) (Refund, error) {
	if r.Kind == RefundKindMerchant {
		if _, err := tx.Exec(ctx, `UPDATE payments SET refunded = refunded - $2, updated_at = $3 WHERE id = $1`,
			r.PaymentID, int64(r.Amount), s.clock.Now()); err != nil {
			return Refund{}, err
		}
	}
	return s.setRefundState(ctx, tx, r.ID, RefundFailed, nil, &reason)
}

func (s *Service) setRefundState(ctx context.Context, tx pgx.Tx, id uuid.UUID, state string, bankRef, reason *string) (Refund, error) {
	r, err := scanRefund(tx.QueryRow(ctx, `
		UPDATE refunds
		SET state = $2, version = version + 1, updated_at = $3,
		    bank_ref = COALESCE($4, bank_ref), failure_reason = COALESCE($5, failure_reason),
		    next_poll_at = NULL,
		    bank_confirmed_at = CASE WHEN $4::text IS NOT NULL THEN $3 ELSE bank_confirmed_at END
		WHERE id = $1 AND state = 'pending'
		RETURNING `+refundCols, id, state, s.clock.Now(), bankRef, reason))
	if err != nil {
		return Refund{}, err
	}
	return r, s.emitRefund(ctx, tx, r)
}

// ExpireHolds deals with refund and payout holds past their deadline. It is
// the backstop behind the pollers, which normally resolve everything long
// before. It never voids blindly: it asks the bank first, because a hold
// whose money already moved must be committed, not released. Only an
// answer of failed, never-received or still-pending gives the hold up.
// It returns how many holds it resolved.
func (s *Service) ExpireHolds(ctx context.Context, limit int) (int, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT kind, ref_id FROM journal_entries
		WHERE state = 'pending' AND expires_at <= $1
		ORDER BY expires_at
		LIMIT $2`, s.clock.Now(), limit)
	if err != nil {
		return 0, err
	}
	type held struct {
		kind string
		ref  uuid.UUID
	}
	var due []held
	for rows.Next() {
		var h held
		if err := rows.Scan(&h.kind, &h.ref); err != nil {
			rows.Close()
			return 0, err
		}
		due = append(due, h)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	n := 0
	var errs []error
	for _, h := range due {
		res, err := s.bankStatus(ctx, h.ref)
		if err != nil {
			continue // bank unreachable: try again next round
		}
		if res.Status != bank.Success {
			res = bank.Result{Status: bank.Failed, Reason: "hold_expired"}
		}
		err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
			var err error
			switch h.kind {
			case "refund":
				_, err = s.resolveRefund(ctx, tx, h.ref, res)
			case "payout":
				_, err = s.resolvePayout(ctx, tx, h.ref, res)
			default:
				return fmt.Errorf("payments: expired hold of unknown kind %q", h.kind)
			}
			return err
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("expire %s %s: %w", h.kind, h.ref, err))
			continue
		}
		n++
	}
	return n, errors.Join(errs...)
}

// GetRefund reads one of a merchant's refunds.
func (s *Service) GetRefund(ctx context.Context, merchantID, id uuid.UUID) (RefundView, error) {
	r, err := scanRefund(s.pool.QueryRow(ctx, `SELECT `+refundCols+` FROM refunds WHERE id = $1 AND merchant_id = $2`, id, merchantID))
	if errors.Is(err, pgx.ErrNoRows) {
		return RefundView{}, errorf(http.StatusNotFound, "not_found", "refund %s not found", id)
	}
	if err != nil {
		return RefundView{}, err
	}
	return r.View(), nil
}
