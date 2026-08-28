package payments

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/naman777/kosh/internal/bank"
	"github.com/naman777/kosh/internal/crash"
	"github.com/naman777/kosh/internal/idempotency"
	"github.com/naman777/kosh/internal/ledger"
	"github.com/naman777/kosh/internal/money"
)

// CreatePaymentInput is POST /v1/payments.
type CreatePaymentInput struct {
	OrderID uuid.UUID `json:"order_id"`
	Method  string    `json:"method"` // upi | card
}

const recoveryPaymentCreated = "payment_created"

// CreatePayment runs in three phases:
//
//	A. (tx) lock the order, insert the payment as created, record recovery point
//	B. ask the bank to authorize (idempotent on the payment id)
//	C. (tx) apply the answer and store the response
//
// A crash after A resumes at B on retry; a crash after B asks the bank again
// and gets the same answer.
func (s *Service) CreatePayment(ctx context.Context, k idempotency.Key) (Response, error) {
	return s.idempotent(ctx, k, func(rec *idempotency.Record) (Response, error) {
		if rec.RecoveryPoint == idempotency.RecoveryStarted {
			in, err := decode[CreatePaymentInput](k.Body)
			if err != nil {
				return Response{}, err
			}
			if in.Method != "upi" && in.Method != "card" {
				return Response{}, errorf(http.StatusBadRequest, "bad_request", "method must be upi or card")
			}
			if err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
				id, err := s.insertPayment(ctx, tx, k.MerchantID, in)
				if err != nil {
					return err
				}
				return s.idem.Advance(ctx, tx, rec, recoveryPaymentCreated, id)
			}); err != nil {
				return Response{}, err
			}
		}
		crash.Point("payments.create.after_insert")

		p, err := s.getPayment(ctx, s.pool, k.MerchantID, rec.ResourceID.UUID, false)
		if err != nil {
			return Response{}, err
		}
		if p.State != StateCreated {
			// Something else (a status poller) already resolved it.
			return s.finish(ctx, rec, http.StatusCreated, func(pgx.Tx) (any, error) { return p.View(), nil })
		}

		bctx, cancel := context.WithTimeout(ctx, s.cfg.BankTimeout)
		res, err := s.bank.Authorize(bctx, bank.AuthorizeRequest{PaymentID: p.ID, Amount: p.Amount, Method: p.Method})
		cancel()
		if err != nil && !errors.Is(err, bank.ErrUnknownOutcome) && !errors.Is(err, context.DeadlineExceeded) {
			return Response{}, fmt.Errorf("payments: authorize: %w", err)
		}
		if err != nil {
			res = bank.Result{Status: bank.Pending}
		}
		crash.Point("payments.create.after_bank")

		return s.finish(ctx, rec, http.StatusCreated, func(tx pgx.Tx) (any, error) {
			p, err := s.applyAuthorization(ctx, tx, p.ID, StateCreated, res)
			if errors.Is(err, errStale) {
				p, err = s.getPayment(ctx, tx, k.MerchantID, rec.ResourceID.UUID, false)
			}
			return p.View(), err
		})
	})
}

func (s *Service) insertPayment(ctx context.Context, tx pgx.Tx, merchantID uuid.UUID, in CreatePaymentInput) (uuid.UUID, error) {
	var amount money.Paise
	err := tx.QueryRow(ctx, `SELECT amount FROM orders WHERE id = $1 AND merchant_id = $2 FOR UPDATE`,
		in.OrderID, merchantID).Scan(&amount)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, errorf(http.StatusNotFound, "not_found", "order %s not found", in.OrderID)
	}
	if err != nil {
		return uuid.Nil, err
	}
	p, err := scanPayment(tx.QueryRow(ctx, `
		INSERT INTO payments (id, merchant_id, order_id, amount, method, state, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, 'created', $6, $6)
		RETURNING `+paymentCols,
		s.ids.New(), merchantID, in.OrderID, amount, in.Method, s.clock.Now()))
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.ConstraintName == "payments_one_live_per_order" {
		return uuid.Nil, errorf(http.StatusConflict, "order_already_has_payment",
			"order %s already has a payment that has not failed", in.OrderID)
	}
	if err != nil {
		return uuid.Nil, err
	}
	return p.ID, s.emitPayment(ctx, tx, p)
}

// applyAuthorization moves a payment out of from according to the bank's answer.
func (s *Service) applyAuthorization(ctx context.Context, tx pgx.Tx, id uuid.UUID, from State, res bank.Result) (Payment, error) {
	switch res.Status {
	case bank.Success:
		ref := res.BankRef
		return s.transition(ctx, tx, id, from, StateAuthorized, change{bankRef: &ref})
	case bank.Failed:
		reason := res.Reason
		if reason == "" {
			reason = "declined"
		}
		return s.transition(ctx, tx, id, from, StateFailed, change{failureReason: &reason})
	default:
		if from == StatePending {
			// Still no final answer: nothing changes.
			return scanPayment(tx.QueryRow(ctx, `SELECT `+paymentCols+` FROM payments WHERE id = $1`, id))
		}
		return s.transition(ctx, tx, id, from, StatePending, change{})
	}
}

// Capture posts the capture entry: the bank owes Kosh the full amount, and
// Kosh owes the merchant the amount less fee and GST.
func (s *Service) Capture(ctx context.Context, k idempotency.Key, paymentID uuid.UUID) (Response, error) {
	return s.idempotent(ctx, k, func(rec *idempotency.Record) (Response, error) {
		return s.finish(ctx, rec, http.StatusOK, func(tx pgx.Tx) (any, error) {
			p, err := s.transition(ctx, tx, paymentID, StateAuthorized, StateCaptured, change{})
			if errors.Is(err, errStale) {
				cur, err := s.getPayment(ctx, tx, k.MerchantID, paymentID, false)
				if err != nil {
					return nil, err
				}
				if cur.State == StateCaptured || cur.State == StateRefunded {
					return nil, errorf(http.StatusConflict, "already_captured", "payment %s is already captured", paymentID)
				}
				return nil, errorf(http.StatusConflict, "invalid_state", "payment %s is %s, not authorized", paymentID, cur.State)
			}
			if err != nil {
				return nil, err
			}
			if p.MerchantID != k.MerchantID {
				// Rolls back the transition: another merchant's payment looks like a missing one.
				return nil, errorf(http.StatusNotFound, "not_found", "payment %s not found", paymentID)
			}
			if err := s.postCapture(ctx, tx, p); err != nil {
				return nil, err
			}
			if err := s.idem.Advance(ctx, tx, rec, "captured", p.ID); err != nil {
				return nil, err
			}
			return p.View(), nil
		})
	})
}

func (s *Service) postCapture(ctx context.Context, tx pgx.Tx, p Payment) error {
	feeBps, payable, err := s.merchantPayable(ctx, tx, p.MerchantID)
	if err != nil {
		return err
	}
	b, err := money.Fees(p.Amount, feeBps)
	if err != nil {
		return err
	}
	postings := []ledger.Posting{
		{Account: s.sys[ledger.BankReceivable], Side: ledger.Debit, Amount: b.Amount},
		{Account: payable, Side: ledger.Credit, Amount: b.Net},
		{Account: s.sys[ledger.FeeRevenue], Side: ledger.Credit, Amount: b.Fee},
		{Account: s.sys[ledger.GSTPayable], Side: ledger.Credit, Amount: b.GST},
	}
	// Drop zero lines (a 0% fee merchant, or a payment so small the fee rounds to 0).
	kept := postings[:0]
	for _, ps := range postings {
		if ps.Amount > 0 {
			kept = append(kept, ps)
		}
	}
	err = s.ledger.Post(ctx, tx, ledger.Entry{ID: s.ids.New(), Kind: "capture", RefID: p.ID, Postings: kept})
	if errors.Is(err, ledger.ErrDuplicateEntry) {
		// Unreachable while the state guard holds; the ledger's unique
		// (kind, ref_id) is the second line of defence for invariant 3.
		return fmt.Errorf("payments: second capture entry for %s blocked: %w", p.ID, err)
	}
	return err
}

// GetPayment reads one of a merchant's payments.
func (s *Service) GetPayment(ctx context.Context, merchantID, id uuid.UUID) (PaymentView, error) {
	p, err := s.getPayment(ctx, s.pool, merchantID, id, false)
	if err != nil {
		return PaymentView{}, err
	}
	return p.View(), nil
}
