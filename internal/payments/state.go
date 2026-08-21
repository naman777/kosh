package payments

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/naman777/kosh/internal/money"
)

// State is a payment's state.
type State string

const (
	StateCreated    State = "created"    // row exists, bank not yet asked (or asked with no answer recorded)
	StatePending    State = "pending"    // bank call timed out or answered pending
	StateAuthorized State = "authorized" // bank collected the money; awaiting capture
	StateCaptured   State = "captured"   // merchant is owed the money (ledger entry posted)
	StateFailed     State = "failed"     // bank declined, or the attempt was abandoned
	StateRefunded   State = "refunded"   // fully refunded
)

// transitions is the whole state machine. Anything not listed is illegal.
var transitions = map[State][]State{
	StateCreated:    {StatePending, StateAuthorized, StateFailed},
	StatePending:    {StateAuthorized, StateFailed},
	StateAuthorized: {StateCaptured, StateFailed},
	StateCaptured:   {StateRefunded},
}

// CanTransition reports whether from → to is legal.
func CanTransition(from, to State) bool {
	for _, s := range transitions[from] {
		if s == to {
			return true
		}
	}
	return false
}

// ErrIllegalTransition is a programming error: code asked for a transition
// the state machine does not allow.
var ErrIllegalTransition = errors.New("payments: illegal state transition")

// errStale means the guarded update matched no row: the payment was not in
// the expected state (a lost race or a repeated request).
var errStale = errors.New("payments: payment not in expected state")

// Payment is a payments row.
type Payment struct {
	ID            uuid.UUID
	MerchantID    uuid.UUID
	OrderID       uuid.UUID
	Amount        money.Paise
	Refunded      money.Paise
	Method        string
	State         State
	BankRef       *string
	FailureReason *string
	Version       int
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

const paymentCols = `id, merchant_id, order_id, amount, refunded, method, state, bank_ref, failure_reason, version, created_at, updated_at`

func scanPayment(row pgx.Row) (Payment, error) {
	var p Payment
	err := row.Scan(&p.ID, &p.MerchantID, &p.OrderID, &p.Amount, &p.Refunded, &p.Method, &p.State,
		&p.BankRef, &p.FailureReason, &p.Version, &p.CreatedAt, &p.UpdatedAt)
	return p, err
}

// change carries optional column updates that ride along with a transition.
type change struct {
	bankRef       *string
	failureReason *string
}

// transition moves a payment from → to with a guarded update, bumping its
// version. It returns errStale when the payment was not in state from.
// Every payment state change in Kosh goes through here.
func (s *Service) transition(ctx context.Context, tx pgx.Tx, id uuid.UUID, from, to State, c change) (Payment, error) {
	if !CanTransition(from, to) {
		return Payment{}, fmt.Errorf("%w: %s → %s", ErrIllegalTransition, from, to)
	}
	p, err := scanPayment(tx.QueryRow(ctx, `
		UPDATE payments
		SET state = $3, version = version + 1, updated_at = $4,
		    bank_ref = COALESCE($5, bank_ref),
		    failure_reason = COALESCE($6, failure_reason)
		WHERE id = $1 AND state = $2
		RETURNING `+paymentCols,
		id, from, to, s.clock.Now(), c.bankRef, c.failureReason))
	if errors.Is(err, pgx.ErrNoRows) {
		return Payment{}, errStale
	}
	if err != nil {
		return Payment{}, fmt.Errorf("payments: transition %s → %s: %w", from, to, err)
	}
	return p, nil
}

func (s *Service) getPayment(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, merchantID, id uuid.UUID, forUpdate bool) (Payment, error) {
	sql := `SELECT ` + paymentCols + ` FROM payments WHERE id = $1 AND merchant_id = $2`
	if forUpdate {
		sql += ` FOR UPDATE`
	}
	p, err := scanPayment(q.QueryRow(ctx, sql, id, merchantID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Payment{}, errorf(404, "not_found", "payment %s not found", id)
	}
	return p, err
}
