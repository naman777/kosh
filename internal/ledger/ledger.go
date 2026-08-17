// Package ledger is Kosh's double-entry ledger.
//
// Every money movement is a journal entry made of postings; each posting
// debits or credits one account by a positive amount in paise, and an
// entry's debits always equal its credits. Accounts keep running totals
// (posted_dr/cr, pending_dr/cr) that are updated in the same transaction as
// the postings, so a balance read is a single row and never a SUM.
//
// Three rules keep it correct under concurrency:
//
//  1. Postings and balance updates commit in one transaction. The ledger
//     never opens its own transaction; callers pass a pgx.Tx so payment
//     state, ledger rows and outbox events commit together.
//  2. Accounts are locked in ascending id order (SELECT … ORDER BY id FOR
//     UPDATE), so two entries touching the same accounts can never deadlock.
//  3. A hold writes only pending_* totals. CommitHold or VoidHold moves it
//     exactly once, guarded by UPDATE … WHERE state = 'pending'.
package ledger

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/naman777/kosh/internal/clock"
	"github.com/naman777/kosh/internal/money"
)

// AccountID identifies an account.
type AccountID int64

// Side is the side of a posting.
type Side string

const (
	Debit  Side = "dr"
	Credit Side = "cr"
)

// NormalSide is the side on which an account's balance grows: debit for
// assets and expenses, credit for liabilities and revenue.
type NormalSide string

const (
	NormalDebit  NormalSide = "debit"
	NormalCredit NormalSide = "credit"
)

// Flags restrict how an account's balance may move. Both are checked against
// posted plus pending debits (or credits), so a hold reserves funds.
type Flags int32

const (
	// DebitsMustNotExceedCredits keeps a credit-normal account from going
	// negative: pending_dr + posted_dr ≤ posted_cr. Merchant payables use it.
	DebitsMustNotExceedCredits Flags = 1 << 0
	// CreditsMustNotExceedDebits is the mirror image for debit-normal accounts.
	CreditsMustNotExceedDebits Flags = 1 << 1
)

// Entry states.
const (
	StatePosted  = "posted"
	StatePending = "pending"
	StateVoided  = "voided"
)

// Posting moves Amount on one side of one account.
type Posting struct {
	Account AccountID
	Side    Side
	Amount  money.Paise
}

// Entry is one business event's postings. (Kind, RefID) is unique across the
// ledger, so the same business event can never be recorded twice.
type Entry struct {
	ID       uuid.UUID
	Kind     string
	RefID    uuid.UUID
	Postings []Posting
}

// Balances are an account's running totals.
type Balances struct {
	PostedDr, PostedCr, PendingDr, PendingCr money.Paise
}

// Account is one row of the chart of accounts.
type Account struct {
	ID      AccountID
	Code    string
	OwnerID uuid.NullUUID
	Normal  NormalSide
	Flags   Flags
	Balances
}

// Posted is the posted balance on the account's normal side.
func (a Account) Posted() money.Paise {
	if a.Normal == NormalDebit {
		return a.PostedDr - a.PostedCr
	}
	return a.PostedCr - a.PostedDr
}

// Available is the posted balance less pending movements away from the
// normal side, which is what a hold has reserved.
func (a Account) Available() money.Paise {
	if a.Normal == NormalDebit {
		return a.PostedDr - a.PostedCr - a.PendingCr
	}
	return a.PostedCr - a.PostedDr - a.PendingDr
}

var (
	ErrNoID              = errors.New("ledger: entry needs an id and ref id")
	ErrNoKind            = errors.New("ledger: entry needs a kind")
	ErrTooFewPostings    = errors.New("ledger: entry needs at least two postings")
	ErrNonPositive       = errors.New("ledger: posting amount must be positive")
	ErrBadSide           = errors.New("ledger: posting side must be dr or cr")
	ErrUnbalanced        = errors.New("ledger: debits do not equal credits")
	ErrOverflow          = errors.New("ledger: amount overflow")
	ErrDuplicateEntry    = errors.New("ledger: entry already exists for this id or (kind, ref_id)")
	ErrAccountNotFound   = errors.New("ledger: account not found")
	ErrInsufficientFunds = errors.New("ledger: debits would exceed credits")
	ErrExceedsDebits     = errors.New("ledger: credits would exceed debits")
	ErrEntryNotFound     = errors.New("ledger: entry not found")
	ErrNotHold           = errors.New("ledger: entry is not a pending hold")
	ErrHoldResolved      = errors.New("ledger: hold already posted or voided")
	ErrHoldExpired       = errors.New("ledger: hold expired")
	ErrBadExpiry         = errors.New("ledger: hold expiry must be in the future")
)

// Querier is satisfied by *pgxpool.Pool, *pgx.Conn and pgx.Tx.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Ledger posts entries. It is stateless apart from its clock and safe for
// concurrent use.
type Ledger struct {
	clock clock.Clock
}

// New returns a Ledger that timestamps entries with c.
func New(c clock.Clock) *Ledger { return &Ledger{clock: c} }

func flagError(base error, id AccountID) error {
	return fmt.Errorf("%w (account %d)", base, id)
}
