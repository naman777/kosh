// Package bank is Kosh's view of the outside banking world: the acquiring
// bank for cards and the UPI switch. Everything goes through Client, so tests
// and the simulation harness can swap in a scripted Fake, and cmd/banksim
// can play the bank over HTTP.
//
// Every call is keyed by Kosh's own id (payment or refund id), and a real
// bank dedupes on it, so retrying a call after a timeout is always safe.
package bank

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/naman777/kosh/internal/money"
)

// Status is the bank's answer.
type Status string

const (
	// Success: the money moved. BankRef is set.
	Success Status = "success"
	// Failed: the bank declined. Nothing moved.
	Failed Status = "failed"
	// Pending: the bank accepted the request but has no final answer yet.
	Pending Status = "pending"
)

// ErrUnknownOutcome means the call timed out or the connection broke, so the
// money may or may not have moved. Callers must treat the operation as
// pending and ask again later with Status; they must never assume failure.
var ErrUnknownOutcome = errors.New("bank: outcome unknown")

// Result is a final or pending answer from the bank.
type Result struct {
	Status  Status `json:"status"`
	BankRef string `json:"bank_ref,omitempty"` // UTR for UPI, RRN for cards
	Reason  string `json:"reason,omitempty"`   // decline reason when Failed
}

// AuthorizeRequest asks the bank to collect a payment from the customer.
type AuthorizeRequest struct {
	PaymentID uuid.UUID   `json:"payment_id"`
	Amount    money.Paise `json:"amount"`
	Method    string      `json:"method"` // upi | card
}

// RefundRequest asks the bank to return money to the customer.
type RefundRequest struct {
	RefundID       uuid.UUID   `json:"refund_id"`
	PaymentBankRef string      `json:"payment_bank_ref"`
	Amount         money.Paise `json:"amount"`
}

// Client talks to the bank.
type Client interface {
	Authorize(ctx context.Context, req AuthorizeRequest) (Result, error)
	Refund(ctx context.Context, req RefundRequest) (Result, error)
	// Status returns the latest answer for a payment or refund id.
	Status(ctx context.Context, id uuid.UUID) (Result, error)
}
