package bank

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
)

// Outcome scripts what the Fake does with the next new request.
type Outcome int

const (
	// OutcomeSuccess answers Success.
	OutcomeSuccess Outcome = iota
	// OutcomeFail answers Failed.
	OutcomeFail
	// OutcomeTimeout returns ErrUnknownOutcome but records Success, like a
	// bank that moved the money and lost the reply.
	OutcomeTimeout
	// OutcomeTimeoutFailed returns ErrUnknownOutcome and records Failed.
	OutcomeTimeoutFailed
	// OutcomePending answers Pending and stays pending until Settle.
	OutcomePending
	// OutcomeTimeoutPending returns ErrUnknownOutcome and stays pending
	// until Settle: the slow-UPI case that ends in a late success.
	OutcomeTimeoutPending
	// OutcomeDown returns ErrUnknownOutcome and records nothing, like an
	// outage: the bank never saw the request.
	OutcomeDown
)

// Fake is an in-process bank for tests. It dedupes by id like a real bank:
// asking again about the same id returns the recorded answer.
type Fake struct {
	mu      sync.Mutex
	queue   []Outcome
	def     Outcome
	results map[uuid.UUID]Result
	seq     atomic.Int64
	// Calls counts Authorize, Refund and Payout calls, including repeats.
	Calls atomic.Int64
}

// NewFake returns a Fake that succeeds by default.
func NewFake() *Fake { return &Fake{results: map[uuid.UUID]Result{}} }

// Next queues outcomes for the next new requests, in order.
func (f *Fake) Next(o ...Outcome) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queue = append(f.queue, o...)
}

// SetDefault sets the outcome used when the queue is empty.
func (f *Fake) SetDefault(o Outcome) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.def = o
}

// Settle records a final answer for id, as if the bank finished later. A
// Success without a BankRef gets one.
func (f *Fake) Settle(id uuid.UUID, r Result) Result {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Status == Success && r.BankRef == "" {
		r.BankRef = fmt.Sprintf("LT%010d", f.seq.Add(1))
	}
	f.results[id] = r
	return r
}

func (f *Fake) decide(id uuid.UUID, prefix string) (Result, error) {
	f.Calls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if r, ok := f.results[id]; ok {
		return r, nil
	}
	o := f.def
	if len(f.queue) > 0 {
		o, f.queue = f.queue[0], f.queue[1:]
	}
	ref := fmt.Sprintf("%s%010d", prefix, f.seq.Add(1))
	switch o {
	case OutcomeFail:
		f.results[id] = Result{Status: Failed, Reason: "declined"}
		return f.results[id], nil
	case OutcomeTimeout:
		f.results[id] = Result{Status: Success, BankRef: ref}
		return Result{}, ErrUnknownOutcome
	case OutcomeTimeoutFailed:
		f.results[id] = Result{Status: Failed, Reason: "declined"}
		return Result{}, ErrUnknownOutcome
	case OutcomePending:
		f.results[id] = Result{Status: Pending}
		return f.results[id], nil
	case OutcomeTimeoutPending:
		f.results[id] = Result{Status: Pending}
		return Result{}, ErrUnknownOutcome
	case OutcomeDown:
		return Result{}, ErrUnknownOutcome
	default:
		f.results[id] = Result{Status: Success, BankRef: ref}
		return f.results[id], nil
	}
}

// Authorize implements Client.
func (f *Fake) Authorize(_ context.Context, req AuthorizeRequest) (Result, error) {
	return f.decide(req.PaymentID, "UT")
}

// Refund implements Client.
func (f *Fake) Refund(_ context.Context, req RefundRequest) (Result, error) {
	return f.decide(req.RefundID, "RF")
}

// Payout implements Client.
func (f *Fake) Payout(_ context.Context, req PayoutRequest) (Result, error) {
	return f.decide(req.PayoutID, "PO")
}

// Status implements Client.
func (f *Fake) Status(_ context.Context, id uuid.UUID) (Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r, ok := f.results[id]; ok {
		return r, nil
	}
	return Result{Status: Failed, Reason: ReasonUnknownID}, nil
}
