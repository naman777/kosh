package sim

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/naman777/kosh/internal/bank"
	"github.com/naman777/kosh/internal/banksim"
	"github.com/naman777/kosh/internal/clock"
	"github.com/naman777/kosh/internal/money"
)

// Bank is a deterministic, in-process bank for the harness. It behaves like
// banksim (dedupe by id, declines, timeouts, pending-then-final, outages,
// late and duplicated callbacks, settlement files with injected faults) but
// every decision comes from its seeded RNG and every time from the fake
// clock, and callbacks wait in a queue until the harness delivers them.
type Bank struct {
	mu    sync.Mutex
	clk   clock.Clock
	rng   *rand.Rand
	seed  uint64
	recs  map[uuid.UUID]*simRecord
	order []uuid.UUID
	seq   int
	queue []scheduled

	FailRate, TimeoutRate, PendingRate float64
	Outage                             bool
	Faults                             banksim.SettlementFaults
}

type simRecord struct {
	kind    string
	id      uuid.UUID
	amount  money.Paise
	method  string
	final   bank.Result
	finalAt time.Time
}

type scheduled struct {
	at time.Time
	cb bank.Callback
}

// NewBank returns a healthy sim bank.
func NewBank(seed uint64, clk clock.Clock) *Bank {
	return &Bank{clk: clk, seed: seed, rng: rand.New(rand.NewPCG(seed, 0xba4b)), recs: map[uuid.UUID]*simRecord{}}
}

func (b *Bank) view(r *simRecord) bank.Result {
	if b.clk.Now().Before(r.finalAt) {
		return bank.Result{Status: bank.Pending}
	}
	return r.final
}

func (b *Bank) decide(kind string, id uuid.UUID, amount money.Paise, method, prefix string) (bank.Result, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.Outage {
		return bank.Result{}, bank.ErrUnknownOutcome
	}
	if r, ok := b.recs[id]; ok {
		return b.view(r), nil
	}
	now := b.clk.Now()
	b.seq++
	r := &simRecord{kind: kind, id: id, amount: amount, method: method, finalAt: now,
		final: bank.Result{Status: bank.Success, BankRef: fmt.Sprintf("%s%08d", prefix, b.seq)}}
	timeout := false
	switch x := b.rng.Float64(); {
	case x < b.FailRate:
		r.final = bank.Result{Status: bank.Failed, Reason: "declined_by_issuer"}
	case x < b.FailRate+b.TimeoutRate:
		timeout = true
	case x < b.FailRate+b.TimeoutRate+b.PendingRate:
		// Decided now, revealed later: from a minute to two hours.
		r.finalAt = now.Add(time.Minute + time.Duration(b.rng.Int64N(int64(2*time.Hour))))
		if b.rng.Float64() < 0.2 {
			r.final = bank.Result{Status: bank.Failed, Reason: "collect_request_expired"}
		}
	}
	b.recs[id] = r
	b.order = append(b.order, id)

	// Callbacks: none 20% of the time (the poller must cope), else 1–3
	// copies, each up to 15 minutes after the answer is final.
	if b.rng.Float64() >= 0.2 {
		copies := 1 + b.rng.IntN(3)
		for i := 0; i < copies; i++ {
			at := r.finalAt.Add(time.Duration(b.rng.Int64N(int64(15 * time.Minute))))
			b.queue = append(b.queue, scheduled{at: at, cb: bank.Callback{Kind: kind, ID: id, Status: r.final.Status,
				BankRef: r.final.BankRef, Reason: r.final.Reason}})
		}
	}
	if timeout {
		return bank.Result{}, bank.ErrUnknownOutcome
	}
	return b.view(r), nil
}

// Authorize implements bank.Client.
func (b *Bank) Authorize(_ context.Context, req bank.AuthorizeRequest) (bank.Result, error) {
	return b.decide(bank.KindPayment, req.PaymentID, req.Amount, req.Method, "UTR")
}

// Refund implements bank.Client.
func (b *Bank) Refund(_ context.Context, req bank.RefundRequest) (bank.Result, error) {
	return b.decide(bank.KindRefund, req.RefundID, req.Amount, "", "RFN")
}

// Payout implements bank.Client.
func (b *Bank) Payout(_ context.Context, req bank.PayoutRequest) (bank.Result, error) {
	return b.decide(bank.KindPayout, req.PayoutID, req.Amount, "", "NEFT")
}

// Status implements bank.Client.
func (b *Bank) Status(_ context.Context, id uuid.UUID) (bank.Result, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.Outage {
		return bank.Result{}, bank.ErrUnknownOutcome
	}
	r, ok := b.recs[id]
	if !ok {
		return bank.Result{Status: bank.Failed, Reason: bank.ReasonUnknownID}, nil
	}
	return b.view(r), nil
}

// Due removes and returns the callbacks due by now, in scheduling order.
func (b *Bank) Due() []bank.Callback {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.clk.Now()
	var due []bank.Callback
	rest := b.queue[:0]
	for _, s := range b.queue {
		if !s.at.After(now) {
			due = append(due, s.cb)
		} else {
			rest = append(rest, s)
		}
	}
	b.queue = rest
	return due
}

// SettlementFile implements bank.SettlementSource with the same builder and
// CSV format as banksim.
func (b *Bank) SettlementFile(_ context.Context, date string) ([]byte, error) {
	b.mu.Lock()
	now := b.clk.Now()
	var items []banksim.SettledItem
	for _, id := range b.order {
		r := b.recs[id]
		if r.final.Status != bank.Success || r.finalAt.After(now) {
			continue
		}
		items = append(items, banksim.SettledItem{Kind: r.kind, ID: r.id, Amount: r.amount, Method: r.method,
			BankRef: r.final.BankRef, FinalAt: r.finalAt})
	}
	faults := b.Faults
	b.mu.Unlock()
	lines, _, err := banksim.BuildSettlement(items, date, b.seed, faults)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	err = banksim.WriteCSV(&buf, lines)
	return buf.Bytes(), err
}
