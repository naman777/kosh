package ledger_test

import (
	"context"
	"errors"
	"flag"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/naman777/kosh/internal/ledger"
	"github.com/naman777/kosh/internal/money"
)

var transfers = flag.Int("transfers", 5_000, "number of random operations in TestPropertyDB (use 1000000 for the full run)")

// TestConcurrentTransfers has 50 goroutines move money among 5 merchant
// accounts in both directions at once. Crossing transfers (A→B while B→A)
// deadlock unless accounts are locked in a fixed order. Expect: no deadlock
// or other errors besides ErrInsufficientFunds, money conserved, no account
// negative, and every invariant holding.
func TestConcurrentTransfers(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	recv := f.sys[ledger.BankReceivable]

	const (
		nAccounts = 5
		workers   = 50
		perWorker = 40
		funding   = money.Paise(100_000)
	)
	accts := make([]ledger.AccountID, nAccounts)
	for i := range accts {
		accts[i] = f.merchant(t)
		if err := f.post(t, dr(recv, funding), cr(accts[i], funding)); err != nil {
			t.Fatal(err)
		}
	}

	var ok, insufficient atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(seed, 7))
			for i := 0; i < perWorker; i++ {
				from := accts[r.IntN(nAccounts)]
				to := accts[r.IntN(nAccounts)]
				for to == from {
					to = accts[r.IntN(nAccounts)]
				}
				amt := money.Paise(r.Int64N(30_000) + 1)
				err := pgx.BeginFunc(ctx, f.pool, func(tx pgx.Tx) error {
					return f.l.Post(ctx, tx, ledger.Entry{ID: uuid.New(), Kind: "transfer", RefID: uuid.New(),
						Postings: []ledger.Posting{dr(from, amt), cr(to, amt)}})
				})
				switch {
				case err == nil:
					ok.Add(1)
				case errors.Is(err, ledger.ErrInsufficientFunds):
					insufficient.Add(1)
				default:
					errs <- err
					return
				}
			}
		}(uint64(w))
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("unexpected error (a deadlock here means lock ordering is broken): %v", err)
	}

	var total money.Paise
	for _, id := range accts {
		a := f.account(t, id)
		if a.Posted() < 0 {
			t.Fatalf("account %d negative: %v", id, a.Posted())
		}
		total += a.Posted()
	}
	if total != funding*nAccounts {
		t.Fatalf("money not conserved: %v, want %v", total, funding*nAccounts)
	}
	t.Logf("%d transfers posted, %d refused for insufficient funds", ok.Load(), insufficient.Load())
	if ok.Load() == 0 {
		t.Fatal("no transfer succeeded")
	}
	f.checkInvariants(t)
}

// TestHoldResolveRace commits and voids the same hold from two goroutines.
// Exactly one must win, and balances must move exactly once.
func TestHoldResolveRace(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	m := f.merchant(t)
	recv := f.sys[ledger.BankReceivable]
	if err := f.post(t, dr(recv, 1_000_000), cr(m, 1_000_000)); err != nil {
		t.Fatal(err)
	}

	var commits int
	for round := 0; round < 40; round++ {
		id := uuid.New()
		err := f.tx(t, func(tx pgx.Tx) error {
			return f.l.Hold(ctx, tx, ledger.Entry{ID: id, Kind: "refund", RefID: uuid.New(),
				Postings: []ledger.Posting{dr(m, 100), cr(recv, 100)}}, f.clk.Now().Add(time.Hour))
		})
		if err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		results := make([]error, 4)
		start := make(chan struct{})
		for i := range results {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				results[i] = pgx.BeginFunc(ctx, f.pool, func(tx pgx.Tx) error {
					if i%2 == 0 {
						return f.l.CommitHold(ctx, tx, id)
					}
					return f.l.VoidHold(ctx, tx, id)
				})
			}(i)
		}
		close(start)
		wg.Wait()

		wins := 0
		for i, err := range results {
			switch {
			case err == nil:
				wins++
				if i%2 == 0 {
					commits++
				}
			case errors.Is(err, ledger.ErrHoldResolved):
			default:
				t.Fatalf("round %d: %v", round, err)
			}
		}
		if wins != 1 {
			t.Fatalf("round %d: %d resolutions succeeded, want exactly 1", round, wins)
		}
	}
	a := f.account(t, m)
	if want := money.Paise(1_000_000 - 100*commits); a.Posted() != want || a.PendingDr != 0 {
		t.Fatalf("merchant %+v, want posted %v and nothing pending", a, want)
	}
	f.checkInvariants(t)
}

// TestPropertyDB runs random posts, holds, commits and voids from 8 workers
// against a real database, then checks every invariant. The default is 5,000
// operations; `-transfers=1000000` is the full run.
func TestPropertyDB(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	n := *transfers
	if testing.Short() {
		n = 500
	}

	// 12 accounts: the 5 system accounts plus 7 merchants (flagged).
	var accts []ledger.AccountID
	for _, id := range f.sys {
		accts = append(accts, id)
	}
	for i := 0; i < 7; i++ {
		accts = append(accts, f.merchant(t))
	}

	var (
		mu    sync.Mutex
		holds []uuid.UUID
		next  atomic.Int64
		stats [4]atomic.Int64 // posted, held, resolved, refused
	)
	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(seed, 99))
			for next.Add(1) <= int64(n) {
				var err error
				switch op := r.IntN(10); {
				case op < 2: // resolve a random open hold
					mu.Lock()
					if len(holds) == 0 {
						mu.Unlock()
						continue
					}
					k := r.IntN(len(holds))
					id := holds[k]
					holds = append(holds[:k], holds[k+1:]...)
					mu.Unlock()
					err = pgx.BeginFunc(ctx, f.pool, func(tx pgx.Tx) error {
						if r.IntN(2) == 0 {
							return f.l.CommitHold(ctx, tx, id)
						}
						return f.l.VoidHold(ctx, tx, id)
					})
					if err == nil {
						stats[2].Add(1)
					}
				default:
					e := randomEntry(r, accts, 50_000)
					hold := op < 4
					err = pgx.BeginFunc(ctx, f.pool, func(tx pgx.Tx) error {
						if hold {
							return f.l.Hold(ctx, tx, e, f.clk.Now().Add(time.Hour))
						}
						return f.l.Post(ctx, tx, e)
					})
					if err == nil && hold {
						mu.Lock()
						holds = append(holds, e.ID)
						mu.Unlock()
						stats[1].Add(1)
					} else if err == nil {
						stats[0].Add(1)
					}
				}
				if errors.Is(err, ledger.ErrInsufficientFunds) {
					stats[3].Add(1)
					err = nil
				}
				if err != nil {
					errs <- err
					return
				}
			}
		}(uint64(w))
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	t.Logf("%d ops: %d posted, %d holds, %d resolved, %d refused by flags",
		n, stats[0].Load(), stats[1].Load(), stats[2].Load(), stats[3].Load())
	f.checkInvariants(t)
}

// randomEntry builds a balanced entry with 1–2 debit and 1–3 credit legs.
func randomEntry(r *rand.Rand, accts []ledger.AccountID, maxAmount int64) ledger.Entry {
	pick := func() ledger.AccountID { return accts[r.IntN(len(accts))] }
	total := money.Paise(r.Int64N(maxAmount) + 3)
	var ps []ledger.Posting
	if r.IntN(2) == 0 {
		ps = append(ps, dr(pick(), total))
	} else {
		a := money.Paise(r.Int64N(int64(total-1))) + 1
		ps = append(ps, dr(pick(), a), dr(pick(), total-a))
	}
	left := total
	for left > 0 {
		amt := left
		if r.IntN(2) == 0 && left > 1 {
			amt = money.Paise(r.Int64N(int64(left-1))) + 1
		}
		ps = append(ps, cr(pick(), amt))
		left -= amt
	}
	return ledger.Entry{ID: uuid.New(), Kind: "transfer", RefID: uuid.New(), Postings: ps}
}
