// Command ledgerbench measures ledger throughput and latency against a real
// Postgres. It creates a scratch database, runs W workers posting two-leg
// transfers for a fixed time, checks the invariants, and drops the database.
//
//	go run ./cmd/ledgerbench -mode spread    # random pairs of 1,000 accounts: no shared rows, the ceiling
//	go run ./cmd/ledgerbench -mode hot       # every transfer credits one merchant
//	go run ./cmd/ledgerbench -mode capture   # real capture shape: bank_receivable, merchant, fee, GST
//
// KOSH_TEST_DATABASE_URL is the admin connection (default: compose Postgres).
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/naman777/kosh/internal/clock"
	"github.com/naman777/kosh/internal/ledger"
	"github.com/naman777/kosh/internal/money"
	"github.com/naman777/kosh/internal/testdb"
	"github.com/naman777/kosh/invariants"
)

func main() {
	workers := flag.Int("workers", 16, "concurrent posters")
	duration := flag.Duration("duration", 15*time.Second, "how long to run")
	accounts := flag.Int("accounts", 1000, "accounts to spread transfers over")
	mode := flag.String("mode", "spread", "spread | hot | capture")
	flag.Parse()

	ctx := context.Background()
	scratch, err := testdb.Clone(ctx, int32(*workers)+4)
	check(err)
	defer scratch.Drop()
	pool := scratch.Pool
	l := ledger.New(clock.Real{})
	sys, err := l.EnsureSystemAccounts(ctx, pool)
	check(err)
	// Wallets have no balance limit, so random transfers never fail.
	wallets := make([]ledger.AccountID, *accounts)
	merchants := make([]ledger.AccountID, *accounts)
	for i := range wallets {
		wallets[i], err = l.EnsureAccount(ctx, pool, ledger.AccountSpec{Code: fmt.Sprint("wallet_", i), Normal: ledger.NormalDebit})
		check(err)
		merchants[i], err = l.EnsureAccount(ctx, pool, ledger.MerchantPayableSpec(uuid.New()))
		check(err)
	}
	entry := func(r *rand.Rand) []ledger.Posting {
		amt := money.Paise(r.Int64N(100_000) + 100)
		switch *mode {
		case "hot":
			return []ledger.Posting{
				{Account: wallets[r.IntN(len(wallets))], Side: ledger.Debit, Amount: amt},
				{Account: merchants[0], Side: ledger.Credit, Amount: amt},
			}
		case "capture":
			b, _ := money.Fees(amt+10_000, 200) // ≥ ₹100, so fee and GST are never zero
			return []ledger.Posting{
				{Account: sys[ledger.BankReceivable], Side: ledger.Debit, Amount: b.Amount},
				{Account: merchants[r.IntN(len(merchants))], Side: ledger.Credit, Amount: b.Net},
				{Account: sys[ledger.FeeRevenue], Side: ledger.Credit, Amount: b.Fee},
				{Account: sys[ledger.GSTPayable], Side: ledger.Credit, Amount: b.GST},
			}
		default:
			from := r.IntN(len(wallets))
			to := r.IntN(len(wallets) - 1)
			if to >= from {
				to++
			}
			return []ledger.Posting{
				{Account: wallets[from], Side: ledger.Debit, Amount: amt},
				{Account: wallets[to], Side: ledger.Credit, Amount: amt},
			}
		}
	}

	var (
		ops       atomic.Int64
		mu        sync.Mutex
		latencies []time.Duration
		wg        sync.WaitGroup
	)
	deadline := time.Now().Add(*duration)
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(seed, 3))
			var local []time.Duration
			for time.Now().Before(deadline) {
				ps := entry(r)
				start := time.Now()
				err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
					return l.Post(ctx, tx, ledger.Entry{ID: uuid.New(), Kind: "transfer", RefID: uuid.New(), Postings: ps})
				})
				check(err)
				local = append(local, time.Since(start))
				ops.Add(1)
			}
			mu.Lock()
			latencies = append(latencies, local...)
			mu.Unlock()
		}(uint64(w))
	}
	wg.Wait()

	rep, err := invariants.Check(ctx, pool, 1, 2, 5, 11)
	check(err)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	pct := func(p float64) time.Duration { return latencies[int(float64(len(latencies)-1)*p)] }
	fmt.Printf("ledger transfers, mode=%s, %d workers, %d accounts, %s\n", *mode, *workers, *accounts, *duration)
	fmt.Printf("  transfers   %d\n", ops.Load())
	fmt.Printf("  throughput  %.0f transfers/s\n", float64(ops.Load())/duration.Seconds())
	fmt.Printf("  latency     p50 %.2f ms  p99 %.2f ms  max %.2f ms\n", ms(pct(0.5)), ms(pct(0.99)), ms(pct(1)))
	fmt.Printf("  invariants  %s\n", rep)
	if !rep.OK() {
		scratch.Drop()
		os.Exit(1)
	}
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "ledgerbench:", err)
		os.Exit(1)
	}
}
