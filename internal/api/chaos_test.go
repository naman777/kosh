package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/naman777/kosh/internal/api"
	"github.com/naman777/kosh/internal/bank"
	"github.com/naman777/kosh/internal/banksim"
	"github.com/naman777/kosh/internal/payments"
	"github.com/naman777/kosh/internal/testdb"
	"github.com/naman777/kosh/internal/worker"
	"github.com/naman777/kosh/invariants"
)

// shiftClock is the wall clock plus an offset the test can jump forward,
// so "tomorrow's settlement" happens a few seconds into the test.
type shiftClock struct{ offset atomic.Int64 }

func (c *shiftClock) Now() time.Time {
	return time.Now().UTC().Add(time.Duration(c.offset.Load())).Truncate(time.Microsecond)
}

// TestBankChaos is phase 4's done-when: merchants, customers and the
// worker run against a bank that declines, times out, answers "pending"
// and decides later, goes down, and sends every callback up to four times.
// Then the clock jumps a day, settlement pays merchants through the same
// bank, the faults stop, and the system drains. Every invariant must hold.
func TestBankChaos(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos test takes ~20 s")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := testdb.New(t)
	clk := &shiftClock{}
	startDay := clk.Now().In(payments.IST).Format("2006-01-02")

	sim := banksim.New(banksim.Config{}, 7)
	simSrv := httptest.NewServer(sim)
	defer simSrv.Close()

	svc, err := payments.New(ctx, payments.Deps{
		Pool: pool, Bank: bank.NewHTTPClient(simSrv.URL), Clock: clk,
		Config: payments.Config{
			BankTimeout: 300 * time.Millisecond, PollBase: 50 * time.Millisecond, PollMax: 300 * time.Millisecond,
			CreatedGrace: time.Second, PendingTimeout: time.Second, RefundPendingTimeout: 3 * time.Second,
			RefundHoldTTL: time.Hour, PayoutHoldTTL: time.Hour,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	apiSrv := httptest.NewServer(api.New(svc, api.Config{AdminToken: "admin", BankCallbackSecret: "cb"}, nil))
	defer apiSrv.Close()

	chaos := banksim.Config{
		FailRate: 0.1, TimeoutRate: 0.1, PendingRate: 0.25,
		LatencyMinMs: 1, LatencyMaxMs: 10, HangForMs: 1_000, FinalAfterMs: 1_500,
		CallbackURL: apiSrv.URL + "/bank/callbacks", CallbackSecret: "cb", CallbackDelayMs: 50, MaxDuplicateCallbacks: 4,
	}
	healthy := chaos
	healthy.FailRate, healthy.TimeoutRate, healthy.PendingRate = 0, 0, 0

	// Merchants, each funded with one clean sale so refunds have a balance to draw on.
	sim.SetConfig(healthy)
	admin := client{t: t, base: apiSrv.URL, auth: "admin"}
	var merchants []client
	for i := 0; i < 3; i++ {
		_, body := admin.do("POST", "/admin/merchants", "", map[string]any{"name": fmt.Sprint("Chaos ", i)})
		merchants = append(merchants, client{t: t, base: apiSrv.URL, auth: decode[payments.CreatedMerchant](t, body).APIKey})
	}

	// The worker runs throughout.
	w := &worker.Worker{Svc: svc, Clock: clk, SettlementEvery: 200 * time.Millisecond}
	var (
		workerErrs []error
		werrMu     sync.Mutex
		wg         sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			if _, err := w.RunOnce(ctx); err != nil && ctx.Err() == nil {
				werrMu.Lock()
				workerErrs = append(workerErrs, err)
				werrMu.Unlock()
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	var seq atomic.Int64
	// call retries a request with the same key until it gets a final answer.
	call := func(c client, method, path string, body any) (int, []byte) {
		key := fmt.Sprint("k-", seq.Add(1))
		for attempt := 0; ; attempt++ {
			code, out := c.do(method, path, key, body)
			if (code != 409 && code < 500) || attempt == 40 {
				return code, out
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	sale := func(c client, amount int64) (payments.PaymentView, bool) {
		_, body := call(c, "POST", "/v1/orders", map[string]any{"amount": amount})
		o := decode[payments.OrderView](t, body)
		code, body := call(c, "POST", "/v1/payments", map[string]any{"order_id": o.ID, "method": []string{"upi", "card"}[amount%2]})
		if code != 201 {
			t.Errorf("payment: %d %s", code, body)
			return payments.PaymentView{}, false
		}
		p := decode[payments.PaymentView](t, body)
		if p.State != payments.StateAuthorized {
			return p, false
		}
		if code, body := call(c, "POST", "/v1/payments/"+p.ID.String()+"/capture", nil); code != 200 {
			t.Errorf("capture: %d %s", code, body)
			return p, false
		}
		return p, true
	}
	for _, m := range merchants {
		if _, ok := sale(m, 500_000); !ok {
			t.Fatal("funding sale failed on a healthy bank")
		}
	}

	// Chaos: outages flicker on and off while customers pay and merchants refund.
	sim.SetConfig(chaos)
	stopFlicker := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopFlicker:
				return
			case <-time.After(600 * time.Millisecond):
			}
			c := chaos
			c.Outage = true
			sim.SetConfig(c)
			time.Sleep(150 * time.Millisecond)
			sim.SetConfig(chaos)
		}
	}()
	var clients sync.WaitGroup
	for g := 0; g < 8; g++ {
		clients.Add(1)
		go func(g int) {
			defer clients.Done()
			r := rand.New(rand.NewPCG(uint64(g), 9))
			for i := 0; i < 12; i++ {
				m := merchants[r.IntN(len(merchants))]
				p, ok := sale(m, 1_000+r.Int64N(50_000))
				if ok && r.IntN(10) < 4 {
					code, body := call(m, "POST", "/v1/payments/"+p.ID.String()+"/refunds", map[string]any{"amount": 1 + r.Int64N(p.Amount)})
					if code != 201 && code != 422 {
						t.Errorf("refund: %d %s", code, body)
					}
				}
			}
		}(g)
	}
	clients.Wait()
	close(stopFlicker)

	// Tomorrow: settlement pays merchants through the still-faulty bank.
	workday := clk.Now().In(payments.IST).Format("2006-01-02")
	clk.offset.Store(int64(24 * time.Hour))
	time.Sleep(3 * time.Second)

	// The bank recovers (callbacks still on); everything must drain.
	sim.SetConfig(healthy)
	deadline := time.Now().Add(45 * time.Second)
	for {
		var open, payouts int
		pool.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM payments WHERE state IN ('created', 'pending')) +
			(SELECT count(*) FROM refunds WHERE state = 'pending') +
			(SELECT count(*) FROM payouts WHERE state = 'pending')`).Scan(&open)
		pool.QueryRow(ctx, `SELECT count(*) FROM payouts`).Scan(&payouts)
		if open == 0 && payouts > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("did not drain: %d unresolved, %d payouts", open, payouts)
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond) // let straggling duplicate callbacks land
	cancel()
	wg.Wait()

	werrMu.Lock()
	for _, err := range workerErrs {
		t.Errorf("worker: %v", err)
	}
	werrMu.Unlock()

	stats := map[string]int{}
	for _, q := range []struct{ name, sql string }{
		{"payments", `SELECT count(*) FROM payments`},
		{"captured", `SELECT count(*) FROM payments WHERE state IN ('captured', 'refunded')`},
		{"failed", `SELECT count(*) FROM payments WHERE state = 'failed'`},
		{"late_successes", `SELECT count(*) FROM journal_entries WHERE kind = 'late_success'`},
		{"auto_refunds_processed", `SELECT count(*) FROM refunds WHERE kind = 'auto' AND state = 'processed'`},
		{"merchant_refunds", `SELECT count(*) FROM refunds WHERE kind = 'merchant'`},
		{"payouts_processed", `SELECT count(*) FROM payouts WHERE state = 'processed'`},
		{"payouts_failed", `SELECT count(*) FROM payouts WHERE state = 'failed'`},
	} {
		var n int
		pool.QueryRow(context.Background(), q.sql).Scan(&n)
		stats[q.name] = n
	}
	stats["callbacks_delivered"] = sim.Sent()
	// T+1: everything was earned during the workload (one day, or two if it
	// crossed midnight IST), so no payout may be for any other day. (This caught the READ COMMITTED snapshot bug in
	// eligibility; see TestSettlementIgnoresConcurrentCapture.)
	var early int
	pool.QueryRow(context.Background(), `SELECT count(*) FROM payouts WHERE settlement_date NOT BETWEEN $1::date AND $2::date`,
		startDay, workday).Scan(&early)
	if early != 0 {
		t.Errorf("%d payouts outside %s..%s", early, startDay, workday)
	}
	raw, _ := json.Marshal(stats)
	t.Logf("chaos run: %s", raw)
	if stats["late_successes"] == 0 || stats["failed"] == 0 || stats["payouts_processed"] == 0 || stats["callbacks_delivered"] == 0 {
		t.Errorf("the run did not exercise every path: %s", raw)
	}

	rep, err := invariants.CheckAt(context.Background(), pool, clk.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := rep.Err(); err != nil {
		t.Fatal(err)
	}
}
