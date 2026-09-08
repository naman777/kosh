package banksim_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/naman777/kosh/internal/bank"
	"github.com/naman777/kosh/internal/banksim"
	"github.com/naman777/kosh/internal/clock"
	"github.com/naman777/kosh/internal/money"
)

var t0 = time.Date(2026, 11, 9, 10, 0, 0, 0, banksim.IST)

func start(t *testing.T, cfg banksim.Config, clk clock.Clock) (*banksim.Server, *bank.HTTPClient) {
	t.Helper()
	sim := banksim.NewWithClock(cfg, 42, clk)
	srv := httptest.NewServer(sim)
	t.Cleanup(srv.Close)
	return sim, bank.NewHTTPClient(srv.URL)
}

func TestDedupeByID(t *testing.T) {
	_, c := start(t, banksim.Config{FailRate: 0.5}, clock.NewFake(t0))
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		id := uuid.New()
		first, err := c.Authorize(ctx, bank.AuthorizeRequest{PaymentID: id, Amount: 100, Method: "upi"})
		if err != nil {
			t.Fatal(err)
		}
		again, _ := c.Authorize(ctx, bank.AuthorizeRequest{PaymentID: id, Amount: 100, Method: "upi"})
		status, _ := c.Status(ctx, id)
		if again != first || status != first {
			t.Fatalf("answers differ: %+v %+v %+v", first, again, status)
		}
	}
}

func TestOutageRecordsNothing(t *testing.T) {
	sim, c := start(t, banksim.Config{Outage: true}, clock.NewFake(t0))
	ctx := context.Background()
	id := uuid.New()
	if _, err := c.Authorize(ctx, bank.AuthorizeRequest{PaymentID: id, Amount: 100}); !errors.Is(err, bank.ErrUnknownOutcome) {
		t.Fatalf("outage: %v", err)
	}
	sim.SetConfig(banksim.Config{})
	if r, _ := c.Status(ctx, id); r.Status != bank.Failed || r.Reason != bank.ReasonUnknownID {
		t.Fatalf("after outage the bank should not know the id: %+v", r)
	}
}

func TestPendingThenFinal(t *testing.T) {
	clk := clock.NewFake(t0)
	_, c := start(t, banksim.Config{PendingRate: 1, FinalAfterMs: 60_000}, clk)
	ctx := context.Background()
	id := uuid.New()
	r, err := c.Authorize(ctx, bank.AuthorizeRequest{PaymentID: id, Amount: 100, Method: "upi"})
	if err != nil || r.Status != bank.Pending {
		t.Fatalf("first answer %+v %v", r, err)
	}
	clk.Advance(30 * time.Second)
	if r, _ := c.Status(ctx, id); r.Status != bank.Pending {
		t.Fatalf("before final_at: %+v", r)
	}
	clk.Advance(31 * time.Second)
	if r, _ := c.Status(ctx, id); r.Status == bank.Pending {
		t.Fatalf("after final_at still pending")
	}
}

func TestDuplicateCallbacks(t *testing.T) {
	var (
		mu    sync.Mutex
		got   = map[uuid.UUID]int{}
		bad   int
		total int
	)
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		defer mu.Unlock()
		total++
		if !bank.VerifyCallback("cb-secret", r.Header.Get(bank.HeaderCallbackSignature), body) {
			bad++
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var cb bank.Callback
		json.Unmarshal(body, &cb)
		got[cb.ID]++
	}))
	defer hook.Close()

	sim, c := start(t, banksim.Config{CallbackURL: hook.URL, CallbackSecret: "cb-secret", CallbackDelayMs: 10, MaxDuplicateCallbacks: 5}, clock.Real{})
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		c.Authorize(ctx, bank.AuthorizeRequest{PaymentID: uuid.New(), Amount: 100, Method: "upi"})
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n == 20 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	dups := 0
	for _, n := range got {
		if n < 1 || n > 5 {
			t.Fatalf("callback count %d outside 1..5", n)
		}
		dups += n - 1
	}
	if len(got) != 20 || bad != 0 || dups == 0 || sim.Sent() != total {
		t.Fatalf("ids=%d bad=%d dups=%d sent=%d total=%d", len(got), bad, dups, sim.Sent(), total)
	}
}

func TestSettlementFile(t *testing.T) {
	clk := clock.NewFake(t0)
	sim, c := start(t, banksim.Config{Settlement: banksim.SettlementFaults{
		FeeVarianceRate: 0.05, MissingRate: 0.05, DuplicateRate: 0.05, MismatchRate: 0.05,
	}}, clk)
	ctx := context.Background()
	payments := 0
	for i := 0; i < 400; i++ {
		method := "upi"
		if i%3 == 0 {
			method = "card"
		}
		id := uuid.New()
		r, _ := c.Authorize(ctx, bank.AuthorizeRequest{PaymentID: id, Amount: money.Paise(10_000 + i), Method: method})
		payments++
		if i%10 == 0 {
			c.Refund(ctx, bank.RefundRequest{RefundID: uuid.New(), PaymentBankRef: r.BankRef, Amount: 500})
		}
	}
	// Something decided tomorrow is not in today's file.
	clk.Advance(24 * time.Hour)
	c.Authorize(ctx, bank.AuthorizeRequest{PaymentID: uuid.New(), Amount: 1, Method: "upi"})

	lines, faults, err := sim.Settlement("2026-11-09")
	if err != nil {
		t.Fatal(err)
	}
	again, faults2, _ := sim.Settlement("2026-11-09")
	if len(again) != len(lines) || len(faults2) != len(faults) {
		t.Fatal("settlement file is not deterministic")
	}
	kinds := map[string]int{}
	for _, f := range faults {
		kinds[f.Kind]++
	}
	const refunds = 40
	if want := payments + refunds - kinds["missing"] + kinds["duplicate"]; len(lines) != want {
		t.Fatalf("%d lines, want %d (faults %v)", len(lines), want, kinds)
	}
	for _, k := range []string{"missing", "duplicate", "amount_mismatch", "fee_variance"} {
		if kinds[k] == 0 {
			t.Errorf("no %s faults injected in 440 lines", k)
		}
	}
	for _, l := range lines {
		if l.Net != l.Gross-l.Fee-l.Tax || l.ValueDate != "2026-11-10" {
			t.Fatalf("bad line %+v", l)
		}
	}
}
