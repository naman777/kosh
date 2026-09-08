package webhooks_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/naman777/kosh/internal/bank"
	"github.com/naman777/kosh/internal/bus"
	"github.com/naman777/kosh/internal/clock"
	"github.com/naman777/kosh/internal/crash"
	"github.com/naman777/kosh/internal/idempotency"
	"github.com/naman777/kosh/internal/outbox"
	"github.com/naman777/kosh/internal/payments"
	"github.com/naman777/kosh/internal/rng"
	"github.com/naman777/kosh/internal/testdb"
	"github.com/naman777/kosh/internal/webhooks"
	"github.com/naman777/kosh/invariants"
)

// pipe is the whole event path in one process: payments → outbox → relay →
// bus → dispatcher → receiver, all on a fake clock.
type pipe struct {
	t        *testing.T
	ctx      context.Context
	pool     *pgxpool.Pool
	clk      *clock.Fake
	svc      *payments.Service
	bus      *bus.Mem
	consumer bus.Consumer
	relay    *outbox.Relay
	disp     *webhooks.Dispatcher
	recv     *webhooks.Receiver
	merchant uuid.UUID
	seq      int
}

func newPipe(t *testing.T) *pipe {
	t.Helper()
	t.Cleanup(crash.Disarm)
	ctx := context.Background()
	pool := testdb.New(t)
	clk := clock.NewFake(time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC))
	svc, err := payments.New(ctx, payments.Deps{Pool: pool, Bank: bank.NewFake(), Clock: clk,
		Config: payments.Config{BankTimeout: time.Second, RefundHoldTTL: 72 * time.Hour}})
	if err != nil {
		t.Fatal(err)
	}
	url := "http://merchant.test/webhooks"
	m, err := svc.CreateMerchant(ctx, payments.CreateMerchantInput{Name: "Webhook Merchant", WebhookURL: &url})
	if err != nil {
		t.Fatal(err)
	}
	b := bus.NewMem()
	recv := webhooks.NewReceiver(m.WebhookSecret, clk)
	return &pipe{
		t: t, ctx: ctx, pool: pool, clk: clk, svc: svc, bus: b, consumer: b.Consumer("dispatcher"),
		relay: &outbox.Relay{Pool: pool, Publisher: b, Clock: clk},
		disp: &webhooks.Dispatcher{Pool: pool, Clock: clk, RNG: rng.New(7), Sender: webhooks.HandlerSender{Handler: recv},
			SendTimeout: 50 * time.Millisecond},
		recv: recv, merchant: m.Merchant.ID,
	}
}

func (p *pipe) call(fn func(context.Context, idempotency.Key) (payments.Response, error), path string, body any) []byte {
	p.t.Helper()
	p.seq++
	raw, _ := json.Marshal(body)
	r, err := fn(p.ctx, idempotency.Key{MerchantID: p.merchant, Key: fmt.Sprint("k", p.seq), Method: "POST", Path: path, Body: raw})
	if err != nil {
		p.t.Fatal(err)
	}
	if r.Status >= 300 {
		p.t.Fatalf("%s: %d %s", path, r.Status, r.Body)
	}
	return r.Body
}

// sale runs order → payment → capture and returns the payment id.
func (p *pipe) sale(amount int64) uuid.UUID {
	p.t.Helper()
	var o payments.OrderView
	json.Unmarshal(p.call(p.svc.CreateOrder, "/v1/orders", map[string]any{"amount": amount}), &o)
	var pay payments.PaymentView
	json.Unmarshal(p.call(p.svc.CreatePayment, "/v1/payments", map[string]any{"order_id": o.ID, "method": "upi"}), &pay)
	path := "/v1/payments/" + pay.ID.String() + "/capture"
	p.call(func(ctx context.Context, k idempotency.Key) (payments.Response, error) {
		return p.svc.Capture(ctx, k, pay.ID)
	}, path, map[string]any{})
	return pay.ID
}

func (p *pipe) refund(id uuid.UUID, amount int64) {
	p.t.Helper()
	path := "/v1/payments/" + id.String() + "/refunds"
	p.call(func(ctx context.Context, k idempotency.Key) (payments.Response, error) {
		return p.svc.CreateRefund(ctx, k, id)
	}, path, map[string]any{"amount": amount})
}

// workload makes 10 sales and 3 refunds: 3 events per sale, 2 per refund.
func (p *pipe) workload() {
	p.t.Helper()
	var ids []uuid.UUID
	for i := 0; i < 10; i++ {
		ids = append(ids, p.sale(int64(10_000+i*1_000)))
	}
	for _, id := range ids[:3] {
		p.refund(id, 1_000)
	}
}

func (p *pipe) ingestOnce() int {
	ctx, cancel := context.WithTimeout(p.ctx, 20*time.Millisecond)
	defer cancel()
	n, err := p.disp.IngestOnce(ctx, p.consumer)
	if err != nil && ctx.Err() == nil {
		p.t.Fatal(err)
	}
	return n
}

// drain runs every stage until nothing moves.
func (p *pipe) drain() {
	p.t.Helper()
	for round := 0; round < 200; round++ {
		moved := 0
		for {
			n, err := p.relay.RunOnce(p.ctx)
			if err != nil {
				p.t.Fatal(err)
			}
			if n == 0 {
				break
			}
			moved += n
		}
		moved += p.ingestOnce()
		n, err := p.disp.SendOnce(p.ctx)
		if err != nil {
			p.t.Fatal(err)
		}
		moved += n
		if moved == 0 {
			return
		}
	}
	p.t.Fatal("pipeline did not drain")
}

func (p *pipe) events() int {
	var n int
	p.pool.QueryRow(p.ctx, `SELECT count(*) FROM outbox`).Scan(&n)
	return n
}

func (p *pipe) checkInvariants() {
	p.t.Helper()
	rep, err := invariants.CheckAt(p.ctx, p.pool, p.clk.Now())
	if err != nil {
		p.t.Fatal(err)
	}
	if err := rep.Err(); err != nil {
		p.t.Fatal(err)
	}
}

func TestPipelineDeliversEverything(t *testing.T) {
	p := newPipe(t)
	p.workload()
	p.drain()
	s := p.recv.Stats()
	if want := p.events(); want != 36 || s.Unique != want {
		t.Fatalf("receiver saw %d unique events, outbox has %d (want 36)", s.Unique, p.events())
	}
	if s.Duplicates != 0 || s.Rejected != 0 {
		t.Fatalf("healthy run: %+v", s)
	}
	p.checkInvariants()
}

// TestRelayCrashAfterPublish kills the relay after Kafka acked a batch but
// before the rows were marked published. On restart the batch is published
// again; the dispatcher dedupes it and the merchant sees each event once.
func TestRelayCrashAfterPublish(t *testing.T) {
	p := newPipe(t)
	p.workload()
	crash.Arm("relay.after_publish", 1, nil)
	if _, crashed := crash.Recover(func() { p.relay.RunOnce(p.ctx) }); !crashed {
		t.Fatal("relay did not crash")
	}
	var unpublished int
	p.pool.QueryRow(p.ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&unpublished)
	if unpublished != p.events() || p.bus.Len() == 0 {
		t.Fatalf("after crash: %d unpublished, %d on the bus", unpublished, p.bus.Len())
	}

	p.drain() // the "restarted" relay
	if p.bus.Len() <= p.events() {
		t.Fatalf("expected duplicate publishes, bus has %d for %d events", p.bus.Len(), p.events())
	}
	var deliveries int
	p.pool.QueryRow(p.ctx, `SELECT count(*) FROM webhook_deliveries`).Scan(&deliveries)
	if s := p.recv.Stats(); deliveries != p.events() || s.Unique != p.events() || s.Duplicates != 0 {
		t.Fatalf("%d deliveries, receiver %+v, for %d events", deliveries, s, p.events())
	}
	p.checkInvariants()
}

// TestDispatcherCrashAfterIngest kills the consumer after writing deliveries
// but before committing offsets. The restarted consumer gets the batch again.
func TestDispatcherCrashAfterIngest(t *testing.T) {
	p := newPipe(t)
	p.workload()
	for n, _ := p.relay.RunOnce(p.ctx); n > 0; n, _ = p.relay.RunOnce(p.ctx) {
	}
	crash.Arm("dispatcher.after_ingest", 1, nil)
	if _, crashed := crash.Recover(func() { p.ingestOnce() }); !crashed {
		t.Fatal("dispatcher did not crash")
	}
	p.consumer = p.bus.Consumer("dispatcher") // restart: resume from the last commit
	p.drain()
	if s := p.recv.Stats(); s.Unique != p.events() || s.Duplicates != 0 {
		t.Fatalf("receiver %+v for %d events", s, p.events())
	}
	p.checkInvariants()
}

// TestDispatcherCrashAfterSend kills the sender after the merchant got the
// events but before they were recorded as delivered. Once the claim lease
// runs out they are sent again: duplicates, never losses.
func TestDispatcherCrashAfterSend(t *testing.T) {
	p := newPipe(t)
	p.workload()
	for n, _ := p.relay.RunOnce(p.ctx); n > 0; n, _ = p.relay.RunOnce(p.ctx) {
	}
	for p.ingestOnce() > 0 {
	}
	p.disp.BatchSize = 1000
	crash.Arm("dispatcher.after_send", 1, nil)
	if _, crashed := crash.Recover(func() { p.disp.SendOnce(p.ctx) }); !crashed {
		t.Fatal("dispatcher did not crash")
	}
	if s := p.recv.Stats(); s.Unique != p.events() {
		t.Fatalf("merchant should already have everything: %+v", s)
	}
	// Invariant 8 is violated until the retry: the merchant has them, we don't know it.
	rep, _ := invariants.CheckAt(p.ctx, p.pool, p.clk.Now(), 8)
	if rep.OK() {
		t.Fatal("invariant 8 should flag the unrecorded deliveries before recovery")
	}

	p.clk.Advance(time.Second) // past the 2 × send-timeout lease
	p.drain()
	s := p.recv.Stats()
	if s.Unique != p.events() || s.Duplicates != p.events() {
		t.Fatalf("want every event exactly twice, got %+v for %d events", s, p.events())
	}
	p.checkInvariants()
}

// TestMerchantDown26Hours: the endpoint returns 500 for 26 hours. Retries
// back off, the merchant is disabled at 24 hours, and replay delivers
// everything once it is back.
func TestMerchantDown26Hours(t *testing.T) {
	p := newPipe(t)
	p.recv.SetMode(webhooks.ModeFail)
	p.workload()
	start := p.clk.Now()

	var disabledAt *time.Time
	attemptsBefore := 0
	for p.clk.Now().Sub(start) < 26*time.Hour {
		p.drain()
		p.clk.Advance(10 * time.Minute)
	}
	p.pool.QueryRow(p.ctx, `SELECT webhook_disabled_at FROM merchants WHERE id = $1`, p.merchant).Scan(&disabledAt)
	if disabledAt == nil {
		t.Fatal("merchant not disabled after 26 hours of failures")
	}
	if d := disabledAt.Sub(start); d < 24*time.Hour || d > 25*time.Hour {
		t.Fatalf("disabled after %v, want just past 24h", d)
	}
	p.pool.QueryRow(p.ctx, `SELECT max(attempt) FROM webhook_deliveries`).Scan(&attemptsBefore)
	if attemptsBefore < 10 || attemptsBefore > 40 {
		t.Fatalf("max attempts %d: backoff looks wrong", attemptsBefore)
	}
	// Disabled merchants are outside invariant 8; everything else holds.
	p.checkInvariants()

	// Back up: nothing flows until the merchant asks for a replay.
	p.recv.SetMode(webhooks.ModeOK)
	p.clk.Advance(2 * time.Hour)
	p.drain()
	if s := p.recv.Stats(); s.Unique != 0 {
		t.Fatalf("delivered to a disabled merchant: %+v", s)
	}
	var res payments.ReplayResult
	json.Unmarshal(p.call(p.svc.ReplayWebhooks, "/v1/webhooks/replay", map[string]any{}), &res)
	if res.Replayed != int64(p.events()) {
		t.Fatalf("replayed %d, want %d", res.Replayed, p.events())
	}
	p.drain()
	if s := p.recv.Stats(); s.Unique != p.events() {
		t.Fatalf("after replay: %+v for %d events", s, p.events())
	}
	p.checkInvariants()
}

// TestHangingMerchant: an endpoint that never answers is a failure after
// the send timeout, and is retried.
func TestHangingMerchant(t *testing.T) {
	p := newPipe(t)
	p.recv.SetMode(webhooks.ModeHang)
	p.sale(5_000)
	p.drain()
	var attempts int
	var lastErr *string
	p.pool.QueryRow(p.ctx, `SELECT max(attempt), max(last_error) FROM webhook_deliveries`).Scan(&attempts, &lastErr)
	if attempts != 1 || lastErr == nil {
		t.Fatalf("attempts %d, last error %v", attempts, lastErr)
	}
	p.recv.SetMode(webhooks.ModeOK)
	p.clk.Advance(15 * time.Second)
	p.drain()
	if s := p.recv.Stats(); s.Unique != p.events() {
		t.Fatalf("receiver %+v", s)
	}
	p.checkInvariants()
}

// TestReplayRange re-sends events that were already delivered.
func TestReplayRange(t *testing.T) {
	p := newPipe(t)
	p.sale(7_000)
	p.drain()
	var lo, hi int64
	p.pool.QueryRow(p.ctx, `SELECT min(id), max(id) FROM outbox`).Scan(&lo, &hi)
	var res payments.ReplayResult
	json.Unmarshal(p.call(p.svc.ReplayWebhooks, "/v1/webhooks/replay", map[string]any{"from_event_id": lo, "to_event_id": hi}), &res)
	if res.Replayed != hi-lo+1 {
		t.Fatalf("replayed %d", res.Replayed)
	}
	p.drain()
	if s := p.recv.Stats(); s.Unique != p.events() || s.Duplicates != p.events() {
		t.Fatalf("receiver %+v", s)
	}
	p.checkInvariants()
}
