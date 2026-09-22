// Package sim is Kosh's deterministic simulation harness.
//
// One seed drives everything nondeterministic: the workload, the id
// generator, the webhook jitter, a fake clock, and an in-process bank that
// declines, times out, answers "pending", goes down and sends late,
// duplicated callbacks. One goroutine runs a few dozen randomly chosen steps
// (pay, retry with the same key, capture, refund, deliver callbacks, advance
// time by seconds to hours, run the worker, relay and dispatcher, break the
// merchant's endpoint, crash at a named crashpoint and restart, reconcile,
// resolve exceptions) and checks the invariants after every step. At the end
// it turns the faults off, drains the system, and checks all eleven.
//
// Postgres is real (a fresh database per seed). With one goroutine nothing
// races, so the same seed replays the same run; every step is written to a
// trace whose hash proves it.
package sim

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"math/rand/v2"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/naman777/kosh/internal/banksim"
	"github.com/naman777/kosh/internal/bus"
	"github.com/naman777/kosh/internal/clock"
	"github.com/naman777/kosh/internal/crash"
	"github.com/naman777/kosh/internal/idempotency"
	"github.com/naman777/kosh/internal/ids"
	"github.com/naman777/kosh/internal/outbox"
	"github.com/naman777/kosh/internal/payments"
	"github.com/naman777/kosh/internal/recon"
	"github.com/naman777/kosh/internal/rng"
	"github.com/naman777/kosh/internal/webhooks"
	"github.com/naman777/kosh/internal/worker"
	"github.com/naman777/kosh/invariants"
)

// Config sets up a run.
type Config struct {
	Seed      uint64
	Steps     int // default 60
	Merchants int // default 3
	// Verbose collects the whole trace in the report.
	Verbose bool
}

// Report is the outcome of a run.
type Report struct {
	Seed      uint64         `json:"seed"`
	Steps     int            `json:"steps"`
	TraceHash string         `json:"trace_hash"`
	Counts    map[string]int `json:"counts"`
	Elapsed   time.Duration  `json:"elapsed"`
	Failure   *Failure       `json:"failure,omitempty"`
	Trace     []string       `json:"trace,omitempty"`
}

// Failure is what broke and where.
type Failure struct {
	Step   int    `json:"step"`
	Action string `json:"action"`
	Err    string `json:"error"`
	Tail   []string
}

// Failed reports whether the run broke an invariant or hit an error.
func (r Report) Failed() bool { return r.Failure != nil }

// Replay is the command that reruns this seed.
func (r Report) Replay() string { return fmt.Sprintf("go run ./cmd/sim -seed %d -v", r.Seed) }

// The simulated world starts on a Monday morning, IST.
var start = time.Date(2026, 11, 2, 9, 0, 0, 0, payments.IST)

// crashpoints the harness may arm.
var crashpoints = []string{
	"payments.create.after_insert", "payments.create.after_bank",
	"refunds.create.after_reserve", "refunds.create.after_bank",
	"payouts.after_hold", "payouts.after_bank",
	"relay.before_publish", "relay.after_publish",
	"dispatcher.after_ingest", "dispatcher.after_claim", "dispatcher.after_send",
}

type request struct {
	merchant uuid.UUID
	key      idempotency.Key
	call     func(context.Context, idempotency.Key) (payments.Response, error)
	first    []byte // the first final response; every replay must match it
}

type world struct {
	cfg      Config
	ctx      context.Context
	arms     *crash.Arms
	rng      *rand.Rand
	clk      *clock.Fake
	ids      *ids.Seeded
	pool     *pgxpool.Pool
	bank     *Bank
	svc      *payments.Service
	recon    *recon.Engine
	bus      *bus.Mem
	consumer bus.Consumer
	relay    *outbox.Relay
	disp     *webhooks.Dispatcher
	recvs    []*webhooks.Receiver
	worker   *worker.Worker

	merchants []uuid.UUID
	requests  []*request
	keySeq    int

	step   int
	action string
	trace  hash.Hash
	lines  []string
	counts map[string]int
}

// Run executes one seed against an empty, migrated database.
func Run(ctx context.Context, pool *pgxpool.Pool, cfg Config) (rep Report) {
	if cfg.Steps == 0 {
		cfg.Steps = 60
	}
	if cfg.Merchants == 0 {
		cfg.Merchants = 3
	}
	began := time.Now()
	rep = Report{Seed: cfg.Seed, Steps: cfg.Steps}
	w, err := newWorld(ctx, pool, cfg)
	if err == nil {
		err = w.run()
	}
	if w != nil {
		rep.TraceHash = hex.EncodeToString(w.trace.Sum(nil))[:16]
		rep.Counts = w.counts
		if cfg.Verbose {
			rep.Trace = w.lines
		}
	}
	if err != nil {
		f := &Failure{Err: err.Error()}
		if w != nil {
			f.Step, f.Action = w.step, w.action
			f.Tail = w.lines[max(0, len(w.lines)-25):]
		}
		rep.Failure = f
	}
	rep.Elapsed = time.Since(began)
	return rep
}

func newWorld(ctx context.Context, pool *pgxpool.Pool, cfg Config) (*world, error) {
	ctx, arms := crash.WithArms(ctx)
	clk := clock.NewFake(start)
	g := ids.NewSeeded(cfg.Seed)
	w := &world{cfg: cfg, ctx: ctx, arms: arms, clk: clk, ids: g, pool: pool,
		rng:   rand.New(rand.NewPCG(cfg.Seed, 0x5157)),
		bank:  NewBank(cfg.Seed, clk),
		trace: sha256.New(), counts: map[string]int{}}
	w.bank.Faults = settlementFaults(w.rng)

	var err error
	if w.svc, err = payments.New(ctx, payments.Deps{Pool: pool, Bank: w.bank, Clock: clk, IDs: g}); err != nil {
		return w, err
	}
	if w.recon, err = recon.New(ctx, pool, clk, g); err != nil {
		return w, err
	}
	w.bus = bus.NewMem()
	w.consumer = w.bus.Consumer("dispatcher")
	w.relay = &outbox.Relay{Pool: pool, Publisher: w.bus, Clock: clk}

	mux := http.NewServeMux()
	for i := 0; i < cfg.Merchants; i++ {
		url := fmt.Sprintf("http://merchant.sim/%d", i)
		m, err := w.svc.CreateMerchant(ctx, payments.CreateMerchantInput{
			Name: fmt.Sprintf("Sim Merchant %d", i), FeeBps: ptr(100 + 50*i), WebhookURL: &url})
		if err != nil {
			return w, err
		}
		recv := webhooks.NewReceiver(m.WebhookSecret, clk)
		mux.Handle(fmt.Sprintf("/%d", i), recv)
		w.recvs = append(w.recvs, recv)
		w.merchants = append(w.merchants, m.Merchant.ID)
	}
	w.disp = &webhooks.Dispatcher{Pool: pool, Clock: clk, RNG: rng.New(cfg.Seed), Sender: webhooks.HandlerSender{Handler: mux}}
	w.worker = &worker.Worker{Svc: w.svc, Clock: clk, Recon: w.recon, Settlements: w.bank, SettlementEvery: time.Minute}
	return w, nil
}

func settlementFaults(r *rand.Rand) banksim.SettlementFaults {
	if r.IntN(3) == 0 {
		return banksim.SettlementFaults{} // a clean bank, sometimes
	}
	return banksim.SettlementFaults{FeeVarianceRate: 0.05, MissingRate: 0.05, DuplicateRate: 0.05, MismatchRate: 0.05}
}

func ptr[T any](v T) *T { return &v }

// note appends a line to the trace.
func (w *world) note(format string, args ...any) {
	line := fmt.Sprintf("%03d %s: %s", w.step, w.action, fmt.Sprintf(format, args...))
	w.trace.Write([]byte(line))
	w.trace.Write([]byte{'\n'})
	w.lines = append(w.lines, line)
}

type action struct {
	name   string
	weight int
	fn     func() error
}

func (w *world) actions() []action {
	return []action{
		{"pay", 22, w.pay},
		{"retry", 10, w.retry},
		{"capture", 14, w.capture},
		{"refund", 9, w.refund},
		{"double_refund", 3, w.doubleRefund},
		{"callbacks", 10, w.callbacks},
		{"advance", 12, w.advance},
		{"worker", 8, w.workerRound},
		{"events", 7, w.events},
		{"merchant_flip", 2, w.merchantFlip},
		{"bank_mood", 3, w.bankMood},
		{"crash", 7, w.crashStep},
		{"resolve", 3, w.resolve},
	}
}

func pick(r *rand.Rand, as []action) action {
	total := 0
	for _, a := range as {
		total += a.weight
	}
	x := r.IntN(total)
	for _, a := range as {
		if x < a.weight {
			return a
		}
		x -= a.weight
	}
	return as[len(as)-1]
}

func (w *world) run() error {
	w.bankMood()
	acts := w.actions()
	for w.step = 1; w.step <= w.cfg.Steps; w.step++ {
		a := pick(w.rng, acts)
		w.action = a.name
		w.counts[a.name]++
		if err := a.fn(); err != nil {
			return err
		}
		if err := w.check(false); err != nil {
			return err
		}
	}
	w.action = "drain"
	if err := w.drain(); err != nil {
		return err
	}
	if err := w.check(true); err != nil {
		return err
	}
	return w.coverage()
}

// coverage records which interesting paths this run reached, so a soak can
// show it exercised them rather than passing by never getting there.
func (w *world) coverage() error {
	rows, err := w.pool.Query(w.ctx, `
		SELECT 'payment.' || state, count(*) FROM payments GROUP BY state
		UNION ALL SELECT 'refund.' || kind || '.' || state, count(*) FROM refunds GROUP BY kind, state
		UNION ALL SELECT 'payout.' || state, count(*) FROM payouts GROUP BY state
		UNION ALL SELECT 'recon.' || status, count(*) FROM recon_results GROUP BY status
		UNION ALL SELECT 'entry.late_success', count(*) FROM journal_entries WHERE kind = 'late_success'
		UNION ALL SELECT 'webhook.retried', count(*) FROM webhook_deliveries WHERE attempt > 1`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			k string
			n int
		)
		if err := rows.Scan(&k, &n); err != nil {
			return err
		}
		if n > 0 {
			w.counts["db."+k] = n
		}
	}
	return rows.Err()
}

// check runs the invariants. Invariant 8 ("eventually delivered") only holds
// once the system is quiescent, so it waits for the end.
func (w *world) check(final bool) error {
	which := []int{1, 2, 3, 4, 5, 6, 7, 9, 10, 11}
	if final {
		which = nil
	}
	rep, err := invariants.CheckAt(w.ctx, w.pool, w.clk.Now(), which...)
	if err != nil {
		return err
	}
	return rep.Err()
}

// ---- requests ---------------------------------------------------------------

func (w *world) newRequest(m uuid.UUID, path string, body any, call func(context.Context, idempotency.Key) (payments.Response, error)) *request {
	w.keySeq++
	raw, _ := json.Marshal(body)
	r := &request{merchant: m, call: call,
		key: idempotency.Key{MerchantID: m, Key: fmt.Sprintf("sim-%d", w.keySeq), Method: "POST", Path: path, Body: raw}}
	w.requests = append(w.requests, r)
	return r
}

// do sends a request. A crash restarts the "process"; a 409 (the key is
// still leased by a dead request) is fine, the client retries later. Any
// other error is a bug. A replay must return exactly the first response.
func (w *world) do(r *request) (payments.Response, bool, error) {
	var (
		resp payments.Response
		err  error
	)
	if point, crashed := crash.Recover(func() { resp, err = r.call(w.ctx, r.key) }); crashed {
		w.restart(point)
		w.note("%s crashed at %s", r.key.Key, point)
		return resp, false, nil
	}
	var perr *payments.Error
	if errors.As(err, &perr) && perr.Status == http.StatusConflict {
		w.note("%s 409 %s", r.key.Key, perr.Code)
		return resp, false, nil
	}
	if err != nil {
		return resp, false, fmt.Errorf("%s %s: %w", r.key.Path, r.key.Key, err)
	}
	if r.first == nil {
		r.first = resp.Body
	} else if !bytes.Equal(r.first, resp.Body) {
		return resp, false, fmt.Errorf("invariant 6: replay of %s returned different bytes:\n%s\n%s", r.key.Key, r.first, resp.Body)
	}
	w.note("%s %d", r.key.Key, resp.Status)
	return resp, true, nil
}

// restart forgets in-process state, as a restarted process would: armed
// crashes, and the bus consumer's uncommitted position.
func (w *world) restart(point string) {
	w.arms.Clear()
	w.consumer = w.bus.Consumer("dispatcher")
	w.counts["crashed"]++
	w.counts["crashed."+point]++
}

func (w *world) merchant() uuid.UUID { return w.merchants[w.rng.IntN(len(w.merchants))] }

func (w *world) pay() error {
	m := w.merchant()
	amount := 100 + w.rng.Int64N(200_000)
	o := w.newRequest(m, "/v1/orders", map[string]any{"amount": amount}, w.svc.CreateOrder)
	resp, ok, err := w.do(o)
	if err != nil || !ok || resp.Status != http.StatusCreated {
		return err
	}
	var order payments.OrderView
	if err := json.Unmarshal(resp.Body, &order); err != nil {
		return err
	}
	method := []string{"upi", "card"}[w.rng.IntN(2)]
	_, _, err = w.do(w.newRequest(m, "/v1/payments", map[string]any{"order_id": order.ID, "method": method}, w.svc.CreatePayment))
	return err
}

// retry resends an earlier request with the same key: the client never got
// (or doesn't trust) the answer.
func (w *world) retry() error {
	if len(w.requests) == 0 {
		return nil
	}
	_, _, err := w.do(w.requests[w.rng.IntN(len(w.requests))])
	return err
}

// payments returns ids of the payments in state, in id order.
func (w *world) paymentsIn(state payments.State) ([]uuid.UUID, []uuid.UUID, error) {
	rows, err := w.pool.Query(w.ctx, `SELECT id, merchant_id FROM payments WHERE state = $1 ORDER BY id`, state)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var ids, ms []uuid.UUID
	for rows.Next() {
		var id, m uuid.UUID
		if err := rows.Scan(&id, &m); err != nil {
			return nil, nil, err
		}
		ids, ms = append(ids, id), append(ms, m)
	}
	return ids, ms, rows.Err()
}

func (w *world) capture() error {
	ids, ms, err := w.paymentsIn(payments.StateAuthorized)
	if err != nil || len(ids) == 0 {
		return err
	}
	i := w.rng.IntN(len(ids))
	id := ids[i]
	_, _, err = w.do(w.newRequest(ms[i], "/v1/payments/"+id.String()+"/capture", map[string]any{},
		func(ctx context.Context, k idempotency.Key) (payments.Response, error) {
			return w.svc.Capture(ctx, k, id)
		}))
	return err
}

func (w *world) refundOne(id, m uuid.UUID) error {
	amount := 1 + w.rng.Int64N(150_000)
	_, _, err := w.do(w.newRequest(m, "/v1/payments/"+id.String()+"/refunds", map[string]any{"amount": amount},
		func(ctx context.Context, k idempotency.Key) (payments.Response, error) {
			return w.svc.CreateRefund(ctx, k, id)
		}))
	return err
}

func (w *world) refund() error {
	ids, ms, err := w.paymentsIn(payments.StateCaptured)
	if err != nil || len(ids) == 0 {
		return err
	}
	i := w.rng.IntN(len(ids))
	return w.refundOne(ids[i], ms[i])
}

func (w *world) doubleRefund() error {
	ids, ms, err := w.paymentsIn(payments.StateCaptured)
	if err != nil || len(ids) == 0 {
		return err
	}
	i := w.rng.IntN(len(ids))
	if err := w.refundOne(ids[i], ms[i]); err != nil {
		return err
	}
	return w.refundOne(ids[i], ms[i])
}

// ---- the outside world -------------------------------------------------------

func (w *world) callbacks() error {
	due := w.bank.Due()
	for _, cb := range due {
		if err := w.svc.HandleBankCallback(w.ctx, cb); err != nil {
			return fmt.Errorf("callback %s %s: %w", cb.Kind, cb.ID, err)
		}
	}
	w.note("%d delivered", len(due))
	return nil
}

var steps = []time.Duration{time.Second, 10 * time.Second, time.Minute, 5 * time.Minute, 30 * time.Minute, 2 * time.Hour, 8 * time.Hour}

func (w *world) advance() error {
	d := steps[w.rng.IntN(len(steps))]
	w.clk.Advance(d)
	w.note("+%s → %s", d, w.clk.Now().In(payments.IST).Format("Jan 02 15:04:05"))
	return nil
}

func (w *world) workerRound() error {
	var (
		st  worker.Stats
		err error
	)
	if point, crashed := crash.Recover(func() { st, err = w.worker.RunOnce(w.ctx) }); crashed {
		w.restart(point)
		w.note("crashed at %s", point)
		return nil
	}
	if err != nil {
		return fmt.Errorf("worker: %w", err)
	}
	w.note("payments=%d refunds=%d payouts=%d expired=%d settled=%v reconciled=%v",
		st.Payments, st.Refunds, st.Payouts, st.Expired, st.Settled != nil, st.Reconciled != nil)
	return nil
}

func (w *world) events() error {
	var (
		published, ingested, sent int
		err                       error
	)
	point, crashed := crash.Recover(func() {
		for i := 0; i < 20; i++ {
			var n int
			if n, err = w.relay.RunOnce(w.ctx); err != nil {
				err = fmt.Errorf("relay: %w", err)
				return
			}
			published += n
			if n == 0 {
				break
			}
		}
		// Poll only while there is something to poll: a blocking Poll with a
		// wall-clock timeout would make the run depend on timing.
		for w.consumer.(interface{ Lag() int }).Lag() > 0 {
			n, ierr := w.disp.IngestOnce(w.ctx, w.consumer)
			if ierr != nil {
				err = fmt.Errorf("ingest: %w", ierr)
				return
			}
			ingested += n
		}
		if sent, err = w.disp.SendOnce(w.ctx); err != nil {
			err = fmt.Errorf("send: %w", err)
		}
	})
	switch {
	case crashed:
		w.restart(point)
		w.note("crashed at %s", point)
		return nil
	case err != nil:
		return err
	}
	w.note("published=%d ingested=%d sent=%d", published, ingested, sent)
	return nil
}

func (w *world) merchantFlip() error {
	i := w.rng.IntN(len(w.recvs))
	mode := webhooks.ModeOK
	if w.recvs[i].Stats().Mode == webhooks.ModeOK {
		mode = webhooks.ModeFail
	}
	w.recvs[i].SetMode(mode)
	w.note("merchant %d endpoint %s", i, mode)
	return nil
}

func (w *world) bankMood() error {
	b := w.bank
	b.mu.Lock()
	b.FailRate = []float64{0, 0.05, 0.2}[w.rng.IntN(3)]
	b.TimeoutRate = []float64{0, 0.05, 0.2}[w.rng.IntN(3)]
	b.PendingRate = []float64{0, 0.1, 0.3}[w.rng.IntN(3)]
	b.Outage = w.rng.IntN(6) == 0
	note := fmt.Sprintf("fail=%.2f timeout=%.2f pending=%.2f outage=%v", b.FailRate, b.TimeoutRate, b.PendingRate, b.Outage)
	b.mu.Unlock()
	w.note("%s", note)
	return nil
}

// crashStep arms a random crashpoint and runs the action that reaches it.
// If that action doesn't get there this time, the point stays armed: the
// process will die whenever the code next passes it, until a restart.
func (w *world) crashStep() error {
	point := crashpoints[w.rng.IntN(len(crashpoints))]
	w.arms.Arm(point, 1)
	w.note("armed %s", point)
	switch {
	case strings.HasPrefix(point, "payments."):
		return w.pay()
	case strings.HasPrefix(point, "refunds."):
		return w.refund()
	case strings.HasPrefix(point, "payouts."):
		return w.workerRound()
	default:
		return w.events()
	}
}

func (w *world) resolve() error {
	exc, err := w.recon.Exceptions(w.ctx, 50)
	if err != nil || len(exc) == 0 {
		return err
	}
	x := exc[w.rng.IntN(len(exc))]
	action := recon.WriteOff
	if x.Status != recon.MissingInBank && w.rng.IntN(2) == 0 {
		action = recon.BankCorrected
	}
	if _, err := w.recon.Resolve(w.ctx, x.ID, action); err != nil {
		return fmt.Errorf("resolve %d (%s) as %s: %w", x.ID, x.Status, action, err)
	}
	w.note("%s %d → %s", x.Status, x.ID, action)
	return nil
}

// drain turns the faults off and runs the world forward until nothing is
// left to do: every request retried, every answer delivered, every payout
// made, every file reconciled, every webhook delivered.
func (w *world) drain() error {
	w.arms.Clear()
	w.bank.mu.Lock()
	w.bank.FailRate, w.bank.TimeoutRate, w.bank.PendingRate, w.bank.Outage = 0, 0, 0, false
	w.bank.mu.Unlock()
	for _, r := range w.recvs {
		r.SetMode(webhooks.ModeOK)
	}
	began := w.clk.Now()
	for round := 0; round < 200; round++ {
		w.clk.Advance(time.Hour)
		if err := w.callbacks(); err != nil {
			return err
		}
		for _, r := range w.requests {
			if r.first == nil {
				if _, _, err := w.do(r); err != nil {
					return err
				}
			}
		}
		if err := w.workerRound(); err != nil {
			return err
		}
		for i := 0; i < 3; i++ {
			if err := w.events(); err != nil {
				return err
			}
		}
		for _, m := range w.merchants {
			var disabled bool
			if err := w.pool.QueryRow(w.ctx, `SELECT webhook_disabled_at IS NOT NULL FROM merchants WHERE id = $1`, m).Scan(&disabled); err != nil {
				return err
			}
			if disabled {
				w.counts["webhook_replays"]++
				if _, _, err := w.do(w.newRequest(m, "/v1/webhooks/replay", map[string]any{}, w.svc.ReplayWebhooks)); err != nil {
					return err
				}
			}
		}
		idle, err := w.quiescent()
		if err != nil {
			return err
		}
		if idle && w.clk.Now().Sub(began) >= 50*time.Hour {
			w.note("quiescent after %d rounds", round+1)
			return nil
		}
	}
	return errors.New("did not drain in 200 simulated hours")
}

func (w *world) quiescent() (bool, error) {
	var open int
	err := w.pool.QueryRow(w.ctx, `SELECT
		(SELECT count(*) FROM payments WHERE state IN ('created', 'pending')) +
		(SELECT count(*) FROM refunds WHERE state = 'pending') +
		(SELECT count(*) FROM payouts WHERE state = 'pending') +
		(SELECT count(*) FROM outbox WHERE published_at IS NULL) +
		(SELECT count(*) FROM webhook_deliveries WHERE delivered_at IS NULL)`).Scan(&open)
	if err != nil {
		return false, err
	}
	pending := 0
	for _, r := range w.requests {
		if r.first == nil {
			pending++
		}
	}
	return open == 0 && pending == 0, nil
}

// Summary is a one-line description of a report.
func (r Report) Summary() string {
	keys := make([]string, 0, len(r.Counts))
	for k := range r.Counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, r.Counts[k]))
	}
	status := "ok"
	if r.Failed() {
		status = "FAIL"
	}
	return fmt.Sprintf("seed %d %s in %s, trace %s [%s]", r.Seed, status, r.Elapsed.Round(time.Millisecond), r.TraceHash, strings.Join(parts, " "))
}
