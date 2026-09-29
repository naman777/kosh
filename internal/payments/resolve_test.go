package payments_test

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/naman777/kosh/internal/bank"
	"github.com/naman777/kosh/internal/crash"
	"github.com/naman777/kosh/internal/ledger"
	"github.com/naman777/kosh/internal/money"
	"github.com/naman777/kosh/internal/payments"
)

func (e *env) payment(id uuid.UUID) payments.PaymentView {
	e.t.Helper()
	p, err := e.svc.GetPayment(e.ctx, e.m, id)
	if err != nil {
		e.t.Fatal(err)
	}
	return p
}

func (e *env) pollPayments() int {
	e.t.Helper()
	n, err := e.svc.PollPayments(e.ctx, 100)
	if err != nil {
		e.t.Fatal(err)
	}
	return n
}

func (e *env) pollRefunds() int {
	e.t.Helper()
	n, err := e.svc.PollRefunds(e.ctx, 100)
	if err != nil {
		e.t.Fatal(err)
	}
	return n
}

// system returns a system account's posted balance and pending debits.
func (e *env) system(code string) ledger.Account {
	e.t.Helper()
	a, err := e.svc.Ledger().GetSystemAccount(e.ctx, e.pool, code)
	if err != nil {
		e.t.Fatal(err)
	}
	return a
}

func (e *env) events(aggregate uuid.UUID) []string {
	e.t.Helper()
	rows, err := e.pool.Query(e.ctx, `SELECT event_type FROM outbox WHERE aggregate_id = $1 ORDER BY aggregate_version`, aggregate)
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		rows.Scan(&s)
		out = append(out, s)
	}
	return out
}

func (e *env) callback(kind string, id uuid.UUID, r bank.Result) {
	e.t.Helper()
	if err := e.svc.HandleBankCallback(e.ctx, bank.Callback{Kind: kind, ID: id, Status: r.Status, BankRef: r.BankRef, Reason: r.Reason}); err != nil {
		e.t.Fatal(err)
	}
}

// TestPollerResolvesTimeout: the bank took the money but the reply timed
// out. The payment waits in pending until the poller asks.
func TestPollerResolvesTimeout(t *testing.T) {
	e := newEnv(t)
	e.bank.Next(bank.OutcomeTimeout)
	p := e.pay(e.createOrder(10_000).ID)
	if p.State != payments.StatePending {
		t.Fatalf("state %s", p.State)
	}
	if n := e.pollPayments(); n != 0 {
		t.Fatalf("polled %d before it was due", n)
	}
	e.clk.Advance(6 * time.Second)
	if n := e.pollPayments(); n != 1 {
		t.Fatalf("polled %d", n)
	}
	if got := e.payment(p.ID); got.State != payments.StateAuthorized || got.BankRef == nil {
		t.Fatalf("after poll: %+v", got)
	}
	e.checkInvariants()
}

// TestOutageThenPoll: the bank was down and never saw the request. The
// poller learns that and fails the payment; nothing moved.
func TestOutageThenPoll(t *testing.T) {
	e := newEnv(t)
	e.bank.Next(bank.OutcomeDown)
	p := e.pay(e.createOrder(10_000).ID)
	e.clk.Advance(6 * time.Second)
	e.pollPayments()
	got := e.payment(p.ID)
	if got.State != payments.StateFailed || *got.FailureReason != "not_received_by_bank" {
		t.Fatalf("after poll: %+v", got)
	}
	e.checkInvariants()
}

// TestLateSuccessAutoRefund is the nastiest UPI case. The bank sits on the
// payment past Kosh's 30-minute limit, Kosh fails it, and then the bank
// says success, five times. The customer must get the money back exactly
// once, and the ledger must end where it started.
func TestLateSuccessAutoRefund(t *testing.T) {
	e := newEnv(t)
	e.bank.Next(bank.OutcomeTimeoutPending)
	p := e.pay(e.createOrder(25_000).ID)

	// Polls while the bank says pending change nothing.
	for i := 0; i < 5; i++ {
		e.clk.Advance(5 * time.Minute)
		e.pollPayments()
	}
	if got := e.payment(p.ID); got.State != payments.StatePending {
		t.Fatalf("at 25 min: %s", got.State)
	}
	e.clk.Advance(6 * time.Minute)
	e.pollPayments()
	if got := e.payment(p.ID); got.State != payments.StateFailed || *got.FailureReason != "bank_timeout" {
		t.Fatalf("at 31 min: %+v", got)
	}

	// The bank finally succeeds and says so five times, concurrently.
	final := e.bank.Settle(p.ID, bank.Result{Status: bank.Success})
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); e.callback(bank.KindPayment, p.ID, final) }()
	}
	wg.Wait()

	got := e.payment(p.ID)
	if got.State != payments.StateFailed || got.BankRef == nil || *got.BankRef != final.BankRef {
		t.Fatalf("after late success: %+v", got)
	}
	var autoRefunds, lateEntries int
	e.pool.QueryRow(e.ctx, `SELECT count(*) FROM refunds WHERE payment_id = $1 AND kind = 'auto'`, p.ID).Scan(&autoRefunds)
	e.pool.QueryRow(e.ctx, `SELECT count(*) FROM journal_entries WHERE kind = 'late_success' AND ref_id = $1`, p.ID).Scan(&lateEntries)
	if autoRefunds != 1 || lateEntries != 1 {
		t.Fatalf("%d auto refunds, %d late_success entries, want 1 each", autoRefunds, lateEntries)
	}
	if a := e.system(ledger.CustomerRefundPayable); a.Posted() != 25_000 || a.PendingDr != 25_000 || a.Available() != 0 {
		t.Fatalf("customer_refund_payable before refund: %+v", a)
	}
	e.checkInvariants()

	// The refund poller submits the auto refund to the bank.
	e.pollRefunds()
	if a := e.system(ledger.CustomerRefundPayable); a.Posted() != 0 || a.PendingDr != 0 {
		t.Fatalf("customer_refund_payable after refund: %+v", a)
	}
	if a := e.system(ledger.BankReceivable); a.Posted() != 0 {
		t.Fatalf("bank_receivable %v, want 0: the money came in and went straight back", a.Posted())
	}
	var refundID uuid.UUID
	e.pool.QueryRow(e.ctx, `SELECT id FROM refunds WHERE payment_id = $1`, p.ID).Scan(&refundID)
	if ev := e.events(refundID); len(ev) != 2 || ev[0] != "refund.created" || ev[1] != "refund.processed" {
		t.Fatalf("refund events %v", ev)
	}
	e.checkInvariants()
}

// TestAbandonedRequest: the client's request dies right after the payment
// row is written, before the bank is asked. The poller fails it once the
// grace period passes; the client's retry then gets "failed" and the bank
// is never charged.
func TestAbandonedRequest(t *testing.T) {
	e := newEnv(t)
	o := e.createOrder(9_900)
	k := e.payKey("abandoned", o.ID)
	crash.Arm("payments.create.after_insert", 1, nil)
	crash.Recover(func() { e.svc.CreatePayment(e.ctx, k) })

	e.clk.Advance(time.Minute)
	if e.pollPayments() != 0 {
		t.Fatal("poller touched a request inside its grace period")
	}
	e.clk.Advance(2 * time.Minute)
	e.pollPayments()

	r, err := e.svc.CreatePayment(e.ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	p := mustDecode[payments.PaymentView](t, r, 201)
	if p.State != payments.StateFailed || e.bank.Calls.Load() != 0 {
		t.Fatalf("state %s, bank calls %d", p.State, e.bank.Calls.Load())
	}
	// The order is free for a new attempt.
	if p2 := e.pay(o.ID); p2.State != payments.StateAuthorized {
		t.Fatalf("new attempt %s", p2.State)
	}
	e.checkInvariants()
}

// TestCrashAfterBankThenPoll: the bank charged the customer and the request
// died before recording it. The poller finds the success.
func TestCrashAfterBankThenPoll(t *testing.T) {
	e := newEnv(t)
	o := e.createOrder(9_900)
	crash.Arm("payments.create.after_bank", 1, nil)
	crash.Recover(func() { e.svc.CreatePayment(e.ctx, e.payKey("k", o.ID)) })
	e.clk.Advance(3 * time.Minute)
	e.pollPayments()
	var state string
	e.pool.QueryRow(e.ctx, `SELECT state FROM payments WHERE order_id = $1`, o.ID).Scan(&state)
	if state != string(payments.StateAuthorized) {
		t.Fatalf("state %s", state)
	}
	e.checkInvariants()
}

// TestDuplicateCallbacks: a pending payment gets its success callback five
// times at once. One transition, one event.
func TestDuplicateCallbacks(t *testing.T) {
	e := newEnv(t)
	e.bank.Next(bank.OutcomePending)
	p := e.pay(e.createOrder(4_000).ID)
	final := e.bank.Settle(p.ID, bank.Result{Status: bank.Success})
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); e.callback(bank.KindPayment, p.ID, final) }()
	}
	wg.Wait()
	if ev := e.events(p.ID); len(ev) != 3 || ev[2] != "payment.authorized" {
		t.Fatalf("events %v", ev)
	}
	e.checkInvariants()
}

func TestUnknownCallbackIsRejected(t *testing.T) {
	e := newEnv(t)
	err := e.svc.HandleBankCallback(e.ctx, bank.Callback{Kind: bank.KindPayment, ID: uuid.New(), Status: bank.Success, BankRef: "X"})
	if err != payments.ErrUnknownCallback {
		t.Fatalf("got %v", err)
	}
}

// TestRefundPolling: a refund the bank answers late is resolved by the
// poller; a refund whose request died after phase A is submitted by it.
func TestRefundPolling(t *testing.T) {
	e := newEnv(t)
	e.captured(money.Rupees(1000))
	p := e.captured(money.Rupees(1000))

	e.bank.Next(bank.OutcomeTimeoutPending)
	slow := mustDecode[payments.RefundView](t, e.refund(p.ID, 10_000), 201)
	e.bank.Settle(slow.ID, bank.Result{Status: bank.Success})

	crash.Arm("refunds.create.after_reserve", 1, nil)
	k := e.refundKey("dies", p.ID, 20_000)
	crash.Recover(func() { e.svc.CreateRefund(e.ctx, k, p.ID) })

	e.clk.Advance(3 * time.Minute)
	if n := e.pollRefunds(); n != 2 {
		t.Fatalf("polled %d refunds", n)
	}
	var processed int
	e.pool.QueryRow(e.ctx, `SELECT count(*) FROM refunds WHERE state = 'processed'`).Scan(&processed)
	if processed != 2 {
		t.Fatalf("%d processed", processed)
	}
	// The client's retry of the dead request sees the finished refund.
	r, err := e.svc.CreateRefund(e.ctx, k, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if v := mustDecode[payments.RefundView](t, r, 201); v.State != payments.RefundProcessed || v.Amount != 20_000 {
		t.Fatalf("retry %+v", v)
	}
	e.checkInvariants()
}

// TestPollerSkipsPoisonItem: the bank answers one payment with a reference
// that already belongs to another (a bank bug, or a restarted simulator).
// That payment can't be applied, but it must not stop the poller from
// resolving the rest of its batch. (Found in a live smoke run.)
func TestPollerSkipsPoisonItem(t *testing.T) {
	e := newEnv(t)
	taken := e.pay(e.createOrder(1_000).ID) // authorized, owns a bank_ref
	e.bank.Next(bank.OutcomeTimeoutPending, bank.OutcomeTimeoutPending)
	bad := e.pay(e.createOrder(2_000).ID)
	good := e.pay(e.createOrder(3_000).ID)
	e.bank.Settle(bad.ID, bank.Result{Status: bank.Success, BankRef: *taken.BankRef})
	e.bank.Settle(good.ID, bank.Result{Status: bank.Success})

	e.clk.Advance(6 * time.Second)
	n, err := e.svc.PollPayments(e.ctx, 100)
	if err == nil || n != 1 {
		t.Fatalf("poll: resolved %d, err %v; want 1 resolved and the poison item reported", n, err)
	}
	if got := e.payment(good.ID); got.State != payments.StateAuthorized {
		t.Fatalf("good payment %s: blocked by the bad one", got.State)
	}
	if got := e.payment(bad.ID); got.State != payments.StatePending {
		t.Fatalf("bad payment %s", got.State)
	}
	e.checkInvariants()
}
