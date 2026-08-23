package payments_test

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/naman777/kosh/internal/bank"
	"github.com/naman777/kosh/internal/crash"
	"github.com/naman777/kosh/internal/ledger"
	"github.com/naman777/kosh/internal/money"
	"github.com/naman777/kosh/internal/payments"
)

func TestStateMachine(t *testing.T) {
	states := []payments.State{payments.StateCreated, payments.StatePending, payments.StateAuthorized,
		payments.StateCaptured, payments.StateFailed, payments.StateRefunded}
	legal := map[[2]payments.State]bool{
		{"created", "pending"}: true, {"created", "authorized"}: true, {"created", "failed"}: true,
		{"pending", "authorized"}: true, {"pending", "failed"}: true,
		{"authorized", "captured"}: true, {"authorized", "failed"}: true,
		{"captured", "refunded"}: true,
	}
	for _, from := range states {
		for _, to := range states {
			if got := payments.CanTransition(from, to); got != legal[[2]payments.State{from, to}] {
				t.Errorf("CanTransition(%s, %s) = %v", from, to, got)
			}
		}
	}
}

// TestHappyPath walks the README example: ₹1,000 at 2% + 18% GST, then
// partial and full refunds.
func TestHappyPath(t *testing.T) {
	e := newEnv(t)
	p := e.captured(money.Rupees(1000))
	if p.State != payments.StateCaptured || p.BankRef == nil {
		t.Fatalf("payment %+v", p)
	}

	payable := func() ledger.Account {
		var id ledger.AccountID
		if err := e.pool.QueryRow(e.ctx, `SELECT id FROM accounts WHERE code = 'merchant_payable' AND owner_id = $1`, e.m).Scan(&id); err != nil {
			t.Fatal(err)
		}
		a, err := e.svc.Ledger().GetAccount(e.ctx, e.pool, id)
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	if got := payable().Posted(); got != 97_640 {
		t.Fatalf("merchant payable %v, want ₹976.40", got)
	}

	r := mustDecode[payments.RefundView](t, e.refund(p.ID, 30_000), 201)
	if r.State != payments.RefundProcessed || r.Amount != 30_000 {
		t.Fatalf("refund %+v", r)
	}
	// Fees are not returned on refunds, so the merchant (₹676.40 left) can't
	// cover the remaining ₹700. Invariant 5 in action.
	resp := e.refund(p.ID, 0)
	if resp.Status != 422 || errCode(t, resp) != "insufficient_balance" {
		t.Fatalf("refund beyond balance: %d %s", resp.Status, resp.Body)
	}
	if got := payable(); got.Posted() != 67_640 || got.PendingDr != 0 {
		t.Fatalf("payable after refused refund: %+v", got)
	}

	// Another sale funds the merchant; now the rest can be refunded.
	e.captured(money.Rupees(500))
	r = mustDecode[payments.RefundView](t, e.refund(p.ID, 0), 201)
	if r.Amount != 70_000 {
		t.Fatalf("full-remaining refund amount %d", r.Amount)
	}
	got, err := e.svc.GetPayment(e.ctx, e.m, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != payments.StateRefunded || got.Refunded != got.Amount {
		t.Fatalf("payment after full refund: %+v", got)
	}
	if resp := e.refund(p.ID, 1); resp.Status != 422 || errCode(t, resp) != "fully_refunded" {
		t.Fatalf("refund on refunded payment: %d %s", resp.Status, resp.Body)
	}
	e.checkInvariants()
}

// TestRetryStorm is the phase 2 done-when: a client retrying any call 5
// times never creates a second payment or capture, and always gets the
// same bytes back.
func TestRetryStorm(t *testing.T) {
	e := newEnv(t)
	o := e.createOrder(money.Rupees(250))

	// Sequential retries of create-payment.
	k := e.payKey("pay-1", o.ID)
	var first []byte
	for i := 0; i < 5; i++ {
		r, err := e.svc.CreatePayment(e.ctx, k)
		if err != nil {
			t.Fatal(err)
		}
		if r.Status != 201 {
			t.Fatalf("attempt %d: %d %s", i, r.Status, r.Body)
		}
		if i == 0 {
			first = r.Body
		} else if !bytes.Equal(r.Body, first) {
			t.Fatalf("attempt %d returned different bytes:\n%s\n%s", i, r.Body, first)
		}
	}
	if n := e.bank.Calls.Load(); n != 1 {
		t.Fatalf("bank called %d times, want 1", n)
	}
	p := mustDecode[payments.PaymentView](t, payments.Response{Status: 201, Body: first}, 201)

	// Concurrent retries of capture.
	ck := e.captureKey("cap-1", p.ID)
	var wg sync.WaitGroup
	results := make([]payments.Response, 5)
	errs := make([]error, 5)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = e.svc.Capture(e.ctx, ck, p.ID)
		}(i)
	}
	wg.Wait()
	var ok [][]byte
	for i, r := range results {
		var perr *payments.Error
		switch {
		case errs[i] == nil && r.Status == 200:
			ok = append(ok, r.Body)
		case errs[i] != nil && asError(errs[i], &perr) && perr.Code == "idempotency_key_in_use":
		default:
			t.Fatalf("capture %d: %v %d %s", i, errs[i], r.Status, r.Body)
		}
	}
	if len(ok) == 0 {
		t.Fatal("no capture succeeded")
	}
	// Retry after the dust settles: same bytes as the winner.
	r, err := e.svc.Capture(e.ctx, ck, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range ok {
		if !bytes.Equal(b, r.Body) {
			t.Fatalf("capture replies differ:\n%s\n%s", b, r.Body)
		}
	}
	// A capture with a *new* key is a new request, and is refused.
	r, err = e.svc.Capture(e.ctx, e.captureKey("cap-2", p.ID), p.ID)
	if err != nil || r.Status != 409 || errCode(t, r) != "already_captured" {
		t.Fatalf("second capture: %v %d %s", err, r.Status, r.Body)
	}

	if n := e.count("payments"); n != 1 {
		t.Fatalf("%d payments, want 1", n)
	}
	var captures int
	e.pool.QueryRow(e.ctx, `SELECT count(*) FROM journal_entries WHERE kind = 'capture'`).Scan(&captures)
	if captures != 1 {
		t.Fatalf("%d capture entries, want 1", captures)
	}
	e.checkInvariants()
}

func asError(err error, target **payments.Error) bool {
	pe, ok := err.(*payments.Error)
	if ok {
		*target = pe
	}
	return ok
}

// TestKeyReusedWithDifferentBody: 422, and nothing is written.
func TestKeyReusedWithDifferentBody(t *testing.T) {
	e := newEnv(t)
	o1, o2 := e.createOrder(10_000), e.createOrder(20_000)
	if _, err := e.svc.CreatePayment(e.ctx, e.payKey("same", o1.ID)); err != nil {
		t.Fatal(err)
	}
	before := e.count("payments")
	_, err := e.svc.CreatePayment(e.ctx, e.payKey("same", o2.ID))
	var perr *payments.Error
	if !asError(err, &perr) || perr.Status != 422 {
		t.Fatalf("got %v", err)
	}
	if e.count("payments") != before {
		t.Fatal("a mismatched retry wrote a payment")
	}
	e.checkInvariants()
}

// TestConcurrentRefunds fires 10 refunds of ₹200 at a ₹1,000 payment at once.
// Exactly 5 fit; the rest are refused and refunded never exceeds amount.
func TestConcurrentRefunds(t *testing.T) {
	e := newEnv(t)
	e.captured(money.Rupees(10_000)) // fund the merchant
	p := e.captured(money.Rupees(1000))

	var wg sync.WaitGroup
	results := make([]payments.Response, 10)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := e.svc.CreateRefund(e.ctx, e.refundKey("r-"+string(rune('a'+i)), p.ID, 20_000), p.ID)
			if err != nil {
				t.Error(err)
			}
			results[i] = r
		}(i)
	}
	wg.Wait()
	created, refused := 0, 0
	for _, r := range results {
		switch {
		case r.Status == 201:
			created++
		case r.Status == 422 || r.Status == 409:
			refused++
		default:
			t.Fatalf("refund: %d %s", r.Status, r.Body)
		}
	}
	if created != 5 || refused != 5 {
		t.Fatalf("%d created, %d refused; want 5 and 5", created, refused)
	}
	got, _ := e.svc.GetPayment(e.ctx, e.m, p.ID)
	if got.Refunded != got.Amount || got.State != payments.StateRefunded {
		t.Fatalf("payment %+v", got)
	}
	e.checkInvariants()
}

// TestCrashBetweenPhases kills the request after the bank answered but
// before the result was stored, then retries. The retry must wait out the
// dead request's lease, resume, and produce exactly one payment.
func TestCrashBetweenPhases(t *testing.T) {
	for _, point := range []string{"payments.create.after_insert", "payments.create.after_bank"} {
		t.Run(point, func(t *testing.T) {
			e := newEnv(t)
			o := e.createOrder(money.Rupees(99))
			k := e.payKey("crashy", o.ID)

			crash.Arm(point, 1, nil)
			if _, crashed := crash.Recover(func() { e.svc.CreatePayment(e.ctx, k) }); !crashed {
				t.Fatal("did not crash")
			}

			// The dead request still holds the lease.
			_, err := e.svc.CreatePayment(e.ctx, k)
			var perr *payments.Error
			if !asError(err, &perr) || perr.Code != "idempotency_key_in_use" {
				t.Fatalf("retry during lease: %v", err)
			}

			e.clk.Advance(61 * time.Second)
			r, err := e.svc.CreatePayment(e.ctx, k)
			if err != nil {
				t.Fatal(err)
			}
			p := mustDecode[payments.PaymentView](t, r, 201)
			if p.State != payments.StateAuthorized {
				t.Fatalf("resumed payment %s", p.State)
			}
			if n := e.count("payments"); n != 1 {
				t.Fatalf("%d payments after crash and retry", n)
			}
			// One more retry replays the stored bytes.
			r2, _ := e.svc.CreatePayment(e.ctx, k)
			if !bytes.Equal(r.Body, r2.Body) {
				t.Fatal("replay differs")
			}
			e.checkInvariants()
		})
	}
}

func TestCrashDuringRefund(t *testing.T) {
	e := newEnv(t)
	e.captured(money.Rupees(1000))
	p := e.captured(money.Rupees(1000))
	k := e.refundKey("refund-crash", p.ID, 10_000)

	crash.Arm("refunds.create.after_bank", 1, nil)
	if _, crashed := crash.Recover(func() { e.svc.CreateRefund(e.ctx, k, p.ID) }); !crashed {
		t.Fatal("did not crash")
	}
	// Between the crash and the retry, the hold keeps the money reserved and
	// the invariants still hold.
	e.checkInvariants()

	e.clk.Advance(61 * time.Second)
	r, err := e.svc.CreateRefund(e.ctx, k, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if v := mustDecode[payments.RefundView](t, r, 201); v.State != payments.RefundProcessed {
		t.Fatalf("refund %+v", v)
	}
	if n := e.count("refunds"); n != 1 {
		t.Fatalf("%d refunds", n)
	}
	e.checkInvariants()
}

func TestBankTimeoutLeavesPaymentPending(t *testing.T) {
	e := newEnv(t)
	o := e.createOrder(5_000)
	e.bank.Next(bank.OutcomeTimeout)
	k := e.payKey("slow", o.ID)
	r, err := e.svc.CreatePayment(e.ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	if p := mustDecode[payments.PaymentView](t, r, 201); p.State != payments.StatePending {
		t.Fatalf("state %s, want pending (never failed on a timeout)", p.State)
	}
	// The order is not free for another attempt while this one is unresolved.
	r2, err := e.svc.CreatePayment(e.ctx, e.payKey("second", o.ID))
	if err != nil || r2.Status != 409 || errCode(t, r2) != "order_already_has_payment" {
		t.Fatalf("second attempt: %v %d %s", err, r2.Status, r2.Body)
	}
	e.checkInvariants()
}

func TestDeclineThenRetryWithNewAttempt(t *testing.T) {
	e := newEnv(t)
	o := e.createOrder(5_000)
	e.bank.Next(bank.OutcomeFail)
	p := e.pay(o.ID)
	if p.State != payments.StateFailed || p.FailureReason == nil {
		t.Fatalf("payment %+v", p)
	}
	// Capture of a failed payment is refused.
	r, _ := e.svc.Capture(e.ctx, e.captureKey("c", p.ID), p.ID)
	if r.Status != 409 || errCode(t, r) != "invalid_state" {
		t.Fatalf("capture failed payment: %d %s", r.Status, r.Body)
	}
	// A failed attempt frees the order.
	if p2 := e.pay(o.ID); p2.State != payments.StateAuthorized {
		t.Fatalf("second attempt %s", p2.State)
	}
	e.checkInvariants()
}

func TestRefundDeclinedByBank(t *testing.T) {
	e := newEnv(t)
	e.captured(money.Rupees(1000))
	p := e.captured(money.Rupees(1000))
	e.bank.Next(bank.OutcomeFail)
	r := mustDecode[payments.RefundView](t, e.refund(p.ID, 40_000), 201)
	if r.State != payments.RefundFailed {
		t.Fatalf("refund %+v", r)
	}
	got, _ := e.svc.GetPayment(e.ctx, e.m, p.ID)
	if got.Refunded != 0 {
		t.Fatalf("refunded %d after a failed refund", got.Refunded)
	}
	e.checkInvariants()
}

func TestRefundHoldExpires(t *testing.T) {
	e := newEnv(t)
	e.captured(money.Rupees(1000))
	p := e.captured(money.Rupees(1000))
	e.bank.Next(bank.OutcomeTimeout)
	r := mustDecode[payments.RefundView](t, e.refund(p.ID, 40_000), 201)
	if r.State != payments.RefundPending {
		t.Fatalf("refund %+v", r)
	}
	if n, err := e.svc.ExpireRefundHolds(e.ctx, 10); err != nil || n != 0 {
		t.Fatalf("expired early: %d %v", n, err)
	}
	e.checkInvariants()

	e.clk.Advance(73 * time.Hour)
	if n, err := e.svc.ExpireRefundHolds(e.ctx, 10); err != nil || n != 1 {
		t.Fatalf("expire: %d %v", n, err)
	}
	got, _ := e.svc.GetRefund(e.ctx, e.m, r.ID)
	if got.State != payments.RefundFailed {
		t.Fatalf("refund after expiry %+v", got)
	}
	pay, _ := e.svc.GetPayment(e.ctx, e.m, p.ID)
	if pay.Refunded != 0 {
		t.Fatalf("refunded %d after expiry", pay.Refunded)
	}
	e.checkInvariants()
}

func TestZeroFeeMerchant(t *testing.T) {
	e := newEnv(t)
	e.m = e.merchant(0)
	p := e.captured(12_345)
	if p.State != payments.StateCaptured {
		t.Fatal(p.State)
	}
	e.checkInvariants()
}
