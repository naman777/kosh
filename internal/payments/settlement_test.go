package payments_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/naman777/kosh/internal/bank"
	"github.com/naman777/kosh/internal/crash"
	"github.com/naman777/kosh/internal/ledger"
	"github.com/naman777/kosh/internal/money"
	"github.com/naman777/kosh/internal/payments"
)

// Nov 9 2026 is business day D in these tests.
var dayD = time.Date(2026, 11, 9, 0, 0, 0, 0, payments.IST)

func (e *env) payable() ledger.Account {
	e.t.Helper()
	var id ledger.AccountID
	if err := e.pool.QueryRow(e.ctx, `SELECT id FROM accounts WHERE code = 'merchant_payable' AND owner_id = $1`, e.m).Scan(&id); err != nil {
		e.t.Fatal(err)
	}
	a, err := e.svc.Ledger().GetAccount(e.ctx, e.pool, id)
	if err != nil {
		e.t.Fatal(err)
	}
	return a
}

func (e *env) settle(day time.Time) payments.SettlementReport {
	e.t.Helper()
	rep, err := e.svc.RunSettlement(e.ctx, day)
	if err != nil {
		e.t.Fatal(err)
	}
	return rep
}

func (e *env) payouts() []payments.PayoutView {
	e.t.Helper()
	rows, err := e.pool.Query(e.ctx, `SELECT id FROM payouts WHERE merchant_id = $1 ORDER BY settlement_date`, e.m)
	if err != nil {
		e.t.Fatal(err)
	}
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		rows.Scan(&id)
		ids = append(ids, id)
	}
	rows.Close()
	var out []payments.PayoutView
	for _, id := range ids {
		p, err := e.svc.GetPayout(e.ctx, e.m, id)
		if err != nil {
			e.t.Fatal(err)
		}
		out = append(out, p)
	}
	return out
}

// TestSettlementEligibility checks the T+1 amount: day D's captures, less
// its refunds and open holds, and none of day D+1's captures.
func TestSettlementEligibility(t *testing.T) {
	e := newEnv(t)
	e.clk.Set(dayD.Add(10 * time.Hour))
	p1 := e.captured(money.Rupees(1000)) // net 97,640
	e.captured(money.Rupees(500))        // net 48,820
	mustDecode[payments.RefundView](t, e.refund(p1.ID, 10_000), 201)
	e.bank.Next(bank.OutcomeTimeoutPending)
	held := mustDecode[payments.RefundView](t, e.refund(p1.ID, 5_000), 201)
	if held.State != payments.RefundPending {
		t.Fatal(held.State)
	}

	if _, err := e.svc.RunSettlement(e.ctx, dayD); !errors.Is(err, payments.ErrTooEarly) {
		t.Fatalf("settling an unfinished day: %v", err)
	}

	e.clk.Set(dayD.Add(33 * time.Hour)) // D+1, 09:00 IST
	e.captured(money.Rupees(200))       // net 19,528, belongs to D+1

	rep := e.settle(dayD)
	const want = 97_640 + 48_820 - 10_000 - 5_000
	if rep.Payouts != 1 || rep.Amount != want {
		t.Fatalf("report %+v, want one payout of %d", rep, want)
	}
	po := e.payouts()
	if len(po) != 1 || po[0].State != payments.PayoutProcessed || po[0].Amount != want || po[0].SettlementDate != "2026-11-09" {
		t.Fatalf("payouts %+v", po)
	}
	if a := e.payable(); a.Available() != 19_528 || a.PendingDr != 5_000 {
		t.Fatalf("payable after payout: available %v pending %v", a.Available(), a.PendingDr)
	}
	if a := e.system(ledger.PayoutClearing); a.Posted() != 0 || a.PendingCr != 0 {
		t.Fatalf("payout_clearing %+v", a)
	}
	if a := e.system(ledger.BankSettlement); a.Posted() != -want {
		t.Fatalf("bank_settlement %v, want -%d (cash sent out)", a.Posted(), want)
	}

	// Running the batch again pays nobody twice.
	if rep := e.settle(dayD); rep.Payouts != 0 {
		t.Fatalf("second run %+v", rep)
	}
	if ev := e.events(po[0].ID); len(ev) != 2 || ev[0] != "payout.created" || ev[1] != "payout.processed" {
		t.Fatalf("payout events %v", ev)
	}
	e.checkInvariants()
}

// TestPayoutFailurePaidNextDay: the bank rejects the payout. The hold is
// voided, the balance is whole again, and D+1's batch pays everything.
func TestPayoutFailurePaidNextDay(t *testing.T) {
	e := newEnv(t)
	e.clk.Set(dayD.Add(10 * time.Hour))
	e.captured(money.Rupees(1000))
	e.clk.Set(dayD.Add(30 * time.Hour))
	e.captured(money.Rupees(100)) // D+1 credit: 9,764
	e.bank.Next(bank.OutcomeFail)
	e.settle(dayD)
	if po := e.payouts(); len(po) != 1 || po[0].State != payments.PayoutFailed {
		t.Fatalf("payouts %+v", po)
	}
	if a := e.payable(); a.Available() != 97_640+9_764 || a.PendingDr != 0 {
		t.Fatalf("payable after failed payout %+v", a)
	}

	e.clk.Set(dayD.Add(54 * time.Hour)) // D+2
	rep := e.settle(dayD.AddDate(0, 0, 1))
	if rep.Amount != 97_640+9_764 {
		t.Fatalf("next day's batch %+v", rep)
	}
	if a := e.payable(); a.Available() != 0 {
		t.Fatalf("payable %v after the catch-up payout", a.Available())
	}
	e.checkInvariants()
}

// TestPayoutCrashResumes: the batch dies after the hold, before the bank is
// asked. The poller submits the payout once the grace period passes.
func TestPayoutCrashResumes(t *testing.T) {
	e := newEnv(t)
	e.clk.Set(dayD.Add(10 * time.Hour))
	e.captured(money.Rupees(1000))
	e.clk.Set(dayD.Add(30 * time.Hour))
	crash.Arm("payouts.after_hold", 1, nil)
	crash.Recover(func() { e.svc.RunSettlement(e.ctx, dayD) })
	if po := e.payouts(); len(po) != 1 || po[0].State != payments.PayoutPending {
		t.Fatalf("after crash %+v", po)
	}
	e.checkInvariants()

	e.clk.Advance(3 * time.Minute)
	if n, err := e.svc.PollPayouts(e.ctx, 10); err != nil || n != 1 {
		t.Fatalf("poll %d %v", n, err)
	}
	if po := e.payouts(); po[0].State != payments.PayoutProcessed {
		t.Fatalf("after poll %+v", po)
	}
	e.checkInvariants()
}

// TestPayoutPendingWaits: a payout the bank keeps pending is never failed
// by the poller (that could pay twice). Only its hold's expiry fails it.
func TestPayoutPendingWaits(t *testing.T) {
	e := newEnv(t)
	e.clk.Set(dayD.Add(10 * time.Hour))
	e.captured(money.Rupees(1000))
	e.clk.Set(dayD.Add(30 * time.Hour))
	e.bank.Next(bank.OutcomePending)
	e.settle(dayD)
	for i := 0; i < 48; i++ {
		e.clk.Advance(time.Hour)
		if _, err := e.svc.PollPayouts(e.ctx, 10); err != nil {
			t.Fatal(err)
		}
	}
	if po := e.payouts(); po[0].State != payments.PayoutPending {
		t.Fatalf("after 2 days %+v", po)
	}
	e.checkInvariants()

	e.clk.Advance(6 * 24 * time.Hour)
	if n, err := e.svc.ExpireHolds(e.ctx, 10); err != nil || n != 1 {
		t.Fatalf("expire %d %v", n, err)
	}
	po := e.payouts()
	if po[0].State != payments.PayoutFailed || *po[0].FailureReason != "hold_expired" {
		t.Fatalf("after expiry %+v", po)
	}
	if a := e.payable(); a.Available() != 97_640 {
		t.Fatalf("payable %v", a.Available())
	}
	e.checkInvariants()
}

// TestSettlementIgnoresConcurrentCapture: a capture commits while the batch
// waits for the merchant's account lock. Today's credit must not leak into
// yesterday's payout. (Found by TestBankChaos: under READ COMMITTED a
// SELECT … FOR UPDATE re-reads the locked row's new balance but its
// subqueries keep the old snapshot, so the new credit was counted in the
// balance and missed by "credits since cutoff".)
func TestSettlementIgnoresConcurrentCapture(t *testing.T) {
	e := newEnv(t)
	e.clk.Set(dayD.Add(10 * time.Hour))
	e.captured(money.Rupees(1000)) // net 97,640 on day D
	e.clk.Set(dayD.Add(33 * time.Hour))

	// A capture on D+1 holds the merchant's account lock, uncommitted.
	payable := e.payable().ID
	tx, err := e.pool.Begin(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(e.ctx)
	if err := e.svc.Ledger().Post(e.ctx, tx, ledger.Entry{ID: uuid.New(), Kind: "capture", RefID: uuid.New(), Postings: []ledger.Posting{
		{Account: e.system(ledger.BankReceivable).ID, Side: ledger.Debit, Amount: 50_000},
		{Account: payable, Side: ledger.Credit, Amount: 50_000},
	}}); err != nil {
		t.Fatal(err)
	}

	done := make(chan payments.SettlementReport)
	go func() {
		rep, err := e.svc.RunSettlement(e.ctx, dayD)
		if err != nil {
			t.Error(err)
		}
		done <- rep
	}()
	time.Sleep(300 * time.Millisecond) // the batch is now waiting on the lock
	if err := tx.Commit(e.ctx); err != nil {
		t.Fatal(err)
	}
	if rep := <-done; rep.Amount != 97_640 {
		t.Fatalf("payout %v, want ₹976.40: the concurrent D+1 capture leaked into day D", rep.Amount)
	}
}
