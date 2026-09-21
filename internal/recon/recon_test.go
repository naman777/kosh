package recon_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/naman777/kosh/internal/bank"
	"github.com/naman777/kosh/internal/banksim"
	"github.com/naman777/kosh/internal/clock"
	"github.com/naman777/kosh/internal/idempotency"
	"github.com/naman777/kosh/internal/ids"
	"github.com/naman777/kosh/internal/ledger"
	"github.com/naman777/kosh/internal/money"
	"github.com/naman777/kosh/internal/payments"
	"github.com/naman777/kosh/internal/recon"
	"github.com/naman777/kosh/internal/testdb"
	"github.com/naman777/kosh/invariants"
)

var dayD = time.Date(2026, 11, 16, 0, 0, 0, 0, recon.IST)

type env struct {
	t     *testing.T
	ctx   context.Context
	pool  *pgxpool.Pool
	clk   *clock.Fake
	sim   *banksim.Server
	bank  *bank.HTTPClient
	svc   *payments.Service
	recon *recon.Engine
	m     uuid.UUID
	seq   int
}

func newEnv(t *testing.T) *env {
	t.Helper()
	ctx := context.Background()
	pool := testdb.New(t)
	clk := clock.NewFake(dayD.Add(10 * time.Hour))
	sim := banksim.NewWithClock(banksim.Config{}, 11, clk)
	srv := httptest.NewServer(sim)
	t.Cleanup(srv.Close)
	bc := bank.NewHTTPClient(srv.URL)
	g := ids.NewSeeded(11)
	svc, err := payments.New(ctx, payments.Deps{Pool: pool, Bank: bc, Clock: clk, IDs: g})
	if err != nil {
		t.Fatal(err)
	}
	eng, err := recon.New(ctx, pool, clk, g)
	if err != nil {
		t.Fatal(err)
	}
	m, err := svc.CreateMerchant(ctx, payments.CreateMerchantInput{Name: "Recon Merchant"})
	if err != nil {
		t.Fatal(err)
	}
	return &env{t: t, ctx: ctx, pool: pool, clk: clk, sim: sim, bank: bc, svc: svc, recon: eng, m: m.Merchant.ID}
}

func (e *env) call(fn func(context.Context, idempotency.Key) (payments.Response, error), path string, body any) []byte {
	e.t.Helper()
	e.seq++
	raw, _ := json.Marshal(body)
	r, err := fn(e.ctx, idempotency.Key{MerchantID: e.m, Key: fmt.Sprint("k", e.seq), Method: "POST", Path: path, Body: raw})
	if err != nil || r.Status >= 300 {
		e.t.Fatalf("%s: %v %d %s", path, err, r.Status, r.Body)
	}
	return r.Body
}

// sale authorizes a payment and, if capture is set, captures it.
func (e *env) sale(amount int64, method string, capture bool) payments.PaymentView {
	e.t.Helper()
	var o payments.OrderView
	json.Unmarshal(e.call(e.svc.CreateOrder, "/v1/orders", map[string]any{"amount": amount}), &o)
	var p payments.PaymentView
	json.Unmarshal(e.call(e.svc.CreatePayment, "/v1/payments", map[string]any{"order_id": o.ID, "method": method}), &p)
	if capture {
		e.call(func(ctx context.Context, k idempotency.Key) (payments.Response, error) { return e.svc.Capture(ctx, k, p.ID) },
			"/v1/payments/"+p.ID.String()+"/capture", map[string]any{})
	}
	return p
}

func (e *env) refund(p payments.PaymentView, amount int64) {
	e.t.Helper()
	e.call(func(ctx context.Context, k idempotency.Key) (payments.Response, error) { return e.svc.CreateRefund(ctx, k, p.ID) },
		"/v1/payments/"+p.ID.String()+"/refunds", map[string]any{"amount": amount})
}

// reconcileDay fetches the bank's file for day over HTTP, ingests it and
// reconciles it.
func (e *env) reconcileDay(day time.Time) recon.Report {
	e.t.Helper()
	date := day.Format("2006-01-02")
	raw, err := e.bank.SettlementFile(e.ctx, date)
	if err != nil {
		e.t.Fatal(err)
	}
	id, err := e.recon.Ingest(e.ctx, date, raw)
	if err != nil {
		e.t.Fatal(err)
	}
	rep, err := e.recon.Reconcile(e.ctx, id)
	if err != nil {
		e.t.Fatal(err)
	}
	return rep
}

func (e *env) checkInvariants() {
	e.t.Helper()
	rep, err := invariants.CheckAt(e.ctx, e.pool, e.clk.Now())
	if err != nil {
		e.t.Fatal(err)
	}
	if err := rep.Err(); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) account(code string) ledger.Account {
	e.t.Helper()
	var id ledger.AccountID
	if err := e.pool.QueryRow(e.ctx, `SELECT id FROM accounts WHERE code = $1 AND owner_id IS NULL`, code).Scan(&id); err != nil {
		e.t.Fatal(err)
	}
	a, err := e.svc.Ledger().GetAccount(e.ctx, e.pool, id)
	if err != nil {
		e.t.Fatal(err)
	}
	return a
}

// TestReconAccuracy is phase 5's recon done-when: a day of traffic, a bank
// file with every kind of injected fault, and recon must classify them.
func TestReconAccuracy(t *testing.T) {
	e := newEnv(t)
	var sold []payments.PaymentView
	for i := 0; i < 300; i++ {
		method := "upi"
		if i%3 == 0 {
			method = "card"
		}
		sold = append(sold, e.sale(int64(10_000+i*37), method, i%10 != 0)) // every tenth stays uncaptured
	}
	for i := 1; i < 300; i += 15 {
		e.refund(sold[i], 1_000)
	}

	e.sim.SetConfig(banksim.Config{Settlement: banksim.SettlementFaults{
		FeeVarianceRate: 0.03, MissingRate: 0.03, DuplicateRate: 0.03, MismatchRate: 0.03,
	}})
	e.clk.Set(dayD.Add(26 * time.Hour)) // D+1, 02:00
	repD := e.reconcileDay(dayD)
	e.clk.Set(dayD.Add(50 * time.Hour)) // D+2: yesterday's file is empty, and missing items are flagged
	repD1 := e.reconcileDay(dayD.AddDate(0, 0, 1))
	t.Logf("day D: %+v", repD)
	t.Logf("day D+1: %+v", repD1)

	lines, faults, _ := e.sim.Settlement(dayD.Format("2006-01-02"))
	if repD.Lines != len(lines) {
		t.Fatalf("reconciled %d lines, file has %d", repD.Lines, len(lines))
	}
	got := map[string][]string{} // bank_ref → statuses
	rows, _ := e.pool.Query(e.ctx, `SELECT bank_ref, status FROM recon_results`)
	for rows.Next() {
		var ref, status string
		rows.Scan(&ref, &status)
		got[ref] = append(got[ref], status)
	}
	rows.Close()

	want := map[string]string{"missing": recon.MissingInBank, "duplicate": recon.Duplicate,
		"amount_mismatch": recon.AmountMismatch, "fee_variance": recon.FeeVariance}
	faulted := map[string]bool{}
	correct, total := 0, 0
	kinds := map[string]int{}
	for _, f := range faults {
		faulted[f.BankRef] = true
		kinds[f.Kind]++
		total++
		for _, s := range got[f.BankRef] {
			if s == want[f.Kind] {
				correct++
				break
			}
		}
	}
	for _, l := range lines {
		if faulted[l.BankRef] {
			continue
		}
		total++
		if len(got[l.BankRef]) == 1 && got[l.BankRef][0] == recon.Matched {
			correct++
		}
	}
	accuracy := float64(correct) / float64(total)
	t.Logf("injected faults %v; %d/%d classified correctly (%.2f%%)", kinds, correct, total, 100*accuracy)
	for _, k := range []string{"missing", "duplicate", "amount_mismatch", "fee_variance"} {
		if kinds[k] == 0 {
			t.Errorf("seed injected no %s faults", k)
		}
	}
	if accuracy < 0.98 {
		t.Fatalf("accuracy %.2f%% below 98%%", 100*accuracy)
	}
	e.checkInvariants()

	// Cash arrived; what is still receivable is exactly the missing items.
	var missingGross int64
	e.pool.QueryRow(e.ctx, `SELECT COALESCE(SUM(expected_gross), 0) FROM recon_results WHERE status = 'missing_in_bank'`).Scan(&missingGross)
	if got := e.account(ledger.BankReceivable).Posted(); int64(got) != missingGross {
		t.Fatalf("bank_receivable %v, want %v (the missing items)", got, money.Paise(missingGross))
	}

	// Work the queue: every exception resolves once, with an entry.
	exc, err := e.recon.Exceptions(e.ctx, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(exc) != len(faults) {
		t.Fatalf("%d open exceptions, %d injected faults", len(exc), len(faults))
	}
	for i, x := range exc {
		action := recon.WriteOff
		if x.Status != recon.MissingInBank && i%2 == 0 {
			action = recon.BankCorrected
		}
		if _, err := e.recon.Resolve(e.ctx, x.ID, action); err != nil {
			t.Fatalf("resolve %s: %v", x.Status, err)
		}
		if _, err := e.recon.Resolve(e.ctx, x.ID, action); !errors.Is(err, recon.ErrNotOpen) {
			t.Fatalf("second resolve: %v", err)
		}
	}
	if a := e.account(ledger.ReconSuspense); a.Posted() != 0 {
		t.Fatalf("suspense %v after resolving everything", a.Posted())
	}
	if a := e.account(ledger.BankReceivable); a.Posted() != 0 {
		t.Fatalf("receivable %v after writing off the missing items", a.Posted())
	}
	e.checkInvariants()
}

// TestReconIdempotent: the same file twice is one file; reconciling twice
// posts once; a different file for the same day is refused.
func TestReconIdempotent(t *testing.T) {
	e := newEnv(t)
	for i := 0; i < 10; i++ {
		e.sale(5_000, "card", true)
	}
	e.clk.Set(dayD.Add(26 * time.Hour))
	date := dayD.Format("2006-01-02")
	raw, _ := e.bank.SettlementFile(e.ctx, date)
	id1, err := e.recon.Ingest(e.ctx, date, raw)
	if err != nil {
		t.Fatal(err)
	}
	id2, _ := e.recon.Ingest(e.ctx, date, raw)
	if id1 != id2 {
		t.Fatal("same file ingested twice")
	}
	r1, _ := e.recon.Reconcile(e.ctx, id1)
	r2, _ := e.recon.Reconcile(e.ctx, id1)
	if r1.ByStatus[recon.Matched] != 10 || r2.ByStatus[recon.Matched] != 10 || r1.Cash != r2.Cash {
		t.Fatalf("reports differ: %+v %+v", r1, r2)
	}
	var entries int
	e.pool.QueryRow(e.ctx, `SELECT count(*) FROM journal_entries WHERE kind = 'bank_settlement'`).Scan(&entries)
	if entries != 1 {
		t.Fatalf("%d settlement entries", entries)
	}
	other := strings.Replace(string(raw), "\n1,", "\n1,", 1) + ""
	other = strings.TrimSuffix(other, "\n")[:len(other)-20] + "\n"
	if _, err := e.recon.Ingest(e.ctx, date, []byte(other)); err == nil {
		t.Fatal("accepted a different, malformed file")
	}
	if _, err := e.recon.Ingest(e.ctx, date, []byte("line_no,bank_ref,reference,gross,fee,tax,net,value_date\n")); !errors.Is(err, recon.ErrFileConflict) {
		t.Fatalf("different file for the same day: %v", err)
	}
	// Cash from 10 card sales of ₹50 at 1% + 18% GST bank fee: 10 × (5,000 − 50 − 9).
	if r1.Cash != 10*(5_000-50-9) {
		t.Fatalf("cash %v", r1.Cash)
	}
	if a := e.account(ledger.BankCharges); a.Posted() != 10*59 {
		t.Fatalf("bank charges %v", a.Posted())
	}
	e.checkInvariants()
}

// TestMissingThenFound: an item missing from its day's file is flagged at
// T+2, then turns up in a later file. The later match closes the exception
// without anyone touching it.
func TestMissingThenFound(t *testing.T) {
	e := newEnv(t)
	p := e.sale(7_000, "upi", true)
	e.clk.Set(dayD.Add(26 * time.Hour))
	// The bank leaves it out of day D's file.
	empty := []byte("line_no,bank_ref,reference,gross,fee,tax,net,value_date\n")
	id, _ := e.recon.Ingest(e.ctx, dayD.Format("2006-01-02"), empty)
	e.recon.Reconcile(e.ctx, id)
	e.clk.Set(dayD.Add(50 * time.Hour))
	id, _ = e.recon.Ingest(e.ctx, dayD.AddDate(0, 0, 1).Format("2006-01-02"), empty)
	if rep, _ := e.recon.Reconcile(e.ctx, id); rep.ByStatus[recon.MissingInBank] != 1 {
		t.Fatalf("D+1 report %+v", rep)
	}
	e.checkInvariants()

	// It shows up two days late.
	e.clk.Set(dayD.Add(74 * time.Hour))
	late := fmt.Sprintf("line_no,bank_ref,reference,gross,fee,tax,net,value_date\n1,%s,%s,7000,0,0,7000,2026-11-19\n", *p.BankRef, p.ID)
	id, _ = e.recon.Ingest(e.ctx, dayD.AddDate(0, 0, 2).Format("2006-01-02"), []byte(late))
	if rep, _ := e.recon.Reconcile(e.ctx, id); rep.ByStatus[recon.Matched] != 1 {
		t.Fatalf("late file %+v", rep)
	}
	if exc, _ := e.recon.Exceptions(e.ctx, 10); len(exc) != 0 {
		t.Fatalf("exception still open: %+v", exc)
	}
	if a := e.account(ledger.BankReceivable); a.Posted() != 0 {
		t.Fatalf("receivable %v", a.Posted())
	}
	e.checkInvariants()
}

// TestUnknownCashIsAnException: a line the ledger knows nothing about (the
// bank completing a refund Kosh gave up on, say) lands in suspense.
func TestUnknownCashIsAnException(t *testing.T) {
	e := newEnv(t)
	e.clk.Set(dayD.Add(26 * time.Hour))
	raw := fmt.Sprintf("line_no,bank_ref,reference,gross,fee,tax,net,value_date\n1,RFN-GHOST,%s,-2500,0,0,-2500,2026-11-17\n", uuid.New())
	id, _ := e.recon.Ingest(e.ctx, dayD.Format("2006-01-02"), []byte(raw))
	rep, err := e.recon.Reconcile(e.ctx, id)
	if err != nil || rep.ByStatus[recon.MissingInLedger] != 1 {
		t.Fatalf("%+v %v", rep, err)
	}
	if a := e.account(ledger.ReconSuspense); a.Posted() != 2_500 {
		t.Fatalf("suspense %v: the bank paid out ₹25 Kosh can't explain", a.Posted())
	}
	exc, _ := e.recon.Exceptions(e.ctx, 10)
	if _, err := e.recon.Resolve(e.ctx, exc[0].ID, "shrug"); !errors.Is(err, recon.ErrBadResolution) {
		t.Fatalf("bad action: %v", err)
	}
	if _, err := e.recon.Resolve(e.ctx, exc[0].ID, recon.WriteOff); err != nil {
		t.Fatal(err)
	}
	if a := e.account(ledger.ReconWriteoff); a.Posted() != 2_500 {
		t.Fatalf("writeoff %v", a.Posted())
	}
	e.checkInvariants()
}
