package ledger_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/naman777/kosh/internal/clock"
	"github.com/naman777/kosh/internal/ledger"
	"github.com/naman777/kosh/internal/money"
	"github.com/naman777/kosh/internal/testdb"
	"github.com/naman777/kosh/invariants"
)

var t0 = time.Date(2026, 10, 19, 9, 0, 0, 0, time.UTC)

type fixture struct {
	pool *pgxpool.Pool
	clk  *clock.Fake
	l    *ledger.Ledger
	sys  map[string]ledger.AccountID
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	pool := testdb.New(t)
	clk := clock.NewFake(t0)
	l := ledger.New(clk)
	sys, err := l.EnsureSystemAccounts(context.Background(), pool)
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{pool: pool, clk: clk, l: l, sys: sys}
}

func (f *fixture) merchant(t *testing.T) ledger.AccountID {
	t.Helper()
	id, err := f.l.EnsureAccount(context.Background(), f.pool, ledger.MerchantPayableSpec(uuid.New()))
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func (f *fixture) tx(t *testing.T, fn func(pgx.Tx) error) error {
	t.Helper()
	return pgx.BeginFunc(context.Background(), f.pool, fn)
}

func (f *fixture) post(t *testing.T, ps ...ledger.Posting) error {
	t.Helper()
	return f.tx(t, func(tx pgx.Tx) error {
		return f.l.Post(context.Background(), tx, ledger.Entry{ID: uuid.New(), Kind: "transfer", RefID: uuid.New(), Postings: ps})
	})
}

func (f *fixture) account(t *testing.T, id ledger.AccountID) ledger.Account {
	t.Helper()
	a, err := f.l.GetAccount(context.Background(), f.pool, id)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// checkInvariants runs the ledger's own invariants. The payment-level ones
// (3–6) need payment rows these tests don't create.
func (f *fixture) checkInvariants(t *testing.T) {
	t.Helper()
	rep, err := invariants.CheckAt(context.Background(), f.pool, f.clk.Now(), 1, 2, 5, 9, 11)
	if err != nil {
		t.Fatal(err)
	}
	if err := rep.Err(); err != nil {
		t.Fatal(err)
	}
}

func dr(a ledger.AccountID, p money.Paise) ledger.Posting {
	return ledger.Posting{Account: a, Side: ledger.Debit, Amount: p}
}
func cr(a ledger.AccountID, p money.Paise) ledger.Posting {
	return ledger.Posting{Account: a, Side: ledger.Credit, Amount: p}
}

func TestCaptureExample(t *testing.T) {
	f := newFixture(t)
	m := f.merchant(t)
	b, err := money.Fees(money.Rupees(1000), 200)
	if err != nil {
		t.Fatal(err)
	}
	err = f.post(t,
		dr(f.sys[ledger.BankReceivable], b.Amount),
		cr(m, b.Net),
		cr(f.sys[ledger.FeeRevenue], b.Fee),
		cr(f.sys[ledger.GSTPayable], b.GST),
	)
	if err != nil {
		t.Fatal(err)
	}
	for id, want := range map[ledger.AccountID]money.Paise{
		f.sys[ledger.BankReceivable]: 100_000,
		m:                            97_640,
		f.sys[ledger.FeeRevenue]:     2_000,
		f.sys[ledger.GSTPayable]:     360,
	} {
		if got := f.account(t, id).Posted(); got != want {
			t.Errorf("account %d posted = %v, want %v", id, got, want)
		}
	}
	f.checkInvariants(t)
}

func TestDuplicateEntryRejected(t *testing.T) {
	f := newFixture(t)
	m := f.merchant(t)
	ref := uuid.New()
	mk := func() ledger.Entry {
		return ledger.Entry{ID: uuid.New(), Kind: "capture", RefID: ref, Postings: []ledger.Posting{
			dr(f.sys[ledger.BankReceivable], 500), cr(m, 500),
		}}
	}
	if err := f.tx(t, func(tx pgx.Tx) error { return f.l.Post(context.Background(), tx, mk()) }); err != nil {
		t.Fatal(err)
	}
	err := f.tx(t, func(tx pgx.Tx) error { return f.l.Post(context.Background(), tx, mk()) })
	if !errors.Is(err, ledger.ErrDuplicateEntry) {
		t.Fatalf("second capture for the same ref: %v", err)
	}
	if got := f.account(t, m).Posted(); got != 500 {
		t.Fatalf("merchant balance %v after duplicate, want ₹5.00", got)
	}
	f.checkInvariants(t)
}

func TestMerchantPayableCannotGoNegative(t *testing.T) {
	f := newFixture(t)
	m := f.merchant(t)
	recv := f.sys[ledger.BankReceivable]
	if err := f.post(t, dr(recv, 1_000), cr(m, 1_000)); err != nil {
		t.Fatal(err)
	}
	err := f.post(t, dr(m, 1_001), cr(recv, 1_001))
	if !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Fatalf("overdraw: %v", err)
	}
	if err := f.post(t, dr(m, 1_000), cr(recv, 1_000)); err != nil {
		t.Fatalf("draining exactly to zero: %v", err)
	}
	f.checkInvariants(t)
}

func TestUnknownAccountRollsBack(t *testing.T) {
	f := newFixture(t)
	err := f.post(t, dr(f.sys[ledger.BankReceivable], 5), cr(999_999, 5))
	if !errors.Is(err, ledger.ErrAccountNotFound) {
		t.Fatalf("got %v", err)
	}
	var n int
	if err := f.pool.QueryRow(context.Background(), `SELECT count(*) FROM journal_entries`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("%d entries left behind by a failed post", n)
	}
}

func TestHoldCommitAndVoid(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	m := f.merchant(t)
	recv := f.sys[ledger.BankReceivable]
	if err := f.post(t, dr(recv, 10_000), cr(m, 10_000)); err != nil {
		t.Fatal(err)
	}

	hold := func(amount money.Paise) uuid.UUID {
		id := uuid.New()
		err := f.tx(t, func(tx pgx.Tx) error {
			return f.l.Hold(ctx, tx, ledger.Entry{ID: id, Kind: "refund", RefID: uuid.New(), Postings: []ledger.Posting{
				dr(m, amount), cr(recv, amount),
			}}, f.clk.Now().Add(time.Hour))
		})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}

	h1 := hold(6_000)
	if a := f.account(t, m); a.Available() != 4_000 || a.Posted() != 10_000 {
		t.Fatalf("after hold: available %v posted %v", a.Available(), a.Posted())
	}
	// The hold reserves funds: a second hold past what is left must fail.
	err := f.tx(t, func(tx pgx.Tx) error {
		return f.l.Hold(ctx, tx, ledger.Entry{ID: uuid.New(), Kind: "refund", RefID: uuid.New(), Postings: []ledger.Posting{
			dr(m, 4_001), cr(recv, 4_001),
		}}, f.clk.Now().Add(time.Hour))
	})
	if !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Fatalf("hold beyond available: %v", err)
	}

	if err := f.tx(t, func(tx pgx.Tx) error { return f.l.CommitHold(ctx, tx, h1) }); err != nil {
		t.Fatal(err)
	}
	if a := f.account(t, m); a.Posted() != 4_000 || a.PendingDr != 0 {
		t.Fatalf("after commit: %+v", a)
	}
	// Resolving twice is refused and moves nothing.
	if err := f.tx(t, func(tx pgx.Tx) error { return f.l.VoidHold(ctx, tx, h1) }); !errors.Is(err, ledger.ErrHoldResolved) {
		t.Fatalf("void after commit: %v", err)
	}

	h2 := hold(3_000)
	if err := f.tx(t, func(tx pgx.Tx) error { return f.l.VoidHold(ctx, tx, h2) }); err != nil {
		t.Fatal(err)
	}
	if a := f.account(t, m); a.Posted() != 4_000 || a.Available() != 4_000 {
		t.Fatalf("after void: %+v", a)
	}
	if err := f.tx(t, func(tx pgx.Tx) error { return f.l.CommitHold(ctx, tx, h2) }); !errors.Is(err, ledger.ErrHoldResolved) {
		t.Fatalf("commit after void: %v", err)
	}
	f.checkInvariants(t)
}

func TestHoldExpiry(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	m := f.merchant(t)
	recv := f.sys[ledger.BankReceivable]
	if err := f.post(t, dr(recv, 500), cr(m, 500)); err != nil {
		t.Fatal(err)
	}
	id, ref := uuid.New(), uuid.New()
	err := f.tx(t, func(tx pgx.Tx) error {
		return f.l.Hold(ctx, tx, ledger.Entry{ID: id, Kind: "payout", RefID: ref, Postings: []ledger.Posting{
			dr(m, 500), cr(f.sys[ledger.PayoutClearing], 500),
		}}, f.clk.Now().Add(time.Minute))
	})
	if err != nil {
		t.Fatal(err)
	}

	f.clk.Advance(2 * time.Minute)
	// Invariant 9 tolerates 10 minutes for the expiry job; past that, an
	// unexpired hold is a violation.
	rep, err := invariants.CheckAt(ctx, f.pool, f.clk.Now().Add(15*time.Minute), 9)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK() {
		t.Fatal("invariant 9 missed a hold left pending past its deadline")
	}
	if err := f.tx(t, func(tx pgx.Tx) error { return f.l.CommitHold(ctx, tx, id) }); !errors.Is(err, ledger.ErrHoldExpired) {
		t.Fatalf("commit after deadline: %v", err)
	}
	var expired []ledger.ExpiredHold
	err = f.tx(t, func(tx pgx.Tx) error {
		var err error
		expired, err = f.l.ExpireHolds(ctx, tx, 100)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(expired) != 1 || expired[0].ID != id || expired[0].RefID != ref || expired[0].Kind != "payout" {
		t.Fatalf("expired = %+v", expired)
	}
	if a := f.account(t, m); a.Available() != 500 {
		t.Fatalf("funds not released: %+v", a)
	}
	if s, _ := f.l.EntryState(ctx, f.pool, id); s != ledger.StateVoided {
		t.Fatalf("state %q", s)
	}
	if err := f.tx(t, func(tx pgx.Tx) error { return f.l.Hold(ctx, tx, ledger.Entry{}, f.clk.Now()) }); !errors.Is(err, ledger.ErrBadExpiry) {
		t.Fatalf("hold with past deadline: %v", err)
	}
	f.checkInvariants(t)
}

// TestAppendOnly proves invariant 11 is enforced by the database itself,
// not just by the ledger code.
func TestAppendOnly(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	m := f.merchant(t)
	if err := f.post(t, dr(f.sys[ledger.BankReceivable], 700), cr(m, 700)); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		`UPDATE postings SET amount = amount + 1`,
		`DELETE FROM postings`,
		`TRUNCATE postings CASCADE`,
		`UPDATE journal_entries SET state = 'voided', resolved_at = now()`,
		`UPDATE journal_entries SET ref_id = gen_random_uuid()`,
		`DELETE FROM journal_entries`,
	} {
		_, err := f.pool.Exec(ctx, sql)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23001" {
			t.Errorf("%s: want restrict_violation, got %v", sql, err)
		}
	}
	f.checkInvariants(t)
}

// TestInvariantsCatchDrift corrupts a balance behind the ledger's back and
// expects invariant 2 to notice. It guards the checker itself.
func TestInvariantsCatchDrift(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	m := f.merchant(t)
	if err := f.post(t, dr(f.sys[ledger.BankReceivable], 700), cr(m, 700)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `UPDATE accounts SET posted_cr = posted_cr + 1 WHERE id = $1`, m); err != nil {
		t.Fatal(err)
	}
	rep, err := invariants.Check(ctx, f.pool, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Results[0].OK() || rep.Results[1].OK() {
		t.Fatalf("drift not detected:\n%s", rep)
	}
}
