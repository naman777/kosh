package ledger

import (
	"errors"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/google/uuid"

	"github.com/naman777/kosh/internal/money"
)

func entry(ps ...Posting) Entry {
	return Entry{ID: uuid.New(), Kind: "transfer", RefID: uuid.New(), Postings: ps}
}

func TestValidate(t *testing.T) {
	ok := []Posting{{1, Debit, 100}, {2, Credit, 60}, {3, Credit, 40}}
	cases := []struct {
		name string
		e    Entry
		want error
	}{
		{"balanced", entry(ok...), nil},
		{"same account both sides", entry(Posting{1, Debit, 5}, Posting{1, Credit, 5}), nil},
		{"one posting", entry(Posting{1, Debit, 100}), ErrTooFewPostings},
		{"unbalanced", entry(Posting{1, Debit, 100}, Posting{2, Credit, 99}), ErrUnbalanced},
		{"zero amount", entry(Posting{1, Debit, 0}, Posting{2, Credit, 0}), ErrNonPositive},
		{"negative amount", entry(Posting{1, Debit, -5}, Posting{2, Credit, -5}), ErrNonPositive},
		{"bad side", entry(Posting{1, "x", 5}, Posting{2, Credit, 5}), ErrBadSide},
		{"overflow", entry(Posting{1, Debit, math.MaxInt64}, Posting{2, Debit, 1}, Posting{3, Credit, 1}), ErrOverflow},
		{"no id", Entry{Kind: "transfer", RefID: uuid.New(), Postings: ok}, ErrNoID},
		{"no kind", Entry{ID: uuid.New(), RefID: uuid.New(), Postings: ok}, ErrNoKind},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := validate(c.e)
			if !errors.Is(err, c.want) {
				t.Fatalf("validate = %v, want %v", err, c.want)
			}
		})
	}
}

func TestCheckFlags(t *testing.T) {
	f := DebitsMustNotExceedCredits
	if err := checkFlags(1, f, Balances{PostedCr: 100, PostedDr: 60, PendingDr: 40}); err != nil {
		t.Fatalf("exactly at the limit should pass: %v", err)
	}
	if err := checkFlags(1, f, Balances{PostedCr: 100, PostedDr: 60, PendingDr: 41}); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("a hold past the limit should fail: %v", err)
	}
	// Pending credits are not spendable yet.
	if err := checkFlags(1, f, Balances{PostedCr: 100, PendingCr: 500, PostedDr: 101}); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("pending credits must not count: %v", err)
	}
	if err := checkFlags(1, CreditsMustNotExceedDebits, Balances{PostedDr: 10, PostedCr: 11}); !errors.Is(err, ErrExceedsDebits) {
		t.Fatalf("mirror flag: %v", err)
	}
}

// randomEntry builds a balanced entry over 2–5 legs drawn from accounts.
func randomEntry(r *rand.Rand, accounts int, maxAmount int64) Entry {
	total := money.Paise(r.Int64N(maxAmount) + 2)
	nDr := 1 + r.IntN(2)
	nCr := 1 + r.IntN(3)
	split := func(n int, side Side) []Posting {
		var ps []Posting
		left := total
		for i := 0; i < n; i++ {
			amt := left
			if i < n-1 && left > 1 {
				amt = money.Paise(r.Int64N(int64(left-1))) + 1
				if left-amt < 1 {
					amt = left - 1
				}
			}
			if amt <= 0 {
				break
			}
			ps = append(ps, Posting{Account: AccountID(r.IntN(accounts) + 1), Side: side, Amount: amt})
			left -= amt
			if left == 0 {
				break
			}
		}
		return ps
	}
	return entry(append(split(nDr, Debit), split(nCr, Credit)...)...)
}

// corrupt breaks a valid entry in one of several ways.
func corrupt(r *rand.Rand, e Entry) Entry {
	ps := append([]Posting(nil), e.Postings...)
	switch r.IntN(4) {
	case 0:
		ps[r.IntN(len(ps))].Amount++
	case 1:
		ps[r.IntN(len(ps))].Amount = 0
	case 2:
		ps = ps[:1]
	case 3:
		ps[r.IntN(len(ps))].Side = "?"
	}
	e.Postings = ps
	return e
}

// TestPropertyPure runs 1,000,000 random entries through the same validation
// and flag checks Post uses, against an in-memory model of the balance
// columns, and checks after every step that the ledger still balances and no
// flagged account is over its limit. Holds are placed, committed and voided
// along the way. Malformed entries must always be rejected.
func TestPropertyPure(t *testing.T) {
	n := 1_000_000
	if testing.Short() {
		n = 50_000
	}
	const accounts = 20
	r := rand.New(rand.NewPCG(20261019, 1))

	flags := map[AccountID]Flags{}
	bal := map[AccountID]Balances{}
	for i := 1; i <= accounts; i++ {
		if i%3 == 0 {
			flags[AccountID(i)] = DebitsMustNotExceedCredits
		}
	}
	type hold map[AccountID]delta
	var holds []hold

	var posted, rejectedFlags, bad int
	check := func(step int) {
		var tot Balances
		for id, b := range bal {
			tot.PostedDr += b.PostedDr
			tot.PostedCr += b.PostedCr
			tot.PendingDr += b.PendingDr
			tot.PendingCr += b.PendingCr
			if err := checkFlags(id, flags[id], b); err != nil {
				t.Fatalf("step %d: %v: %+v", step, err, b)
			}
			if b.PendingDr < 0 || b.PendingCr < 0 {
				t.Fatalf("step %d: negative pending on %d: %+v", step, id, b)
			}
		}
		if tot.PostedDr != tot.PostedCr || tot.PendingDr != tot.PendingCr {
			t.Fatalf("step %d: ledger unbalanced: %+v", step, tot)
		}
	}

	for i := 0; i < n; i++ {
		// Resolve a hold now and then.
		if len(holds) > 0 && r.IntN(4) == 0 {
			k := r.IntN(len(holds))
			h := holds[k]
			holds = append(holds[:k], holds[k+1:]...)
			commit := r.IntN(2) == 0
			for id, d := range h {
				b := bal[id]
				b.PendingDr -= d.Dr
				b.PendingCr -= d.Cr
				if commit {
					b.PostedDr += d.Dr
					b.PostedCr += d.Cr
				}
				bal[id] = b
			}
			check(i)
			continue
		}

		e := randomEntry(r, accounts, 1_000_000)
		if r.IntN(10) == 0 {
			if _, err := validate(corrupt(r, e)); err == nil {
				t.Fatalf("step %d: corrupted entry accepted", i)
			}
			bad++
			continue
		}
		deltas, err := validate(e)
		if err != nil {
			t.Fatalf("step %d: valid entry rejected: %v", i, err)
		}
		pending := r.IntN(4) == 0
		next := map[AccountID]Balances{}
		reject := false
		for id, d := range deltas {
			b, err := apply(bal[id], d, pending)
			if err != nil {
				t.Fatal(err)
			}
			if checkFlags(id, flags[id], b) != nil {
				reject = true
				break
			}
			next[id] = b
		}
		if reject {
			rejectedFlags++
			continue
		}
		for id, b := range next {
			bal[id] = b
		}
		if pending {
			holds = append(holds, hold(deltas))
		}
		posted++
		check(i)
	}
	t.Logf("%d steps: %d accepted, %d rejected by flags, %d malformed rejected, %d holds open",
		n, posted, rejectedFlags, bad, len(holds))
	if posted == 0 || rejectedFlags == 0 {
		t.Fatal("generator is not exercising both paths")
	}
}
