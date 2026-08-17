package ledger

import (
	"sort"

	"github.com/google/uuid"

	"github.com/naman777/kosh/internal/money"
)

// delta is the net debit and credit an entry applies to one account.
type delta struct {
	Dr, Cr money.Paise
}

// validate checks an entry's shape and returns its per-account deltas.
// It touches no database, so the pure property test exercises exactly the
// code Post runs.
func validate(e Entry) (map[AccountID]delta, error) {
	if e.ID == uuid.Nil || e.RefID == uuid.Nil {
		return nil, ErrNoID
	}
	if e.Kind == "" {
		return nil, ErrNoKind
	}
	if len(e.Postings) < 2 {
		return nil, ErrTooFewPostings
	}
	var dr, cr money.Paise
	deltas := make(map[AccountID]delta, len(e.Postings))
	for _, p := range e.Postings {
		if p.Amount <= 0 {
			return nil, ErrNonPositive
		}
		d := deltas[p.Account]
		var err error
		switch p.Side {
		case Debit:
			if dr, err = dr.Add(p.Amount); err != nil {
				return nil, ErrOverflow
			}
			if d.Dr, err = d.Dr.Add(p.Amount); err != nil {
				return nil, ErrOverflow
			}
		case Credit:
			if cr, err = cr.Add(p.Amount); err != nil {
				return nil, ErrOverflow
			}
			if d.Cr, err = d.Cr.Add(p.Amount); err != nil {
				return nil, ErrOverflow
			}
		default:
			return nil, ErrBadSide
		}
		deltas[p.Account] = d
	}
	if dr != cr {
		return nil, ErrUnbalanced
	}
	return deltas, nil
}

// sortedIDs returns the accounts in ascending order: the lock order.
func sortedIDs(deltas map[AccountID]delta) []int64 {
	ids := make([]int64, 0, len(deltas))
	for id := range deltas {
		ids = append(ids, int64(id))
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// apply returns b with d added to the pending or posted totals.
func apply(b Balances, d delta, pending bool) (Balances, error) {
	var err error
	if pending {
		if b.PendingDr, err = b.PendingDr.Add(d.Dr); err != nil {
			return b, ErrOverflow
		}
		if b.PendingCr, err = b.PendingCr.Add(d.Cr); err != nil {
			return b, ErrOverflow
		}
		return b, nil
	}
	if b.PostedDr, err = b.PostedDr.Add(d.Dr); err != nil {
		return b, ErrOverflow
	}
	if b.PostedCr, err = b.PostedCr.Add(d.Cr); err != nil {
		return b, ErrOverflow
	}
	return b, nil
}

// checkFlags reports whether balances b (after an entry) satisfy flags.
// Pending debits count against DebitsMustNotExceedCredits but pending
// credits do not count in its favour: money on hold is reserved, money
// still arriving is not yet spendable.
func checkFlags(id AccountID, f Flags, b Balances) error {
	if f&DebitsMustNotExceedCredits != 0 {
		total, err := b.PendingDr.Add(b.PostedDr)
		if err != nil || total > b.PostedCr {
			return flagError(ErrInsufficientFunds, id)
		}
	}
	if f&CreditsMustNotExceedDebits != 0 {
		total, err := b.PendingCr.Add(b.PostedCr)
		if err != nil || total > b.PostedDr {
			return flagError(ErrExceedsDebits, id)
		}
	}
	return nil
}
