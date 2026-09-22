package ledger

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Post records e as a posted entry inside tx.
func (l *Ledger) Post(ctx context.Context, tx pgx.Tx, e Entry) error {
	return l.write(ctx, tx, e, StatePosted, nil)
}

// Hold records e as a pending entry that reserves funds until it is committed,
// voided or expires at expiresAt.
func (l *Ledger) Hold(ctx context.Context, tx pgx.Tx, e Entry, expiresAt time.Time) error {
	if !expiresAt.After(l.clock.Now()) {
		return ErrBadExpiry
	}
	return l.write(ctx, tx, e, StatePending, &expiresAt)
}

func (l *Ledger) write(ctx context.Context, tx pgx.Tx, e Entry, state string, expiresAt *time.Time) error {
	deltas, err := validate(e)
	if err != nil {
		return err
	}

	// Insert the entry first. A duplicate id or (kind, ref_id) stops here,
	// before any account is locked.
	tag, err := tx.Exec(ctx, `
		INSERT INTO journal_entries (id, kind, ref_id, state, expires_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT DO NOTHING`,
		e.ID, e.Kind, e.RefID, state, expiresAt, l.clock.Now())
	if err != nil {
		return fmt.Errorf("ledger: insert entry: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrDuplicateEntry
	}

	pending := state == StatePending
	if err := l.lockAndApply(ctx, tx, deltas, func(b Balances, d delta) (Balances, error) {
		return apply(b, d, pending)
	}, true); err != nil {
		return err
	}

	accounts := make([]int64, len(e.Postings))
	sides := make([]string, len(e.Postings))
	amounts := make([]int64, len(e.Postings))
	for i, p := range e.Postings {
		accounts[i], sides[i], amounts[i] = int64(p.Account), string(p.Side), int64(p.Amount)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO postings (entry_id, account_id, side, amount)
		SELECT $1, a, s, m FROM unnest($2::bigint[], $3::text[], $4::bigint[]) AS t(a, s, m)`,
		e.ID, accounts, sides, amounts); err != nil {
		return fmt.Errorf("ledger: insert postings: %w", err)
	}

	col := "posted"
	if pending {
		col = "pending"
	}
	return incrementBalances(ctx, tx, deltas, col)
}

// lockAndApply locks every account in deltas in ascending id order, computes
// each account's balances after next, and checks flags when checkLimits is
// set. It writes nothing; the caller updates balances afterwards.
func (l *Ledger) lockAndApply(ctx context.Context, tx pgx.Tx, deltas map[AccountID]delta,
	next func(Balances, delta) (Balances, error), checkLimits bool) error {
	ids := sortedIDs(deltas)
	rows, err := tx.Query(ctx, `
		SELECT id, flags, posted_dr, posted_cr, pending_dr, pending_cr
		FROM accounts WHERE id = ANY($1)
		ORDER BY id
		FOR UPDATE`, ids)
	if err != nil {
		return fmt.Errorf("ledger: lock accounts: %w", err)
	}
	defer rows.Close()

	found := 0
	for rows.Next() {
		var (
			id    AccountID
			flags Flags
			b     Balances
		)
		if err := rows.Scan(&id, &flags, &b.PostedDr, &b.PostedCr, &b.PendingDr, &b.PendingCr); err != nil {
			return err
		}
		found++
		after, err := next(b, deltas[id])
		if err != nil {
			return err
		}
		if checkLimits {
			if err := checkFlags(id, flags, after); err != nil {
				return err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("ledger: lock accounts: %w", err)
	}
	if found != len(ids) {
		return ErrAccountNotFound
	}
	return nil
}

// incrementBalances adds deltas to the posted_* or pending_* columns in one
// statement. Rows are already locked, so update order does not matter.
func incrementBalances(ctx context.Context, tx pgx.Tx, deltas map[AccountID]delta, col string) error {
	ids, drs, crs := deltaArrays(deltas)
	_, err := tx.Exec(ctx, fmt.Sprintf(`
		UPDATE accounts a
		SET %[1]s_dr = a.%[1]s_dr + d.dr, %[1]s_cr = a.%[1]s_cr + d.cr
		FROM unnest($1::bigint[], $2::bigint[], $3::bigint[]) AS d(id, dr, cr)
		WHERE a.id = d.id`, col), ids, drs, crs)
	if err != nil {
		return fmt.Errorf("ledger: update balances: %w", err)
	}
	return nil
}

func deltaArrays(deltas map[AccountID]delta) (ids, drs, crs []int64) {
	ids = sortedIDs(deltas)
	drs = make([]int64, len(ids))
	crs = make([]int64, len(ids))
	for i, id := range ids {
		d := deltas[AccountID(id)]
		drs[i], crs[i] = int64(d.Dr), int64(d.Cr)
	}
	return ids, drs, crs
}

// CommitHold posts a pending hold: its pending totals move to posted. It
// fails with ErrHoldResolved if the hold was already posted or voided, and
// ErrHoldExpired if its deadline has passed.
func (l *Ledger) CommitHold(ctx context.Context, tx pgx.Tx, entryID uuid.UUID) error {
	return l.commit(ctx, tx, entryID, true)
}

// CommitConfirmed posts a pending hold even after its deadline. Use it when
// an outside party (the bank) confirms the money has moved: that is a fact
// the ledger must record, and the deadline only says when an unconfirmed
// hold may be given up. A hold already voided still fails, with
// ErrHoldResolved.
func (l *Ledger) CommitConfirmed(ctx context.Context, tx pgx.Tx, entryID uuid.UUID) error {
	return l.commit(ctx, tx, entryID, false)
}

func (l *Ledger) commit(ctx context.Context, tx pgx.Tx, entryID uuid.UUID, checkDeadline bool) error {
	now := l.clock.Now()
	sql := `UPDATE journal_entries SET state = 'posted', resolved_at = $2 WHERE id = $1 AND state = 'pending'`
	if checkDeadline {
		sql += ` AND expires_at > $2`
	}
	tag, err := tx.Exec(ctx, sql, entryID, now)
	if err != nil {
		return fmt.Errorf("ledger: commit hold: %w", err)
	}
	if tag.RowsAffected() == 0 {
		if !checkDeadline {
			now = time.Time{}
		}
		return l.whyNotResolvable(ctx, tx, entryID, now)
	}
	return l.releaseHold(ctx, tx, entryID, true)
}

// VoidHold cancels a pending hold: its pending totals are removed and nothing
// is posted. An expired hold can still be voided.
func (l *Ledger) VoidHold(ctx context.Context, tx pgx.Tx, entryID uuid.UUID) error {
	tag, err := tx.Exec(ctx, `
		UPDATE journal_entries SET state = 'voided', resolved_at = $2
		WHERE id = $1 AND state = 'pending'`, entryID, l.clock.Now())
	if err != nil {
		return fmt.Errorf("ledger: void hold: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return l.whyNotResolvable(ctx, tx, entryID, time.Time{})
	}
	return l.releaseHold(ctx, tx, entryID, false)
}

// releaseHold moves a just-resolved hold's postings out of pending_*, and
// into posted_* when commit is set.
func (l *Ledger) releaseHold(ctx context.Context, tx pgx.Tx, entryID uuid.UUID, commit bool) error {
	rows, err := tx.Query(ctx, `SELECT account_id, side, amount FROM postings WHERE entry_id = $1`, entryID)
	if err != nil {
		return fmt.Errorf("ledger: load hold postings: %w", err)
	}
	var ps []Posting
	for rows.Next() {
		var p Posting
		if err := rows.Scan(&p.Account, &p.Side, &p.Amount); err != nil {
			rows.Close()
			return err
		}
		ps = append(ps, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	deltas := make(map[AccountID]delta, len(ps))
	for _, p := range ps {
		d := deltas[p.Account]
		if p.Side == Debit {
			d.Dr += p.Amount
		} else {
			d.Cr += p.Amount
		}
		deltas[p.Account] = d
	}

	// Moving pending to posted never changes posted+pending totals, and
	// voiding only lowers them, so flags cannot be newly violated here.
	if err := l.lockAndApply(ctx, tx, deltas, func(b Balances, _ delta) (Balances, error) { return b, nil }, false); err != nil {
		return err
	}
	ids, drs, crs := deltaArrays(deltas)
	posted := "0"
	if commit {
		posted = "1"
	}
	_, err = tx.Exec(ctx, `
		UPDATE accounts a
		SET pending_dr = a.pending_dr - d.dr,
		    pending_cr = a.pending_cr - d.cr,
		    posted_dr  = a.posted_dr + d.dr * `+posted+`,
		    posted_cr  = a.posted_cr + d.cr * `+posted+`
		FROM unnest($1::bigint[], $2::bigint[], $3::bigint[]) AS d(id, dr, cr)
		WHERE a.id = d.id`, ids, drs, crs)
	if err != nil {
		return fmt.Errorf("ledger: release hold: %w", err)
	}
	return nil
}

// whyNotResolvable explains why a guarded hold update touched no row.
// A zero now skips the expiry check (voids ignore expiry).
func (l *Ledger) whyNotResolvable(ctx context.Context, q Querier, entryID uuid.UUID, now time.Time) error {
	var (
		state     string
		expiresAt *time.Time
	)
	err := q.QueryRow(ctx, `SELECT state, expires_at FROM journal_entries WHERE id = $1`, entryID).Scan(&state, &expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrEntryNotFound
	}
	if err != nil {
		return err
	}
	switch {
	case state == StatePending && !now.IsZero() && expiresAt != nil && !expiresAt.After(now):
		return ErrHoldExpired
	case expiresAt == nil:
		return ErrNotHold
	default:
		return ErrHoldResolved
	}
}

// ExpiredHold identifies a hold voided by ExpireHolds, so callers can react
// (for example, mark the refund that placed it as failed).
type ExpiredHold struct {
	ID    uuid.UUID
	Kind  string
	RefID uuid.UUID
}

// ExpireHolds voids up to limit pending holds whose deadline has passed.
// Rows another worker is already expiring are skipped, so several expirers
// can run at once.
func (l *Ledger) ExpireHolds(ctx context.Context, tx pgx.Tx, limit int) ([]ExpiredHold, error) {
	rows, err := tx.Query(ctx, `
		SELECT id, kind, ref_id FROM journal_entries
		WHERE state = 'pending' AND expires_at <= $1
		ORDER BY expires_at, id
		LIMIT $2
		FOR UPDATE SKIP LOCKED`, l.clock.Now(), limit)
	if err != nil {
		return nil, fmt.Errorf("ledger: find expired holds: %w", err)
	}
	var out []ExpiredHold
	for rows.Next() {
		var h ExpiredHold
		if err := rows.Scan(&h.ID, &h.Kind, &h.RefID); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, h)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, h := range out {
		if err := l.VoidHold(ctx, tx, h.ID); err != nil {
			return nil, err
		}
	}
	return out, nil
}
