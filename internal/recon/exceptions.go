package recon

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/naman777/kosh/internal/ledger"
	"github.com/naman777/kosh/internal/money"
)

// Result is a recon_results row.
type Result struct {
	ID              int64        `json:"id"`
	FileID          uuid.UUID    `json:"file_id"`
	LineNo          *int         `json:"line_no"`
	BankRef         *string      `json:"bank_ref"`
	ItemKind        *string      `json:"item_kind"`
	ItemID          *uuid.UUID   `json:"item_id"`
	Status          string       `json:"status"`
	ExpectedGross   *money.Paise `json:"expected_gross"`
	ActualGross     *money.Paise `json:"actual_gross"`
	ExpectedFee     *money.Paise `json:"expected_fee"`
	ActualFee       *money.Paise `json:"actual_fee"`
	Suspense        money.Paise  `json:"suspense"`
	Resolution      *string      `json:"resolution"`
	ResolvedByEntry *uuid.UUID   `json:"resolved_by_entry"`
	ResolvedAt      *time.Time   `json:"resolved_at"`
	CreatedAt       time.Time    `json:"created_at"`
}

const resultCols = `id, file_id, line_no, bank_ref, item_kind, item_id, status, expected_gross, actual_gross,
	expected_fee, actual_fee, suspense, resolution, resolved_by_entry, resolved_at, created_at`

func scanResult(row pgx.Row) (Result, error) {
	var r Result
	err := row.Scan(&r.ID, &r.FileID, &r.LineNo, &r.BankRef, &r.ItemKind, &r.ItemID, &r.Status, &r.ExpectedGross,
		&r.ActualGross, &r.ExpectedFee, &r.ActualFee, &r.Suspense, &r.Resolution, &r.ResolvedByEntry, &r.ResolvedAt, &r.CreatedAt)
	return r, err
}

// Exceptions returns open exceptions, oldest first.
func (e *Engine) Exceptions(ctx context.Context, limit int) ([]Result, error) {
	rows, err := e.pool.Query(ctx, `SELECT `+resultCols+` FROM recon_results
		WHERE status <> 'matched' AND resolved_at IS NULL ORDER BY created_at, id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Result
	for rows.Next() {
		r, err := scanResult(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Resolve closes an exception with an adjusting entry:
//
//   - write_off: the difference goes to recon_writeoff. For missing_in_bank
//     that is the whole receivable the bank never paid.
//   - bank_corrected: the bank paid (or took back) the difference, so it
//     moves between recon_suspense and bank_settlement. Not valid for
//     missing_in_bank, which closes itself when the item turns up.
//
// Each exception resolves once; the entry is linked from the result.
func (e *Engine) Resolve(ctx context.Context, id int64, action string) (Result, error) {
	var out Result
	err := pgx.BeginFunc(ctx, e.pool, func(tx pgx.Tx) error {
		r, err := scanResult(tx.QueryRow(ctx, `SELECT `+resultCols+` FROM recon_results WHERE id = $1 FOR UPDATE`, id))
		if err != nil {
			return err
		}
		if r.Status == Matched || r.ResolvedAt != nil {
			return ErrNotOpen
		}

		deltas := map[ledger.AccountID]money.Paise{}
		switch {
		case r.Status == MissingInBank && action == WriteOff:
			deltas[e.sys[ledger.ReconWriteoff]] += *r.ExpectedGross
			deltas[e.sys[ledger.BankReceivable]] -= *r.ExpectedGross
		case r.Status != MissingInBank && action == WriteOff:
			deltas[e.sys[ledger.ReconWriteoff]] += r.Suspense
			deltas[e.sys[ledger.ReconSuspense]] -= r.Suspense
		case r.Status != MissingInBank && action == BankCorrected:
			deltas[e.sys[ledger.BankSettlement]] += r.Suspense
			deltas[e.sys[ledger.ReconSuspense]] -= r.Suspense
		default:
			return ErrBadResolution
		}

		var entry *uuid.UUID
		if ps := signedPostings(deltas); len(ps) >= 2 {
			eid := e.ids.New()
			if err := e.ledger.Post(ctx, tx, ledger.Entry{ID: eid, Kind: "recon_adjustment", RefID: e.ids.New(), Postings: ps}); err != nil {
				return err
			}
			entry = &eid
		}
		out, err = scanResult(tx.QueryRow(ctx, `
			UPDATE recon_results SET resolution = $2, resolved_by_entry = $3, resolved_at = $4
			WHERE id = $1 RETURNING `+resultCols, id, action, entry, e.clock.Now()))
		return err
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Result{}, ErrNotOpen
	}
	return out, err
}
