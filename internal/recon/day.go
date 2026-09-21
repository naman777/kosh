package recon

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/naman777/kosh/internal/bank"
)

// ReconcileDay fetches the bank's file for date (YYYY-MM-DD), ingests it
// and reconciles it. Safe to repeat: a file already ingested is not fetched
// again, and a reconciled one returns its report.
func (e *Engine) ReconcileDay(ctx context.Context, src bank.SettlementSource, date string) (Report, error) {
	var id uuid.UUID
	err := e.pool.QueryRow(ctx, `SELECT id FROM settlement_files WHERE business_date = $1::date`, date).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		raw, err := src.SettlementFile(ctx, date)
		if err != nil {
			return Report{}, err
		}
		if id, err = e.Ingest(ctx, date, raw); err != nil {
			return Report{}, err
		}
	} else if err != nil {
		return Report{}, err
	}
	return e.Reconcile(ctx, id)
}
