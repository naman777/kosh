package payments

import (
	"context"
	"errors"
	"net/http"

	"github.com/jackc/pgx/v5"

	"github.com/naman777/kosh/internal/idempotency"
)

// ReplayInput is POST /v1/webhooks/replay. With no range, every undelivered
// event is retried now. With a range, those events are delivered again even
// if they were delivered before.
type ReplayInput struct {
	FromEventID *int64 `json:"from_event_id"`
	ToEventID   *int64 `json:"to_event_id"`
}

// ReplayResult says how many deliveries were scheduled.
type ReplayResult struct {
	Replayed int64 `json:"replayed"`
}

// ReplayWebhooks re-enables a merchant's webhooks and schedules deliveries.
func (s *Service) ReplayWebhooks(ctx context.Context, k idempotency.Key) (Response, error) {
	return s.idempotent(ctx, k, func(rec *idempotency.Record) (Response, error) {
		in, err := decode[ReplayInput](k.Body)
		if err != nil {
			return Response{}, err
		}
		if (in.FromEventID == nil) != (in.ToEventID == nil) || (in.FromEventID != nil && *in.FromEventID > *in.ToEventID) {
			return Response{}, errorf(http.StatusBadRequest, "bad_request", "give both from_event_id and to_event_id, in order, or neither")
		}
		return s.finish(ctx, rec, http.StatusOK, func(tx pgx.Tx) (any, error) {
			now := s.clock.Now()
			var url *string
			err := tx.QueryRow(ctx, `
				UPDATE merchants SET webhook_disabled_at = NULL, webhook_failing_since = NULL
				WHERE id = $1 RETURNING webhook_url`, k.MerchantID).Scan(&url)
			if errors.Is(err, pgx.ErrNoRows) || (err == nil && url == nil) {
				return nil, errorf(http.StatusConflict, "no_webhook_url", "this merchant has no webhook URL")
			}
			if err != nil {
				return nil, err
			}
			var res ReplayResult
			if in.FromEventID == nil {
				tag, err := tx.Exec(ctx, `
					UPDATE webhook_deliveries SET next_attempt_at = $2
					WHERE merchant_id = $1 AND delivered_at IS NULL`, k.MerchantID, now)
				if err != nil {
					return nil, err
				}
				res.Replayed = tag.RowsAffected()
				return res, nil
			}
			tag, err := tx.Exec(ctx, `
				INSERT INTO webhook_deliveries (event_id, merchant_id, next_attempt_at, created_at)
				SELECT id, merchant_id, $4, $4 FROM outbox
				WHERE merchant_id = $1 AND id BETWEEN $2 AND $3
				ON CONFLICT (event_id, merchant_id)
				DO UPDATE SET delivered_at = NULL, next_attempt_at = EXCLUDED.next_attempt_at`,
				k.MerchantID, *in.FromEventID, *in.ToEventID, now)
			if err != nil {
				return nil, err
			}
			res.Replayed = tag.RowsAffected()
			return res, nil
		})
	})
}
