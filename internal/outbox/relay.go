package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/naman777/kosh/internal/bus"
	"github.com/naman777/kosh/internal/clock"
	"github.com/naman777/kosh/internal/crash"
)

// Relay copies unpublished outbox rows to the bus.
//
// Each batch is claimed with FOR UPDATE SKIP LOCKED, so several relays can
// run without publishing the same row at once. Rows are marked published
// only after the bus acknowledged them; a crash in between republishes them
// on restart (at-least-once). With more than one relay, events of different
// batches can interleave; receivers order by the envelope's version.
type Relay struct {
	Pool      *pgxpool.Pool
	Publisher bus.Publisher
	Clock     clock.Clock
	BatchSize int
	Idle      time.Duration // sleep when there is nothing to publish
}

// RunOnce publishes up to one batch and returns how many rows it published.
func (r *Relay) RunOnce(ctx context.Context) (int, error) {
	n := 0
	err := pgx.BeginFunc(ctx, r.Pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id, aggregate_id, aggregate_version, merchant_id, event_type, payload, created_at
			FROM outbox
			WHERE published_at IS NULL
			ORDER BY id
			LIMIT $1
			FOR UPDATE SKIP LOCKED`, r.batch())
		if err != nil {
			return err
		}
		var (
			msgs []bus.Message
			ids  []int64
		)
		for rows.Next() {
			var (
				env     Envelope
				id      int64
				aggID   uuid.UUID
				payload []byte
			)
			if err := rows.Scan(&id, &aggID, &env.Version, &env.MerchantID, &env.Type, &payload, &env.CreatedAt); err != nil {
				rows.Close()
				return err
			}
			env.ID, env.AggregateID, env.Data = EventID(id), aggID, payload
			raw, err := json.Marshal(env)
			if err != nil {
				rows.Close()
				return err
			}
			key, _ := aggID.MarshalText()
			msgs = append(msgs, bus.Message{Key: key, Value: raw, EventID: id})
			ids = append(ids, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if len(msgs) == 0 {
			return nil
		}

		crash.At(ctx, "relay.before_publish")
		if err := r.Publisher.Publish(ctx, msgs); err != nil {
			return fmt.Errorf("relay: publish: %w", err)
		}
		crash.At(ctx, "relay.after_publish") // published but not marked: the duplicate window

		if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = $2 WHERE id = ANY($1)`, ids, r.Clock.Now()); err != nil {
			return err
		}
		n = len(ids)
		return nil
	})
	return n, err
}

// Run publishes until ctx ends.
func (r *Relay) Run(ctx context.Context) error {
	idle := r.Idle
	if idle == 0 {
		idle = 50 * time.Millisecond
	}
	for {
		n, err := r.RunOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil || n == 0 {
			// On error, back off and retry: rows stay unpublished, nothing is lost.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(idle):
			}
		}
	}
}

func (r *Relay) batch() int {
	if r.BatchSize <= 0 {
		return 100
	}
	return r.BatchSize
}

// Lag returns the number of unpublished rows and the age of the oldest.
func Lag(ctx context.Context, pool *pgxpool.Pool, now time.Time) (int, time.Duration, error) {
	var (
		n      int
		oldest *time.Time
	)
	err := pool.QueryRow(ctx, `SELECT count(*), min(created_at) FROM outbox WHERE published_at IS NULL`).Scan(&n, &oldest)
	if err != nil || oldest == nil {
		return n, 0, err
	}
	return n, now.Sub(*oldest), nil
}
