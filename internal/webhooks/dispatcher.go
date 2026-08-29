package webhooks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/naman777/kosh/internal/bus"
	"github.com/naman777/kosh/internal/clock"
	"github.com/naman777/kosh/internal/crash"
	"github.com/naman777/kosh/internal/outbox"
	"github.com/naman777/kosh/internal/rng"
)

// Sender makes one delivery attempt. Swapped out in tests and simulation.
type Sender interface {
	Send(ctx context.Context, url string, header http.Header, body []byte) (status int, err error)
}

// HTTPSender sends over the network.
type HTTPSender struct{ Client *http.Client }

// Send implements Sender.
func (s HTTPSender) Send(ctx context.Context, url string, header http.Header, body []byte) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header = header
	c := s.Client
	if c == nil {
		c = http.DefaultClient
	}
	resp, err := c.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	return resp.StatusCode, nil
}

// HandlerSender delivers straight into an http.Handler, with no network.
type HandlerSender struct{ Handler http.Handler }

// Send implements Sender.
func (s HandlerSender) Send(ctx context.Context, url string, header http.Header, body []byte) (int, error) {
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	req.Header = header
	rec := httptest.NewRecorder()
	s.Handler.ServeHTTP(rec, req)
	if err := ctx.Err(); err != nil {
		return 0, err // the handler hung past the send timeout
	}
	return rec.Code, nil
}

// Dispatcher turns events into deliveries and delivers them.
type Dispatcher struct {
	Pool   *pgxpool.Pool
	Clock  clock.Clock
	RNG    rng.RNG
	Sender Sender

	SendTimeout  time.Duration // per attempt; default 10 s
	BaseDelay    time.Duration // first retry delay; default 10 s
	MaxDelay     time.Duration // retry delay cap; default 1 h
	DisableAfter time.Duration // pause a merchant failing this long; default 24 h
	BatchSize    int           // deliveries claimed per round; default 50
	Concurrency  int           // parallel sends; default 16
}

func (d *Dispatcher) defaults() {
	if d.SendTimeout == 0 {
		d.SendTimeout = 10 * time.Second
	}
	if d.BaseDelay == 0 {
		d.BaseDelay = 10 * time.Second
	}
	if d.MaxDelay == 0 {
		d.MaxDelay = time.Hour
	}
	if d.DisableAfter == 0 {
		d.DisableAfter = 24 * time.Hour
	}
	if d.BatchSize == 0 {
		d.BatchSize = 50
	}
	if d.Concurrency == 0 {
		d.Concurrency = 16
	}
}

// Backoff is the delay before retry number attempt (1-based): exponential
// from BaseDelay, capped at MaxDelay, with "equal jitter" (half fixed, half
// random) so retries spread out but never collapse to zero.
func (d *Dispatcher) Backoff(attempt int) time.Duration {
	d.defaults()
	delay := d.MaxDelay
	if attempt < 32 {
		if e := d.BaseDelay << (attempt - 1); e > 0 && e < d.MaxDelay {
			delay = e
		}
	}
	half := delay / 2
	return half + time.Duration(d.RNG.Int64N(int64(half)+1))
}

// Ingest records one delivery per event for its merchant, if the merchant
// has a webhook URL. Already-known events are ignored, so a redelivered
// batch after a crash is harmless.
func (d *Dispatcher) Ingest(ctx context.Context, recs []bus.Record) error {
	var (
		eventIDs  []int64
		merchants []uuid.UUID
	)
	for _, r := range recs {
		var env outbox.Envelope
		if err := json.Unmarshal(r.Value, &env); err != nil {
			return fmt.Errorf("dispatcher: bad envelope at event %d: %w", r.EventID, err)
		}
		id, err := outbox.ParseEventID(env.ID)
		if err != nil {
			return err
		}
		eventIDs = append(eventIDs, id)
		merchants = append(merchants, env.MerchantID)
	}
	if len(eventIDs) == 0 {
		return nil
	}
	now := d.Clock.Now()
	_, err := d.Pool.Exec(ctx, `
		INSERT INTO webhook_deliveries (event_id, merchant_id, next_attempt_at, created_at)
		SELECT e.id, m.id, $3, $3
		FROM unnest($1::bigint[], $2::uuid[]) AS e(id, merchant)
		JOIN merchants m ON m.id = e.merchant AND m.webhook_url IS NOT NULL
		ON CONFLICT DO NOTHING`, eventIDs, merchants, now)
	if err != nil {
		return fmt.Errorf("dispatcher: ingest: %w", err)
	}
	return nil
}

// RunIngest consumes the bus until ctx ends, committing offsets only after
// the deliveries are in the database.
func (d *Dispatcher) RunIngest(ctx context.Context, c bus.Consumer) error {
	for {
		if _, err := d.IngestOnce(ctx, c); err != nil {
			return err
		}
	}
}

// IngestOnce polls one batch, records it, then commits its offsets.
func (d *Dispatcher) IngestOnce(ctx context.Context, c bus.Consumer) (int, error) {
	recs, err := c.Poll(ctx)
	if err != nil {
		return 0, err
	}
	if err := d.Ingest(ctx, recs); err != nil {
		return 0, err
	}
	crash.Point("dispatcher.after_ingest") // in the DB, offsets not committed: redelivered on restart
	return len(recs), c.Commit(ctx, recs)
}

type claimed struct {
	eventID    int64
	merchantID uuid.UUID
	attempt    int
	url        string
	secret     string
	body       []byte
}

// SendOnce claims due deliveries, sends them, and records the results. It
// returns how many it attempted.
//
// A claim bumps next_attempt_at by a lease (twice the send timeout) and
// commits before any network I/O, so no row lock is held during sends. If the
// dispatcher dies mid-send, the lease runs out and the delivery is retried:
// the merchant may see an event twice, never zero times.
func (d *Dispatcher) SendOnce(ctx context.Context) (int, error) {
	d.defaults()
	batch, err := d.claim(ctx)
	if err != nil || len(batch) == 0 {
		return 0, err
	}
	crash.Point("dispatcher.after_claim")

	type result struct {
		status int
		err    error
	}
	results := make([]result, len(batch))
	sem := make(chan struct{}, d.Concurrency)
	var wg sync.WaitGroup
	for i, c := range batch {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, c claimed) {
			defer func() { <-sem; wg.Done() }()
			h := http.Header{}
			h.Set("Content-Type", "application/json")
			h.Set("User-Agent", "Kosh-Webhooks/1")
			h.Set(HeaderEventID, outbox.EventID(c.eventID))
			h.Set(HeaderAttempt, strconv.Itoa(c.attempt))
			h.Set(HeaderSignature, Sign(c.secret, d.Clock.Now(), c.body))
			sctx, cancel := context.WithTimeout(ctx, d.SendTimeout)
			status, err := d.Sender.Send(sctx, c.url, h, c.body)
			cancel()
			results[i] = result{status, err}
		}(i, c)
	}
	wg.Wait()
	crash.Point("dispatcher.after_send") // merchant has it, we haven't recorded it

	for i, c := range batch {
		if err := d.record(ctx, c, results[i].status, results[i].err); err != nil {
			return i, err
		}
	}
	return len(batch), nil
}

func (d *Dispatcher) claim(ctx context.Context) ([]claimed, error) {
	var out []claimed
	err := pgx.BeginFunc(ctx, d.Pool, func(tx pgx.Tx) error {
		now := d.Clock.Now()
		rows, err := tx.Query(ctx, `
			SELECT d.event_id, d.merchant_id, d.attempt + 1, m.webhook_url, m.webhook_secret,
			       o.aggregate_id, o.aggregate_version, o.event_type, o.payload, o.created_at
			FROM webhook_deliveries d
			JOIN outbox o ON o.id = d.event_id
			JOIN merchants m ON m.id = d.merchant_id
			WHERE d.delivered_at IS NULL AND d.next_attempt_at <= $1
			  AND m.webhook_disabled_at IS NULL AND m.webhook_url IS NOT NULL
			ORDER BY d.next_attempt_at
			LIMIT $2
			FOR UPDATE OF d SKIP LOCKED`, now, d.BatchSize)
		if err != nil {
			return err
		}
		for rows.Next() {
			var (
				c       claimed
				env     outbox.Envelope
				payload []byte
			)
			if err := rows.Scan(&c.eventID, &c.merchantID, &c.attempt, &c.url, &c.secret,
				&env.AggregateID, &env.Version, &env.Type, &payload, &env.CreatedAt); err != nil {
				rows.Close()
				return err
			}
			env.ID, env.MerchantID, env.Data = outbox.EventID(c.eventID), c.merchantID, payload
			if c.body, err = json.Marshal(env); err != nil {
				rows.Close()
				return err
			}
			out = append(out, c)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		lease := now.Add(2 * d.SendTimeout)
		for _, c := range out {
			if _, err := tx.Exec(ctx, `
				UPDATE webhook_deliveries SET attempt = $3, next_attempt_at = $4
				WHERE event_id = $1 AND merchant_id = $2`, c.eventID, c.merchantID, c.attempt, lease); err != nil {
				return err
			}
		}
		return nil
	})
	return out, err
}

func (d *Dispatcher) record(ctx context.Context, c claimed, status int, sendErr error) error {
	now := d.Clock.Now()
	return pgx.BeginFunc(ctx, d.Pool, func(tx pgx.Tx) error {
		if sendErr == nil && status >= 200 && status < 300 {
			if _, err := tx.Exec(ctx, `
				UPDATE webhook_deliveries
				SET delivered_at = $3, next_attempt_at = NULL, last_status = $4, last_error = NULL
				WHERE event_id = $1 AND merchant_id = $2 AND delivered_at IS NULL`,
				c.eventID, c.merchantID, now, status); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `UPDATE merchants SET webhook_failing_since = NULL WHERE id = $1`, c.merchantID)
			return err
		}

		var lastErr *string
		if sendErr != nil {
			s := sendErr.Error()
			lastErr = &s
		}
		if _, err := tx.Exec(ctx, `
			UPDATE webhook_deliveries
			SET next_attempt_at = $3, last_status = $4, last_error = $5
			WHERE event_id = $1 AND merchant_id = $2 AND delivered_at IS NULL`,
			c.eventID, c.merchantID, now.Add(d.Backoff(c.attempt)), nullIfZero(status), lastErr); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			UPDATE merchants
			SET webhook_failing_since = COALESCE(webhook_failing_since, $2),
			    webhook_disabled_at = CASE
			      WHEN webhook_disabled_at IS NULL AND COALESCE(webhook_failing_since, $2) <= $3 THEN $2
			      ELSE webhook_disabled_at END
			WHERE id = $1`, c.merchantID, now, now.Add(-d.DisableAfter))
		return err
	})
}

func nullIfZero(n int) *int {
	if n == 0 {
		return nil
	}
	return &n
}

// RunSender sends until ctx ends, sleeping idle between empty rounds.
func (d *Dispatcher) RunSender(ctx context.Context, idle time.Duration) error {
	for {
		n, err := d.SendOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil || n == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(idle):
			}
		}
	}
}
