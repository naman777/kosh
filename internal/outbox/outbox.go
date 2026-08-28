// Package outbox implements the transactional outbox.
//
// State changes call Write inside their own transaction, so an event exists
// if and only if the change committed: there is no window where the database
// says "captured" and Kafka never hears about it, or the reverse. The Relay
// then copies unpublished rows to the message bus. If the relay dies after
// publishing but before marking rows published, they are published again on
// restart, so delivery is at-least-once and consumers dedupe by event id.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Event is one state change.
type Event struct {
	AggregateType string // payment | refund
	AggregateID   uuid.UUID
	Version       int // the aggregate's version after the change
	MerchantID    uuid.UUID
	Type          string // payment.captured ...
	Payload       any    // the resource as it is after the change
}

// Write inserts e in tx. The (aggregate_id, version) unique key makes a
// second event for the same change impossible.
func Write(ctx context.Context, tx pgx.Tx, now time.Time, e Event) (int64, error) {
	payload, err := json.Marshal(e.Payload)
	if err != nil {
		return 0, err
	}
	var id int64
	err = tx.QueryRow(ctx, `
		INSERT INTO outbox (aggregate_type, aggregate_id, aggregate_version, merchant_id, event_type, payload, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id`,
		e.AggregateType, e.AggregateID, e.Version, e.MerchantID, e.Type, payload, now).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("outbox: write %s: %w", e.Type, err)
	}
	return id, nil
}

// Envelope is the JSON every consumer and every merchant webhook receives.
type Envelope struct {
	ID          string          `json:"id"` // "evt_<outbox id>"
	Type        string          `json:"type"`
	AggregateID uuid.UUID       `json:"aggregate_id"`
	Version     int             `json:"version"` // lets receivers drop stale updates
	MerchantID  uuid.UUID       `json:"merchant_id"`
	CreatedAt   time.Time       `json:"created_at"`
	Data        json.RawMessage `json:"data"`
}

// EventID formats an outbox id as a public event id.
func EventID(id int64) string { return "evt_" + strconv.FormatInt(id, 10) }

// ParseEventID reverses EventID.
func ParseEventID(s string) (int64, error) {
	if len(s) < 5 || s[:4] != "evt_" {
		return 0, fmt.Errorf("outbox: bad event id %q", s)
	}
	return strconv.ParseInt(s[4:], 10, 64)
}
