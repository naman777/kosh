// Package idempotency makes POST requests safe to retry.
//
// It follows the "atomic phases" pattern. A request first Acquires its key,
// which takes a lease on it. The handler then runs in phases: each local
// phase is one database transaction that ends with Advance (recording a
// recovery point) or Finish (storing the response), in the same transaction
// as the business writes. External calls (the bank) happen between phases
// and are themselves idempotent on Kosh's ids.
//
// What a retry with the same key sees:
//
//   - same body, request finished: the stored response, byte for byte
//   - same body, request still running: ErrInFlight (409)
//   - same body, request died mid-way (lease older than LockTimeout): it
//     takes over the lease and resumes from the recovery point
//   - different body: ErrMismatch (422), and nothing is written
//
// Every write after Acquire is fenced by the lease id, so a request that
// lost its lease to a takeover can never commit.
package idempotency

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/naman777/kosh/internal/clock"
	"github.com/naman777/kosh/internal/ids"
)

// RecoveryStarted is the recovery point of a freshly acquired key.
const RecoveryStarted = "started"

var (
	// ErrMismatch means the key was used before with a different request.
	ErrMismatch = errors.New("idempotency: key reused with a different request")
	// ErrInFlight means another request with this key holds a live lease.
	ErrInFlight = errors.New("idempotency: a request with this key is in progress")
	// ErrLockLost means another request took over this key's lease.
	ErrLockLost = errors.New("idempotency: lease lost to another request")
)

// Key identifies a request.
type Key struct {
	MerchantID uuid.UUID
	Key        string
	Method     string
	Path       string
	Body       []byte
}

// Hash is SHA-256 over method, path and the raw body.
func (k Key) Hash() []byte {
	h := sha256.New()
	h.Write([]byte(k.Method))
	h.Write([]byte{'\n'})
	h.Write([]byte(k.Path))
	h.Write([]byte{'\n'})
	h.Write(k.Body)
	return h.Sum(nil)
}

// Record is an acquired key.
type Record struct {
	MerchantID    uuid.UUID
	Key           string
	RecoveryPoint string
	ResourceID    uuid.NullUUID

	// Done is set when the request already finished; ResponseCode and
	// Response hold what to replay.
	Done         bool
	ResponseCode int
	Response     []byte

	lease uuid.UUID
}

// Beginner starts transactions (*pgxpool.Pool, *pgx.Conn).
type Beginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Store manages idempotency keys.
type Store struct {
	clock clock.Clock
	ids   ids.Generator
	// LockTimeout is how long a lease is honoured before a retry may take
	// it over. It must exceed the longest external call a phase makes.
	LockTimeout time.Duration
}

// NewStore returns a Store with a 60 s lock timeout.
func NewStore(c clock.Clock, g ids.Generator) *Store {
	return &Store{clock: c, ids: g, LockTimeout: 60 * time.Second}
}

// Acquire takes the lease on k, or returns the finished response, or one of
// ErrMismatch and ErrInFlight.
func (s *Store) Acquire(ctx context.Context, db Beginner, k Key) (*Record, error) {
	var rec *Record
	err := pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		now := s.clock.Now()
		lease := s.ids.New()
		hash := k.Hash()
		tag, err := tx.Exec(ctx, `
			INSERT INTO idempotency_keys
			  (merchant_id, key, request_method, request_path, request_hash, status,
			   lease_id, locked_at, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, 'in_progress', $6, $7, $7, $7)
			ON CONFLICT DO NOTHING`,
			k.MerchantID, k.Key, k.Method, k.Path, hash, lease, now)
		if err != nil {
			return fmt.Errorf("idempotency: insert: %w", err)
		}
		if tag.RowsAffected() == 1 {
			rec = &Record{MerchantID: k.MerchantID, Key: k.Key, RecoveryPoint: RecoveryStarted, lease: lease}
			return nil
		}

		// The key exists. A concurrent inserter has committed by now: the
		// INSERT above waited on it.
		var (
			r        Record
			status   string
			gotHash  []byte
			lockedAt *time.Time
			code     *int
		)
		err = tx.QueryRow(ctx, `
			SELECT request_hash, status, recovery_point, resource_id, locked_at, response_code, response
			FROM idempotency_keys WHERE merchant_id = $1 AND key = $2
			FOR UPDATE`, k.MerchantID, k.Key).Scan(
			&gotHash, &status, &r.RecoveryPoint, &r.ResourceID, &lockedAt, &code, &r.Response)
		if err != nil {
			return fmt.Errorf("idempotency: load: %w", err)
		}
		if !bytes.Equal(gotHash, hash) {
			return ErrMismatch
		}
		r.MerchantID, r.Key = k.MerchantID, k.Key
		if status == "done" {
			r.Done, r.ResponseCode = true, *code
			rec = &r
			return nil
		}
		if lockedAt != nil && now.Sub(*lockedAt) < s.LockTimeout {
			return ErrInFlight
		}
		// The previous owner died or released the key: take over.
		if _, err := tx.Exec(ctx, `
			UPDATE idempotency_keys SET lease_id = $3, locked_at = $4, updated_at = $4
			WHERE merchant_id = $1 AND key = $2`, k.MerchantID, k.Key, lease, now); err != nil {
			return fmt.Errorf("idempotency: take over: %w", err)
		}
		r.lease = lease
		rec = &r
		return nil
	})
	return rec, err
}

// Advance records a recovery point (and the resource it created) inside the
// phase's transaction.
func (s *Store) Advance(ctx context.Context, tx pgx.Tx, r *Record, point string, resource uuid.UUID) error {
	tag, err := tx.Exec(ctx, `
		UPDATE idempotency_keys
		SET recovery_point = $4, resource_id = COALESCE($5, resource_id), updated_at = $6
		WHERE merchant_id = $1 AND key = $2 AND lease_id = $3 AND status = 'in_progress'`,
		r.MerchantID, r.Key, r.lease, point, uuid.NullUUID{UUID: resource, Valid: resource != uuid.Nil}, s.clock.Now())
	if err != nil {
		return fmt.Errorf("idempotency: advance: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLockLost
	}
	r.RecoveryPoint = point
	if resource != uuid.Nil {
		r.ResourceID = uuid.NullUUID{UUID: resource, Valid: true}
	}
	return nil
}

// Finish stores the response inside the final phase's transaction and
// returns the exact bytes to send, which are also what every replay sends.
func (s *Store) Finish(ctx context.Context, tx pgx.Tx, r *Record, code int, body any) ([]byte, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE idempotency_keys
		SET status = 'done', response_code = $4, response = $5, lease_id = NULL, locked_at = NULL, updated_at = $6
		WHERE merchant_id = $1 AND key = $2 AND lease_id = $3 AND status = 'in_progress'`,
		r.MerchantID, r.Key, r.lease, code, raw, s.clock.Now())
	if err != nil {
		return nil, fmt.Errorf("idempotency: finish: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return nil, ErrLockLost
	}
	r.Done, r.ResponseCode, r.Response = true, code, raw
	return raw, nil
}

// Release drops the lease without finishing, after a transient failure, so
// a retry can resume at once instead of waiting out LockTimeout.
func (s *Store) Release(ctx context.Context, db Beginner, r *Record) error {
	return pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			UPDATE idempotency_keys SET lease_id = NULL, locked_at = NULL, updated_at = $4
			WHERE merchant_id = $1 AND key = $2 AND lease_id = $3 AND status = 'in_progress'`,
			r.MerchantID, r.Key, r.lease, s.clock.Now())
		return err
	})
}
