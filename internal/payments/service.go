// Package payments implements merchants, orders, payments and refunds on top
// of the ledger.
//
// Every write endpoint is idempotent (see package idempotency) and split
// into phases: local work in one transaction, then the bank call, then the
// result in a second transaction that also stores the response. A crash
// between phases is recovered by retrying with the same key.
package payments

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/naman777/kosh/internal/bank"
	"github.com/naman777/kosh/internal/clock"
	"github.com/naman777/kosh/internal/idempotency"
	"github.com/naman777/kosh/internal/ids"
	"github.com/naman777/kosh/internal/ledger"
)

// Config tunes the service.
type Config struct {
	// BankTimeout bounds each bank call. A timeout leaves the payment or
	// refund pending, never failed.
	BankTimeout time.Duration
	// RefundHoldTTL is how long a refund's hold may stay pending.
	RefundHoldTTL time.Duration
}

// DefaultConfig matches the build plan: a 30 s bank timeout.
func DefaultConfig() Config {
	return Config{BankTimeout: 30 * time.Second, RefundHoldTTL: 72 * time.Hour}
}

// Deps are the service's collaborators. Tests and the simulation harness
// swap Bank, Clock and IDs for deterministic versions.
type Deps struct {
	Pool   *pgxpool.Pool
	Bank   bank.Client
	Clock  clock.Clock
	IDs    ids.Generator
	Config Config
}

// Service is safe for concurrent use.
type Service struct {
	pool   *pgxpool.Pool
	bank   bank.Client
	clock  clock.Clock
	ids    ids.Generator
	cfg    Config
	ledger *ledger.Ledger
	idem   *idempotency.Store
	sys    map[string]ledger.AccountID
}

// New builds a Service and makes sure the system accounts exist.
func New(ctx context.Context, d Deps) (*Service, error) {
	if d.Clock == nil {
		d.Clock = clock.Real{}
	}
	if d.IDs == nil {
		d.IDs = ids.Random{}
	}
	if d.Config == (Config{}) {
		d.Config = DefaultConfig()
	}
	l := ledger.New(d.Clock)
	sys, err := l.EnsureSystemAccounts(ctx, d.Pool)
	if err != nil {
		return nil, err
	}
	return &Service{
		pool: d.Pool, bank: d.Bank, clock: d.Clock, ids: d.IDs, cfg: d.Config,
		ledger: l, idem: idempotency.NewStore(d.Clock, d.IDs), sys: sys,
	}, nil
}

// Idempotency exposes the key store, so tests can shorten its lock timeout.
func (s *Service) Idempotency() *idempotency.Store { return s.idem }

// Ledger exposes the ledger.
func (s *Service) Ledger() *ledger.Ledger { return s.ledger }

// Response is an HTTP status and the exact JSON body to send.
type Response struct {
	Status int
	Body   []byte
}

// Error is a deterministic client error. Errors raised after a key is
// acquired are stored like any response, so a retry gets the same answer.
type Error struct {
	Status  int    `json:"-"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// Body is the JSON error envelope.
func (e *Error) Body() any { return map[string]*Error{"error": e} }

func errorf(status int, code, format string, args ...any) *Error {
	return &Error{Status: status, Code: code, Message: fmt.Sprintf(format, args...)}
}

// idempotent wraps an operation in Acquire, replay, and error storage.
func (s *Service) idempotent(ctx context.Context, k idempotency.Key, run func(*idempotency.Record) (Response, error)) (Response, error) {
	rec, err := s.idem.Acquire(ctx, s.pool, k)
	switch {
	case errors.Is(err, idempotency.ErrMismatch):
		return Response{}, errorf(http.StatusUnprocessableEntity, "idempotency_key_reused",
			"this Idempotency-Key was already used with a different request")
	case errors.Is(err, idempotency.ErrInFlight):
		return Response{}, errorf(http.StatusConflict, "idempotency_key_in_use",
			"a request with this Idempotency-Key is still in progress; retry later")
	case err != nil:
		return Response{}, err
	}
	if rec.Done {
		return Response{Status: rec.ResponseCode, Body: rec.Response}, nil
	}

	resp, err := run(rec)
	var apiErr *Error
	if errors.As(err, &apiErr) {
		var raw []byte
		ferr := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
			var err error
			raw, err = s.idem.Finish(ctx, tx, rec, apiErr.Status, apiErr.Body())
			return err
		})
		if ferr != nil {
			_ = s.idem.Release(context.WithoutCancel(ctx), s.pool, rec)
			return Response{}, ferr
		}
		return Response{Status: apiErr.Status, Body: raw}, nil
	}
	if err != nil {
		// Transient failure: free the key so a retry resumes immediately.
		_ = s.idem.Release(context.WithoutCancel(ctx), s.pool, rec)
		return Response{}, err
	}
	return resp, nil
}

// finish runs fn and stores the response in the same transaction.
func (s *Service) finish(ctx context.Context, rec *idempotency.Record, status int, fn func(tx pgx.Tx) (any, error)) (Response, error) {
	var raw []byte
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		body, err := fn(tx)
		if err != nil {
			return err
		}
		raw, err = s.idem.Finish(ctx, tx, rec, status, body)
		return err
	})
	if err != nil {
		return Response{}, err
	}
	return Response{Status: status, Body: raw}, nil
}

func decode[T any](body []byte) (T, error) {
	var v T
	if len(body) == 0 {
		return v, nil
	}
	if err := json.Unmarshal(body, &v); err != nil {
		return v, errorf(http.StatusBadRequest, "bad_request", "invalid JSON: %v", err)
	}
	return v, nil
}
