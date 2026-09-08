package payments_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/naman777/kosh/internal/bank"
	"github.com/naman777/kosh/internal/clock"
	"github.com/naman777/kosh/internal/crash"
	"github.com/naman777/kosh/internal/idempotency"
	"github.com/naman777/kosh/internal/ids"
	"github.com/naman777/kosh/internal/money"
	"github.com/naman777/kosh/internal/payments"
	"github.com/naman777/kosh/internal/testdb"
	"github.com/naman777/kosh/invariants"
)

type env struct {
	t    *testing.T
	ctx  context.Context
	pool *pgxpool.Pool
	bank *bank.Fake
	clk  *clock.Fake
	svc  *payments.Service
	m    uuid.UUID // a merchant at 2% fee
	seq  int
}

func newEnv(t *testing.T) *env {
	t.Helper()
	t.Cleanup(crash.Disarm)
	ctx := context.Background()
	pool := testdb.New(t)
	e := &env{t: t, ctx: ctx, pool: pool, bank: bank.NewFake(),
		clk: clock.NewFake(time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC))}
	svc, err := payments.New(ctx, payments.Deps{
		Pool: pool, Bank: e.bank, Clock: e.clk, IDs: ids.NewSeeded(uint64(time.Now().UnixNano())),
		Config: payments.Config{BankTimeout: time.Second, RefundHoldTTL: 72 * time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	e.svc = svc
	e.m = e.merchant(200)
	return e
}

func (e *env) merchant(feeBps int) uuid.UUID {
	e.t.Helper()
	out, err := e.svc.CreateMerchant(e.ctx, payments.CreateMerchantInput{Name: "Test Merchant", FeeBps: &feeBps})
	if err != nil {
		e.t.Fatal(err)
	}
	return out.Merchant.ID
}

// key builds an idempotency key exactly as the HTTP layer would.
func (e *env) key(k, path string, body any) idempotency.Key {
	raw, err := json.Marshal(body)
	if err != nil {
		e.t.Fatal(err)
	}
	return idempotency.Key{MerchantID: e.m, Key: k, Method: "POST", Path: path, Body: raw}
}

func (e *env) newKey() string {
	e.seq++
	return fmt.Sprintf("key-%d", e.seq)
}

func mustDecode[T any](t *testing.T, r payments.Response, wantStatus int) T {
	t.Helper()
	if r.Status != wantStatus {
		t.Fatalf("status %d, want %d: %s", r.Status, wantStatus, r.Body)
	}
	var v T
	if err := json.Unmarshal(r.Body, &v); err != nil {
		t.Fatalf("decode %s: %v", r.Body, err)
	}
	return v
}

func errCode(t *testing.T, r payments.Response) string {
	t.Helper()
	var v struct {
		Error payments.Error `json:"error"`
	}
	if err := json.Unmarshal(r.Body, &v); err != nil {
		t.Fatalf("decode error %s: %v", r.Body, err)
	}
	return v.Error.Code
}

func (e *env) createOrder(amount money.Paise) payments.OrderView {
	e.t.Helper()
	r, err := e.svc.CreateOrder(e.ctx, e.key(e.newKey(), "/v1/orders", map[string]any{"amount": amount}))
	if err != nil {
		e.t.Fatal(err)
	}
	return mustDecode[payments.OrderView](e.t, r, 201)
}

func (e *env) payKey(k string, orderID uuid.UUID) idempotency.Key {
	return e.key(k, "/v1/payments", map[string]any{"order_id": orderID, "method": "upi"})
}

func (e *env) pay(orderID uuid.UUID) payments.PaymentView {
	e.t.Helper()
	r, err := e.svc.CreatePayment(e.ctx, e.payKey(e.newKey(), orderID))
	if err != nil {
		e.t.Fatal(err)
	}
	return mustDecode[payments.PaymentView](e.t, r, 201)
}

func (e *env) captureKey(k string, id uuid.UUID) idempotency.Key {
	return e.key(k, "/v1/payments/"+id.String()+"/capture", map[string]any{})
}

func (e *env) capture(id uuid.UUID) payments.PaymentView {
	e.t.Helper()
	r, err := e.svc.Capture(e.ctx, e.captureKey(e.newKey(), id), id)
	if err != nil {
		e.t.Fatal(err)
	}
	return mustDecode[payments.PaymentView](e.t, r, 200)
}

// captured runs order → payment → capture for amount.
func (e *env) captured(amount money.Paise) payments.PaymentView {
	e.t.Helper()
	p := e.pay(e.createOrder(amount).ID)
	if p.State != payments.StateAuthorized {
		e.t.Fatalf("payment %s, want authorized", p.State)
	}
	return e.capture(p.ID)
}

func (e *env) refundKey(k string, paymentID uuid.UUID, amount money.Paise) idempotency.Key {
	return e.key(k, "/v1/payments/"+paymentID.String()+"/refunds", map[string]any{"amount": amount})
}

func (e *env) refund(paymentID uuid.UUID, amount money.Paise) payments.Response {
	e.t.Helper()
	r, err := e.svc.CreateRefund(e.ctx, e.refundKey(e.newKey(), paymentID, amount), paymentID)
	if err != nil {
		e.t.Fatal(err)
	}
	return r
}

func (e *env) count(table string) int {
	e.t.Helper()
	var n int
	if err := e.pool.QueryRow(e.ctx, `SELECT count(*) FROM `+table).Scan(&n); err != nil {
		e.t.Fatal(err)
	}
	return n
}

func (e *env) checkInvariants() {
	e.t.Helper()
	rep, err := invariants.CheckAt(e.ctx, e.pool, e.clk.Now())
	if err != nil {
		e.t.Fatal(err)
	}
	if err := rep.Err(); err != nil {
		e.t.Fatal(err)
	}
}
