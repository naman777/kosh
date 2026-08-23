package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/naman777/kosh/internal/api"
	"github.com/naman777/kosh/internal/bank"
	"github.com/naman777/kosh/internal/banksim"
	"github.com/naman777/kosh/internal/payments"
	"github.com/naman777/kosh/internal/testdb"
	"github.com/naman777/kosh/invariants"
)

type client struct {
	t    *testing.T
	base string
	auth string
}

func (c client) do(method, path, key string, body any) (int, []byte) {
	c.t.Helper()
	var buf io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		buf = bytes.NewReader(raw)
	}
	req, _ := http.NewRequest(method, c.base+path, buf)
	if c.auth != "" {
		req.Header.Set("Authorization", "Bearer "+c.auth)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out
}

func decode[T any](t *testing.T, b []byte) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatalf("decode %s: %v", b, err)
	}
	return v
}

// TestEndToEnd drives the real HTTP API, with the HTTP bank client talking
// to the bank simulator.
func TestEndToEnd(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	sim := httptest.NewServer(banksim.New(banksim.DefaultConfig(), 1))
	defer sim.Close()

	svc, err := payments.New(ctx, payments.Deps{
		Pool: pool, Bank: bank.NewHTTPClient(sim.URL),
		Config: payments.Config{BankTimeout: 2 * time.Second, RefundHoldTTL: time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(api.New(svc, "admin-secret", nil))
	defer srv.Close()

	admin := client{t: t, base: srv.URL, auth: "admin-secret"}
	if code, _ := (client{t: t, base: srv.URL, auth: "wrong"}).do("POST", "/admin/merchants", "", map[string]any{"name": "x"}); code != 401 {
		t.Fatalf("admin with bad token: %d", code)
	}
	code, body := admin.do("POST", "/admin/merchants", "", map[string]any{"name": "Chai Point"})
	if code != 201 {
		t.Fatalf("create merchant: %d %s", code, body)
	}
	m := decode[payments.CreatedMerchant](t, body)
	c := client{t: t, base: srv.URL, auth: m.APIKey}

	if code, _ := (client{t: t, base: srv.URL, auth: "kosh_sk_nope"}).do("GET", "/v1/payments/00000000-0000-0000-0000-000000000000", "", nil); code != 401 {
		t.Fatalf("bad api key: %d", code)
	}
	if code, body := c.do("POST", "/v1/orders", "", map[string]any{"amount": 100_000}); code != 400 {
		t.Fatalf("missing idempotency key: %d %s", code, body)
	}

	code, body = c.do("POST", "/v1/orders", "o1", map[string]any{"amount": 100_000, "receipt": "rcpt-1"})
	if code != 201 {
		t.Fatalf("order: %d %s", code, body)
	}
	order := decode[payments.OrderView](t, body)

	// Five retries of the same payment request: one payment, same bytes.
	var first []byte
	for i := 0; i < 5; i++ {
		code, body = c.do("POST", "/v1/payments", "p1", map[string]any{"order_id": order.ID, "method": "upi"})
		if code != 201 {
			t.Fatalf("payment attempt %d: %d %s", i, code, body)
		}
		if first == nil {
			first = body
		} else if !bytes.Equal(first, body) {
			t.Fatalf("retry %d differs:\n%s\n%s", i, first, body)
		}
	}
	p := decode[payments.PaymentView](t, first)
	if p.State != payments.StateAuthorized || p.BankRef == nil {
		t.Fatalf("payment %+v", p)
	}

	code, body = c.do("POST", "/v1/payments/"+p.ID.String()+"/capture", "c1", nil)
	if code != 200 {
		t.Fatalf("capture: %d %s", code, body)
	}
	code, body = c.do("POST", "/v1/payments/"+p.ID.String()+"/refunds", "r1", map[string]any{"amount": 25_000})
	if code != 201 {
		t.Fatalf("refund: %d %s", code, body)
	}
	r := decode[payments.RefundView](t, body)
	if r.State != payments.RefundProcessed {
		t.Fatalf("refund %+v", r)
	}
	code, body = c.do("GET", "/v1/payments/"+p.ID.String(), "", nil)
	if code != 200 {
		t.Fatalf("get: %d %s", code, body)
	}
	if got := decode[payments.PaymentView](t, body); got.Refunded != 25_000 || got.State != payments.StateCaptured {
		t.Fatalf("payment after partial refund %+v", got)
	}

	// Same key, different body.
	code, body = c.do("POST", "/v1/payments", "p1", map[string]any{"order_id": order.ID, "method": "card"})
	if code != 422 {
		t.Fatalf("key reuse: %d %s", code, body)
	}

	rep, err := invariants.Check(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if err := rep.Err(); err != nil {
		t.Fatal(err)
	}
}

// TestBankTimeoutOverHTTP: the simulator decides success but replies after
// Kosh's deadline. The payment must be pending, not failed.
func TestBankTimeoutOverHTTP(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	cfg := banksim.Config{TimeoutRate: 1, HangForMs: 5_000}
	sim := httptest.NewServer(banksim.New(cfg, 1))
	defer sim.Close()
	svc, err := payments.New(ctx, payments.Deps{
		Pool: pool, Bank: bank.NewHTTPClient(sim.URL),
		Config: payments.Config{BankTimeout: 200 * time.Millisecond, RefundHoldTTL: time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(api.New(svc, "admin", nil))
	defer srv.Close()
	_, body := client{t: t, base: srv.URL, auth: "admin"}.do("POST", "/admin/merchants", "", map[string]any{"name": "Slow Bank Co"})
	c := client{t: t, base: srv.URL, auth: decode[payments.CreatedMerchant](t, body).APIKey}
	_, body = c.do("POST", "/v1/orders", "o", map[string]any{"amount": 5_000})
	order := decode[payments.OrderView](t, body)
	code, body := c.do("POST", "/v1/payments", "p", map[string]any{"order_id": order.ID, "method": "card"})
	if code != 201 {
		t.Fatalf("%d %s", code, body)
	}
	if p := decode[payments.PaymentView](t, body); p.State != payments.StatePending {
		t.Fatalf("state %s, want pending", p.State)
	}
}
