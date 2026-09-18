package bank

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// HTTPClient talks to a bank over HTTP (in practice, cmd/banksim).
// Any transport error, timeout or 5xx is reported as ErrUnknownOutcome,
// because the bank may have acted before the reply was lost.
type HTTPClient struct {
	BaseURL string
	HTTP    *http.Client
}

// NewHTTPClient returns a client for baseURL. Per-call deadlines come from
// the caller's context.
func NewHTTPClient(baseURL string) *HTTPClient {
	return &HTTPClient{BaseURL: baseURL, HTTP: &http.Client{Timeout: 2 * time.Minute}}
}

func (c *HTTPClient) do(ctx context.Context, method, path string, body any) (Result, error) {
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return Result{}, err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, &buf)
	if err != nil {
		return Result{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %v", ErrUnknownOutcome, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 {
		return Result{}, fmt.Errorf("%w: bank returned %d", ErrUnknownOutcome, resp.StatusCode)
	}
	if resp.StatusCode >= 400 {
		return Result{}, fmt.Errorf("bank: %s %s returned %d", method, path, resp.StatusCode)
	}
	var r Result
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return Result{}, fmt.Errorf("%w: bad reply: %v", ErrUnknownOutcome, err)
	}
	return r, nil
}

// Authorize implements Client.
func (c *HTTPClient) Authorize(ctx context.Context, req AuthorizeRequest) (Result, error) {
	return c.do(ctx, http.MethodPost, "/v1/authorize", req)
}

// Refund implements Client.
func (c *HTTPClient) Refund(ctx context.Context, req RefundRequest) (Result, error) {
	return c.do(ctx, http.MethodPost, "/v1/refunds", req)
}

// Payout implements Client.
func (c *HTTPClient) Payout(ctx context.Context, req PayoutRequest) (Result, error) {
	return c.do(ctx, http.MethodPost, "/v1/payouts", req)
}

// Status implements Client.
func (c *HTTPClient) Status(ctx context.Context, id uuid.UUID) (Result, error) {
	return c.do(ctx, http.MethodGet, "/v1/status/"+id.String(), nil)
}

// SettlementSource fetches the bank's settlement file for a business date
// (YYYY-MM-DD, IST) as raw CSV.
type SettlementSource interface {
	SettlementFile(ctx context.Context, date string) ([]byte, error)
}

// SettlementFile implements SettlementSource.
func (c *HTTPClient) SettlementFile(ctx context.Context, date string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/v1/settlements/"+date, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bank: settlement file %s: status %d", date, resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 64<<20))
}
