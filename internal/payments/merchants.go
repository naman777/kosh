package payments

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/naman777/kosh/internal/ledger"
)

// ErrUnauthorized means an API key is missing, unknown or revoked.
var ErrUnauthorized = errors.New("payments: invalid API key")

// CreateMerchantInput is the admin request to onboard a merchant.
type CreateMerchantInput struct {
	Name       string  `json:"name"`
	FeeBps     *int    `json:"fee_bps"`
	WebhookURL *string `json:"webhook_url"`
}

// CreatedMerchant is returned once: the API key and webhook secret are not
// retrievable later.
type CreatedMerchant struct {
	Merchant      MerchantView `json:"merchant"`
	APIKey        string       `json:"api_key"`
	WebhookSecret string       `json:"webhook_secret"`
}

func randomToken(prefix string) string {
	var b [24]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand never fails on supported platforms
	}
	return prefix + hex.EncodeToString(b[:])
}

func hashKey(key string) []byte {
	h := sha256.Sum256([]byte(key))
	return h[:]
}

// CreateMerchant creates a merchant, its payable account and an API key in
// one transaction.
func (s *Service) CreateMerchant(ctx context.Context, in CreateMerchantInput) (CreatedMerchant, error) {
	in.Name = strings.TrimSpace(in.Name)
	if in.Name == "" {
		return CreatedMerchant{}, errorf(http.StatusBadRequest, "bad_request", "name is required")
	}
	fee := 200
	if in.FeeBps != nil {
		fee = *in.FeeBps
	}
	if fee < 0 || fee > 5000 {
		return CreatedMerchant{}, errorf(http.StatusBadRequest, "bad_request", "fee_bps must be between 0 and 5000")
	}
	if in.WebhookURL != nil && *in.WebhookURL == "" {
		in.WebhookURL = nil
	}

	out := CreatedMerchant{
		Merchant: MerchantView{
			ID: s.ids.New(), Entity: "merchant", Name: in.Name, FeeBps: fee,
			WebhookURL: in.WebhookURL, CreatedAt: s.clock.Now(),
		},
		APIKey:        randomToken("kosh_sk_"),
		WebhookSecret: randomToken("whsec_"),
	}
	m := out.Merchant
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO merchants (id, name, fee_bps, webhook_url, webhook_secret, created_at)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			m.ID, m.Name, m.FeeBps, m.WebhookURL, out.WebhookSecret, m.CreatedAt); err != nil {
			return err
		}
		if _, err := s.ledger.EnsureAccount(ctx, tx, ledger.MerchantPayableSpec(m.ID)); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO api_keys (key_hash, merchant_id, prefix, created_at) VALUES ($1, $2, $3, $4)`,
			hashKey(out.APIKey), m.ID, out.APIKey[:12], m.CreatedAt)
		return err
	})
	if err != nil {
		return CreatedMerchant{}, err
	}
	return out, nil
}

// Authenticate maps an API key to its merchant.
func (s *Service) Authenticate(ctx context.Context, apiKey string) (uuid.UUID, error) {
	if apiKey == "" {
		return uuid.Nil, ErrUnauthorized
	}
	var id uuid.UUID
	err := s.pool.QueryRow(ctx, `
		SELECT merchant_id FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL`,
		hashKey(apiKey)).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, ErrUnauthorized
	}
	return id, err
}

// merchantPayable returns the merchant's fee rate and payable account.
func (s *Service) merchantPayable(ctx context.Context, tx pgx.Tx, merchantID uuid.UUID) (feeBps int64, acct ledger.AccountID, err error) {
	err = tx.QueryRow(ctx, `
		SELECT m.fee_bps, a.id FROM merchants m
		JOIN accounts a ON a.code = $2 AND a.owner_id = m.id
		WHERE m.id = $1`, merchantID, ledger.MerchantPayable).Scan(&feeBps, &acct)
	return feeBps, acct, err
}
