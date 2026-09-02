package payments

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/naman777/kosh/internal/idempotency"
	"github.com/naman777/kosh/internal/money"
)

// CreateOrderInput is POST /v1/orders. Amount is in paise.
type CreateOrderInput struct {
	Amount   money.Paise `json:"amount"`
	Currency string      `json:"currency"`
	Receipt  *string     `json:"receipt"`
}

// maxAmount caps a single order at ₹1 crore, well inside int64 and any
// realistic UPI or card limit.
const maxAmount = money.Paise(1_00_00_000_00)

// CreateOrder creates an order the customer can then pay.
func (s *Service) CreateOrder(ctx context.Context, k idempotency.Key) (Response, error) {
	return s.idempotent(ctx, k, func(rec *idempotency.Record) (Response, error) {
		in, err := decode[CreateOrderInput](k.Body)
		if err != nil {
			return Response{}, err
		}
		if in.Amount <= 0 || in.Amount > maxAmount {
			return Response{}, errorf(http.StatusBadRequest, "bad_request", "amount must be between 1 and %d paise", maxAmount)
		}
		if in.Currency != "" && in.Currency != "INR" {
			return Response{}, errorf(http.StatusBadRequest, "bad_request", "only INR is supported")
		}
		return s.finish(ctx, rec, http.StatusCreated, func(tx pgx.Tx) (any, error) {
			o := OrderView{ID: s.ids.New(), Entity: "order", Amount: int64(in.Amount), Currency: "INR",
				Receipt: in.Receipt, CreatedAt: s.clock.Now()}
			if _, err := tx.Exec(ctx, `
				INSERT INTO orders (id, merchant_id, amount, receipt, created_at) VALUES ($1, $2, $3, $4, $5)`,
				o.ID, k.MerchantID, o.Amount, o.Receipt, o.CreatedAt); err != nil {
				return nil, err
			}
			return o, s.idem.Advance(ctx, tx, rec, "order_created", o.ID)
		})
	})
}

// GetOrder reads one of a merchant's orders.
func (s *Service) GetOrder(ctx context.Context, merchantID, id uuid.UUID) (OrderView, error) {
	o := OrderView{Entity: "order", Currency: "INR"}
	var created time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT id, amount, receipt, created_at FROM orders WHERE id = $1 AND merchant_id = $2`,
		id, merchantID).Scan(&o.ID, &o.Amount, &o.Receipt, &created)
	if errors.Is(err, pgx.ErrNoRows) {
		return OrderView{}, errorf(http.StatusNotFound, "not_found", "order %s not found", id)
	}
	o.CreatedAt = created.UTC()
	return o, err
}
