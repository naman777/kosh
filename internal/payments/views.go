package payments

import (
	"time"

	"github.com/google/uuid"
)

// PaymentView is the API representation of a payment. Amounts are paise.
type PaymentView struct {
	ID            uuid.UUID `json:"id"`
	Entity        string    `json:"entity"`
	OrderID       uuid.UUID `json:"order_id"`
	Amount        int64     `json:"amount"`
	Refunded      int64     `json:"amount_refunded"`
	Currency      string    `json:"currency"`
	Method        string    `json:"method"`
	State         State     `json:"status"`
	BankRef       *string   `json:"bank_ref"`
	FailureReason *string   `json:"failure_reason"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

func (p Payment) View() PaymentView {
	return PaymentView{
		ID: p.ID, Entity: "payment", OrderID: p.OrderID,
		Amount: int64(p.Amount), Refunded: int64(p.Refunded), Currency: "INR",
		Method: p.Method, State: p.State, BankRef: p.BankRef, FailureReason: p.FailureReason,
		CreatedAt: p.CreatedAt.UTC(), UpdatedAt: p.UpdatedAt.UTC(),
	}
}

// OrderView is the API representation of an order.
type OrderView struct {
	ID        uuid.UUID `json:"id"`
	Entity    string    `json:"entity"`
	Amount    int64     `json:"amount"`
	Currency  string    `json:"currency"`
	Receipt   *string   `json:"receipt"`
	CreatedAt time.Time `json:"created_at"`
}

// RefundView is the API representation of a refund.
type RefundView struct {
	ID            uuid.UUID `json:"id"`
	Entity        string    `json:"entity"`
	PaymentID     uuid.UUID `json:"payment_id"`
	Amount        int64     `json:"amount"`
	Currency      string    `json:"currency"`
	State         string    `json:"status"`
	BankRef       *string   `json:"bank_ref"`
	FailureReason *string   `json:"failure_reason"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// MerchantView is the API representation of a merchant.
type MerchantView struct {
	ID         uuid.UUID `json:"id"`
	Entity     string    `json:"entity"`
	Name       string    `json:"name"`
	FeeBps     int       `json:"fee_bps"`
	WebhookURL *string   `json:"webhook_url"`
	CreatedAt  time.Time `json:"created_at"`
}
