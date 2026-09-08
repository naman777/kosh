package bank

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"

	"github.com/google/uuid"
)

// Callback kinds.
const (
	KindPayment = "payment"
	KindRefund  = "refund"
	KindPayout  = "payout"
)

// HeaderCallbackSignature carries hex HMAC-SHA256(secret, body).
const HeaderCallbackSignature = "Bank-Signature"

// Callback is the bank telling Kosh, asynchronously, how a request ended.
// Banks send these late, more than once, and in any order, so handling one
// must be idempotent.
type Callback struct {
	Kind    string    `json:"kind"`
	ID      uuid.UUID `json:"id"`
	Status  Status    `json:"status"`
	BankRef string    `json:"bank_ref,omitempty"`
	Reason  string    `json:"reason,omitempty"`
}

// Result returns the callback's answer.
func (c Callback) Result() Result {
	return Result{Status: c.Status, BankRef: c.BankRef, Reason: c.Reason}
}

func callbackMAC(secret string, body []byte) []byte {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write(body)
	return m.Sum(nil)
}

// SignCallback signs a callback body.
func SignCallback(secret string, body []byte) string {
	return hex.EncodeToString(callbackMAC(secret, body))
}

// VerifyCallback checks a callback signature in constant time.
func VerifyCallback(secret, signature string, body []byte) bool {
	got, err := hex.DecodeString(signature)
	return err == nil && hmac.Equal(got, callbackMAC(secret, body))
}
