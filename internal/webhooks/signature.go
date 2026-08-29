// Package webhooks delivers events to merchants: signed, at least once,
// retried with backoff, paused after 24 hours of failure, and replayable.
package webhooks

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"time"
)

// Headers sent with every delivery.
const (
	HeaderSignature = "Kosh-Signature"
	HeaderEventID   = "Kosh-Event-Id"
	HeaderAttempt   = "Kosh-Attempt"
)

// DefaultTolerance is how old a signature a receiver should accept.
const DefaultTolerance = 5 * time.Minute

var (
	ErrBadSignatureHeader = errors.New("webhooks: malformed signature header")
	ErrSignatureMismatch  = errors.New("webhooks: signature does not match")
	ErrSignatureTooOld    = errors.New("webhooks: signature timestamp outside tolerance")
)

// Sign returns the signature header value for body sent at t:
//
//	t=<unix seconds>,v1=<hex HMAC-SHA256(secret, "<t>.<body>")>
//
// Signing the timestamp with the body stops an attacker replaying an old,
// captured delivery.
func Sign(secret string, t time.Time, body []byte) string {
	ts := strconv.FormatInt(t.Unix(), 10)
	return "t=" + ts + ",v1=" + hex.EncodeToString(mac(secret, ts, body))
}

func mac(secret, ts string, body []byte) []byte {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(ts))
	m.Write([]byte{'.'})
	m.Write(body)
	return m.Sum(nil)
}

// Verify checks a signature header the way a merchant should: constant-time
// comparison, and a timestamp within tolerance of now.
func Verify(secret, header string, body []byte, now time.Time, tolerance time.Duration) error {
	var ts string
	var sigs [][]byte
	for _, part := range strings.Split(header, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return ErrBadSignatureHeader
		}
		switch k {
		case "t":
			ts = v
		case "v1":
			b, err := hex.DecodeString(v)
			if err != nil {
				return ErrBadSignatureHeader
			}
			sigs = append(sigs, b)
		}
	}
	sec, err := strconv.ParseInt(ts, 10, 64)
	if err != nil || len(sigs) == 0 {
		return ErrBadSignatureHeader
	}
	if d := now.Sub(time.Unix(sec, 0)); d > tolerance || d < -tolerance {
		return ErrSignatureTooOld
	}
	want := mac(secret, ts, body)
	for _, s := range sigs {
		if hmac.Equal(s, want) {
			return nil
		}
	}
	return ErrSignatureMismatch
}
