package webhooks

import (
	"errors"
	"testing"
	"time"

	"github.com/naman777/kosh/internal/rng"
)

func TestSignVerify(t *testing.T) {
	now := time.Unix(1_790_000_000, 0)
	body := []byte(`{"id":"evt_1"}`)
	h := Sign("whsec_x", now, body)

	if err := Verify("whsec_x", h, body, now.Add(time.Minute), DefaultTolerance); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}
	if err := Verify("whsec_x", h, []byte(`{"id":"evt_2"}`), now, DefaultTolerance); !errors.Is(err, ErrSignatureMismatch) {
		t.Fatalf("tampered body: %v", err)
	}
	if err := Verify("whsec_other", h, body, now, DefaultTolerance); !errors.Is(err, ErrSignatureMismatch) {
		t.Fatalf("wrong secret: %v", err)
	}
	if err := Verify("whsec_x", h, body, now.Add(6*time.Minute), DefaultTolerance); !errors.Is(err, ErrSignatureTooOld) {
		t.Fatalf("replayed old delivery: %v", err)
	}
	for _, bad := range []string{"", "t=abc,v1=00", "t=1", "v1=zz", "garbage"} {
		if err := Verify("whsec_x", bad, body, now, DefaultTolerance); err == nil {
			t.Fatalf("malformed header %q accepted", bad)
		}
	}
}

func TestBackoff(t *testing.T) {
	d := &Dispatcher{RNG: rng.New(1)}
	for i := 0; i < 1000; i++ {
		if b := d.Backoff(1); b < 5*time.Second || b > 10*time.Second {
			t.Fatalf("attempt 1 backoff %v outside [5s, 10s]", b)
		}
		if b := d.Backoff(4); b < 40*time.Second || b > 80*time.Second {
			t.Fatalf("attempt 4 backoff %v outside [40s, 80s]", b)
		}
		if b := d.Backoff(40); b < 30*time.Minute || b > time.Hour {
			t.Fatalf("attempt 40 backoff %v outside [30m, 1h]", b)
		}
	}
}
