// Command samplemerchant is a reference webhook receiver: it verifies Kosh's
// signature, dedupes by event id, and can be switched to fail or hang.
//
//	SAMPLE_SECRET   the merchant's webhook secret (required)
//	SAMPLE_ADDR     listen address (default :8092)
//
//	POST /webhooks      deliveries from Kosh
//	GET  /admin/stats   unique, duplicate and rejected counts
//	PUT  /admin/mode    {"mode": "ok" | "fail" | "hang"}
package main

import (
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/naman777/kosh/internal/clock"
	"github.com/naman777/kosh/internal/env"
	"github.com/naman777/kosh/internal/webhooks"
)

func main() {
	secret := os.Getenv("SAMPLE_SECRET")
	if secret == "" {
		slog.Error("SAMPLE_SECRET is required (the webhook_secret returned when the merchant was created)")
		os.Exit(2)
	}
	addr := env.String("SAMPLE_ADDR", ":8092")
	r := webhooks.NewReceiver(secret, clock.Real{})
	srv := &http.Server{Addr: addr, Handler: r.Admin(), ReadHeaderTimeout: 5 * time.Second}
	slog.Info("sample merchant listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil {
		slog.Error("sample merchant exiting", "err", err)
		os.Exit(1)
	}
}
