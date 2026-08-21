// Command banksim plays the bank and UPI switch for Kosh, with injectable
// faults. Change them at runtime:
//
//	curl -X PUT localhost:8091/admin/config \
//	  -d '{"fail_rate":0.1,"timeout_rate":0.05,"latency_min_ms":5,"latency_max_ms":50,"hang_for_ms":35000}'
//
//	BANKSIM_ADDR  listen address (default :8091)
//	BANKSIM_SEED  RNG seed (default 1)
package main

import (
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/naman777/kosh/internal/banksim"
	"github.com/naman777/kosh/internal/env"
)

func main() {
	seed, _ := strconv.ParseUint(env.String("BANKSIM_SEED", "1"), 10, 64)
	addr := env.String("BANKSIM_ADDR", ":8091")
	srv := &http.Server{Addr: addr, Handler: banksim.New(banksim.DefaultConfig(), seed), ReadHeaderTimeout: 5 * time.Second}
	slog.Info("banksim listening", "addr", addr, "seed", seed)
	if err := srv.ListenAndServe(); err != nil {
		slog.Error("banksim exiting", "err", err)
		os.Exit(1)
	}
}
