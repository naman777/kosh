// Package banksim plays the bank and the UPI switch over HTTP, with faults
// that can be dialled up at runtime: declines, timeouts and latency.
//
// It dedupes by Kosh's id like a real bank: the first request for an id
// decides its fate, and every later request or status check returns the
// same answer. A "timeout" decides success but delays the reply past the
// caller's deadline, which is the nastiest real-world case: the money moved
// and Kosh doesn't know.
package banksim

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/naman777/kosh/internal/bank"
)

// Config sets fault rates. Rates are probabilities in [0, 1]; whatever is
// left after FailRate and TimeoutRate succeeds.
type Config struct {
	FailRate     float64 `json:"fail_rate"`
	TimeoutRate  float64 `json:"timeout_rate"`
	LatencyMinMs int64   `json:"latency_min_ms"`
	LatencyMaxMs int64   `json:"latency_max_ms"`
	// HangForMs is how long a "timeout" reply is delayed. It should exceed
	// Kosh's bank timeout.
	HangForMs int64 `json:"hang_for_ms"`
}

// DefaultConfig is a healthy bank with 5–50 ms latency.
func DefaultConfig() Config {
	return Config{LatencyMinMs: 5, LatencyMaxMs: 50, HangForMs: 35_000}
}

// Server is the simulator.
type Server struct {
	mu      sync.Mutex
	cfg     Config
	rng     *rand.Rand
	results map[uuid.UUID]bank.Result
	seq     uint64
	mux     *http.ServeMux
}

// New returns a simulator seeded with seed.
func New(cfg Config, seed uint64) *Server {
	s := &Server{cfg: cfg, rng: rand.New(rand.NewPCG(seed, 1)), results: map[uuid.UUID]bank.Result{}, mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /v1/authorize", func(w http.ResponseWriter, r *http.Request) {
		var req bank.AuthorizeRequest
		if !decode(w, r, &req) {
			return
		}
		s.answer(w, r, req.PaymentID, "UTR")
	})
	s.mux.HandleFunc("POST /v1/refunds", func(w http.ResponseWriter, r *http.Request) {
		var req bank.RefundRequest
		if !decode(w, r, &req) {
			return
		}
		s.answer(w, r, req.RefundID, "RFN")
	})
	s.mux.HandleFunc("GET /v1/status/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(r.PathValue("id"))
		if err != nil {
			http.Error(w, "bad id", http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		res, ok := s.results[id]
		s.mu.Unlock()
		if !ok {
			res = bank.Result{Status: bank.Failed, Reason: "unknown_id"}
		}
		writeJSON(w, res)
	})
	s.mux.HandleFunc("GET /admin/config", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		writeJSON(w, s.cfg)
	})
	s.mux.HandleFunc("PUT /admin/config", func(w http.ResponseWriter, r *http.Request) {
		var c Config
		if !decode(w, r, &c) {
			return
		}
		s.mu.Lock()
		s.cfg = c
		s.mu.Unlock()
		writeJSON(w, c)
	})
	return s
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func (s *Server) answer(w http.ResponseWriter, r *http.Request, id uuid.UUID, prefix string) {
	s.mu.Lock()
	res, seen := s.results[id]
	cfg := s.cfg
	hang := false
	latencyMs := cfg.LatencyMinMs
	if cfg.LatencyMaxMs > cfg.LatencyMinMs {
		latencyMs += s.rng.Int64N(cfg.LatencyMaxMs - cfg.LatencyMinMs)
	}
	latency := time.Duration(latencyMs) * time.Millisecond
	if !seen {
		s.seq++
		x := s.rng.Float64()
		switch {
		case x < cfg.FailRate:
			res = bank.Result{Status: bank.Failed, Reason: "declined_by_issuer"}
		case x < cfg.FailRate+cfg.TimeoutRate:
			hang = true
			fallthrough
		default:
			res = bank.Result{Status: bank.Success, BankRef: fmt.Sprintf("%s%09d", prefix, s.seq)}
		}
		s.results[id] = res
	}
	s.mu.Unlock()

	if hang {
		latency = time.Duration(cfg.HangForMs) * time.Millisecond
	}
	select {
	case <-time.After(latency):
	case <-r.Context().Done():
		return // caller gave up; the decision stands
	}
	writeJSON(w, res)
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
