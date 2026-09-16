// Package banksim plays the bank and the UPI switch over HTTP, with faults
// that can be dialled up at runtime.
//
// It dedupes by Kosh's id like a real bank: the first request for an id
// decides its fate, and every later request or status check returns the
// same answer. The faults:
//
//   - fail: a decline.
//   - timeout: the money moves, but the reply arrives after Kosh's deadline.
//   - pending: the bank answers "pending" and decides later (FinalAfterMs);
//     the final answer is visible through Status and sent as a callback.
//   - outage: every request gets 503 and nothing is recorded.
//   - callbacks: each final answer is POSTed to Kosh after CallbackDelayMs,
//     1 to MaxDuplicateCallbacks times.
//
// It also writes daily settlement files with injected bad rows (settlement.go).
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
	"github.com/naman777/kosh/internal/clock"
	"github.com/naman777/kosh/internal/money"
)

// Config sets fault rates. Rates are probabilities in [0, 1]; whatever is
// left after FailRate, TimeoutRate and PendingRate succeeds at once.
type Config struct {
	FailRate     float64 `json:"fail_rate"`
	TimeoutRate  float64 `json:"timeout_rate"`
	PendingRate  float64 `json:"pending_rate"`
	LatencyMinMs int64   `json:"latency_min_ms"`
	LatencyMaxMs int64   `json:"latency_max_ms"`
	// HangForMs is how long a "timeout" reply is delayed. It should exceed
	// Kosh's bank timeout.
	HangForMs int64 `json:"hang_for_ms"`
	// FinalAfterMs is how long a "pending" request stays pending.
	FinalAfterMs int64 `json:"final_after_ms"`
	// Outage makes every request fail with 503 without being recorded.
	Outage bool `json:"outage"`

	CallbackURL           string `json:"callback_url"`
	CallbackSecret        string `json:"callback_secret,omitempty"`
	CallbackDelayMs       int64  `json:"callback_delay_ms"`
	MaxDuplicateCallbacks int    `json:"max_duplicate_callbacks"`

	Settlement SettlementFaults `json:"settlement"`
}

// DefaultConfig is a healthy bank with 5–50 ms latency.
func DefaultConfig() Config {
	return Config{LatencyMinMs: 5, LatencyMaxMs: 50, HangForMs: 35_000, FinalAfterMs: 2_000, MaxDuplicateCallbacks: 1}
}

// record is everything the bank knows about one request.
type record struct {
	Kind      string      `json:"kind"`
	ID        uuid.UUID   `json:"id"`
	Amount    money.Paise `json:"amount"`
	Method    string      `json:"method,omitempty"`
	Final     bank.Result `json:"final"`
	DecidedAt time.Time   `json:"decided_at"`
	FinalAt   time.Time   `json:"final_at"` // answers Pending until then
}

func (r record) view(now time.Time) bank.Result {
	if now.Before(r.FinalAt) {
		return bank.Result{Status: bank.Pending}
	}
	return r.Final
}

// Server is the simulator.
type Server struct {
	mu      sync.Mutex
	cfg     Config
	rng     *rand.Rand
	clock   clock.Clock
	records map[uuid.UUID]*record
	order   []uuid.UUID // decision order, for settlement files
	seq     uint64
	seed    uint64
	runTag  string // keeps references unique across simulator restarts
	mux     *http.ServeMux
	hc      *http.Client

	// CallbacksSent counts callback deliveries (including duplicates).
	CallbacksSent int
}

// New returns a simulator seeded with seed, on the wall clock.
func New(cfg Config, seed uint64) *Server { return NewWithClock(cfg, seed, clock.Real{}) }

// NewWithClock returns a simulator whose settlement dates follow c.
func NewWithClock(cfg Config, seed uint64, c clock.Clock) *Server {
	s := &Server{cfg: cfg, rng: rand.New(rand.NewPCG(seed, 1)), clock: c, seed: seed,
		runTag:  fmt.Sprintf("%04X", uint16(time.Now().UnixNano()>>10)),
		records: map[uuid.UUID]*record{}, mux: http.NewServeMux(), hc: &http.Client{Timeout: 5 * time.Second}}
	s.mux.HandleFunc("POST /v1/authorize", func(w http.ResponseWriter, r *http.Request) {
		var req bank.AuthorizeRequest
		if decode(w, r, &req) {
			s.answer(w, r, record{Kind: bank.KindPayment, ID: req.PaymentID, Amount: req.Amount, Method: req.Method}, "UTR")
		}
	})
	s.mux.HandleFunc("POST /v1/refunds", func(w http.ResponseWriter, r *http.Request) {
		var req bank.RefundRequest
		if decode(w, r, &req) {
			s.answer(w, r, record{Kind: bank.KindRefund, ID: req.RefundID, Amount: req.Amount}, "RFN")
		}
	})
	s.mux.HandleFunc("POST /v1/payouts", func(w http.ResponseWriter, r *http.Request) {
		var req bank.PayoutRequest
		if decode(w, r, &req) {
			s.answer(w, r, record{Kind: bank.KindPayout, ID: req.PayoutID, Amount: req.Amount}, "NEFT")
		}
	})
	s.mux.HandleFunc("GET /v1/status/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(r.PathValue("id"))
		if err != nil {
			http.Error(w, "bad id", http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		rec, ok := s.records[id]
		outage := s.cfg.Outage
		s.mu.Unlock()
		if outage {
			http.Error(w, "bank unavailable", http.StatusServiceUnavailable)
			return
		}
		res := bank.Result{Status: bank.Failed, Reason: bank.ReasonUnknownID}
		if ok {
			res = rec.view(s.clock.Now())
		}
		writeJSON(w, res)
	})
	s.mux.HandleFunc("GET /admin/config", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, s.Config()) })
	s.mux.HandleFunc("PUT /admin/config", func(w http.ResponseWriter, r *http.Request) {
		var c Config
		if decode(w, r, &c) {
			s.SetConfig(c)
			writeJSON(w, c)
		}
	})
	s.mux.HandleFunc("GET /v1/settlements/{date}", s.serveSettlement)
	s.mux.HandleFunc("GET /admin/settlements/{date}/faults", s.serveFaults)
	return s
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// Config returns the current config.
func (s *Server) Config() Config {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg
}

// SetConfig replaces the config.
func (s *Server) SetConfig(c Config) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cfg = c
}

func (s *Server) answer(w http.ResponseWriter, r *http.Request, req record, prefix string) {
	s.mu.Lock()
	cfg := s.cfg
	if cfg.Outage {
		s.mu.Unlock()
		http.Error(w, "bank unavailable", http.StatusServiceUnavailable)
		return
	}
	now := s.clock.Now()
	latencyMs := cfg.LatencyMinMs
	if cfg.LatencyMaxMs > cfg.LatencyMinMs {
		latencyMs += s.rng.Int64N(cfg.LatencyMaxMs - cfg.LatencyMinMs)
	}
	latency := time.Duration(latencyMs) * time.Millisecond

	rec, seen := s.records[req.ID]
	if !seen {
		s.seq++
		rec = &req
		rec.DecidedAt, rec.FinalAt = now, now
		success := bank.Result{Status: bank.Success, BankRef: fmt.Sprintf("%s%s%08d", prefix, s.runTag, s.seq)}
		switch x := s.rng.Float64(); {
		case x < cfg.FailRate:
			rec.Final = bank.Result{Status: bank.Failed, Reason: "declined_by_issuer"}
		case x < cfg.FailRate+cfg.TimeoutRate:
			rec.Final = success
			latency = time.Duration(cfg.HangForMs) * time.Millisecond
		case x < cfg.FailRate+cfg.TimeoutRate+cfg.PendingRate:
			// Decided now, revealed later. Most pending requests succeed.
			rec.Final = success
			if s.rng.Float64() < 0.2 {
				rec.Final = bank.Result{Status: bank.Failed, Reason: "collect_request_expired"}
			}
			rec.FinalAt = now.Add(time.Duration(cfg.FinalAfterMs) * time.Millisecond)
		default:
			rec.Final = success
		}
		s.records[req.ID] = rec
		s.order = append(s.order, req.ID)
		s.scheduleCallbacks(*rec, cfg)
	}
	res := rec.view(now)
	s.mu.Unlock()

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
