package webhooks

import (
	"encoding/json"
	"io"
	"net/http"
	"sync"

	"github.com/naman777/kosh/internal/clock"
)

// Receiver is a reference merchant endpoint: it verifies signatures, dedupes
// by event id, and can be told to fail or hang to exercise retries. It backs
// cmd/samplemerchant and the tests.
type Receiver struct {
	Secret string
	Clock  clock.Clock

	mu       sync.Mutex
	mode     string // ok | fail | hang
	seen     map[string]int
	accepted []string // event ids in first-seen order
	rejected int
	sunk     int // deliveries to /sink
}

// Receiver modes.
const (
	ModeOK   = "ok"
	ModeFail = "fail" // answer 500
	ModeHang = "hang" // never answer (until the sender gives up)
)

// NewReceiver returns a receiver in ModeOK.
func NewReceiver(secret string, c clock.Clock) *Receiver {
	return &Receiver{Secret: secret, Clock: c, mode: ModeOK, seen: map[string]int{}}
}

// SetMode switches behaviour.
func (r *Receiver) SetMode(m string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mode = m
}

// Stats reports what the receiver has seen.
type Stats struct {
	Unique     int            `json:"unique"`
	Duplicates int            `json:"duplicates"`
	Rejected   int            `json:"rejected"`
	Sunk       int            `json:"sunk"`
	Mode       string         `json:"mode"`
	PerEvent   map[string]int `json:"-"`
}

// Stats returns a snapshot.
func (r *Receiver) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := Stats{Unique: len(r.seen), Rejected: r.rejected, Sunk: r.sunk, Mode: r.mode, PerEvent: map[string]int{}}
	for id, n := range r.seen {
		s.PerEvent[id] = n
		s.Duplicates += n - 1
	}
	return s
}

// ServeHTTP handles a delivery.
func (r *Receiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	mode := r.mode
	r.mu.Unlock()
	switch mode {
	case ModeFail:
		http.Error(w, "receiver told to fail", http.StatusInternalServerError)
		return
	case ModeHang:
		<-req.Context().Done()
		return
	}

	body, err := io.ReadAll(io.LimitReader(req.Body, 1<<20))
	if err != nil {
		http.Error(w, "read", http.StatusBadRequest)
		return
	}
	if err := Verify(r.Secret, req.Header.Get(HeaderSignature), body, r.Clock.Now(), DefaultTolerance); err != nil {
		r.mu.Lock()
		r.rejected++
		r.mu.Unlock()
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	var env struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &env); err != nil || env.ID == "" || env.ID != req.Header.Get(HeaderEventID) {
		http.Error(w, "bad envelope", http.StatusBadRequest)
		return
	}

	r.mu.Lock()
	if r.seen[env.ID] == 0 {
		r.accepted = append(r.accepted, env.ID)
		// A real merchant would process the event here, inside its own
		// transaction with the dedupe record.
	}
	r.seen[env.ID]++
	r.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

// Admin exposes stats and mode switching for cmd/samplemerchant.
func (r *Receiver) Admin() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(r.Stats())
	})
	mux.HandleFunc("PUT /admin/mode", func(w http.ResponseWriter, req *http.Request) {
		var in struct {
			Mode string `json:"mode"`
		}
		if err := json.NewDecoder(req.Body).Decode(&in); err != nil || (in.Mode != ModeOK && in.Mode != ModeFail && in.Mode != ModeHang) {
			http.Error(w, `mode must be "ok", "fail" or "hang"`, http.StatusBadRequest)
			return
		}
		r.SetMode(in.Mode)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.Handle("POST /webhooks", r)
	// /sink/... accepts any delivery without checking signatures: a target
	// for load tests where many merchants (with different secrets) post here.
	mux.HandleFunc("POST /sink/{rest...}", func(w http.ResponseWriter, req *http.Request) {
		io.Copy(io.Discard, req.Body)
		r.mu.Lock()
		r.sunk++
		r.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	return mux
}
