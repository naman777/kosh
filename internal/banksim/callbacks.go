package banksim

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"github.com/naman777/kosh/internal/bank"
)

// scheduleCallbacks sends rec's final answer to Kosh once it is final plus
// CallbackDelayMs, 1 to MaxDuplicateCallbacks times. Called with s.mu held.
func (s *Server) scheduleCallbacks(rec record, cfg Config) {
	if cfg.CallbackURL == "" {
		return
	}
	copies := 1
	if cfg.MaxDuplicateCallbacks > 1 {
		copies += s.rng.IntN(cfg.MaxDuplicateCallbacks)
	}
	delay := rec.FinalAt.Sub(rec.DecidedAt) + time.Duration(cfg.CallbackDelayMs)*time.Millisecond
	body, _ := json.Marshal(bank.Callback{Kind: rec.Kind, ID: rec.ID, Status: rec.Final.Status,
		BankRef: rec.Final.BankRef, Reason: rec.Final.Reason})
	for i := 0; i < copies; i++ {
		gap := time.Duration(i) * 20 * time.Millisecond
		time.AfterFunc(delay+gap, func() { s.sendCallback(cfg, body) })
	}
}

// sendCallback POSTs one callback, retrying a few times on failure.
func (s *Server) sendCallback(cfg Config, body []byte) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, cfg.CallbackURL, bytes.NewReader(body))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(bank.HeaderCallbackSignature, bank.SignCallback(cfg.CallbackSecret, body))
		resp, err := s.hc.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode < 300 {
				s.mu.Lock()
				s.CallbacksSent++
				s.mu.Unlock()
				return
			}
		}
		time.Sleep(time.Duration(200*(attempt+1)) * time.Millisecond)
	}
}

// Sent returns how many callbacks were delivered.
func (s *Server) Sent() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.CallbacksSent
}
