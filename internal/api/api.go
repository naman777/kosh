// Package api is Kosh's HTTP interface. It authenticates merchants, reads
// the Idempotency-Key header, and hands everything else to payments.Service.
package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/naman777/kosh/internal/bank"
	"github.com/naman777/kosh/internal/idempotency"
	"github.com/naman777/kosh/internal/payments"
)

// maxBody caps request bodies at 64 KiB.
const maxBody = 64 << 10

// Config holds the API's secrets.
type Config struct {
	// AdminToken guards /admin. Empty disables /admin.
	AdminToken string
	// BankCallbackSecret verifies /bank/callbacks. Empty disables callbacks.
	BankCallbackSecret string
}

// Server routes requests.
type Server struct {
	svc *payments.Service
	cfg Config
	log *slog.Logger
	mux *http.ServeMux
}

// New builds the handler.
func New(svc *payments.Service, cfg Config, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	s := &Server{svc: svc, cfg: cfg, log: log, mux: http.NewServeMux()}

	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	s.mux.HandleFunc("POST /admin/merchants", s.admin(s.createMerchant))
	s.mux.HandleFunc("POST /admin/settlements/run", s.admin(s.runSettlement))
	s.mux.HandleFunc("POST /bank/callbacks", s.bankCallback)
	s.mux.HandleFunc("GET /v1/payouts/{id}", s.merchant(func(w http.ResponseWriter, r *http.Request, m uuid.UUID) {
		id, ok := s.pathID(w, r)
		if ok {
			s.respond(w, r)(s.svc.GetPayout(r.Context(), m, id))
		}
	}))

	s.mux.HandleFunc("POST /v1/orders", s.idempotent(func(ctx context.Context, k idempotency.Key, _ *http.Request) (payments.Response, error) {
		return s.svc.CreateOrder(ctx, k)
	}))
	s.mux.HandleFunc("GET /v1/orders/{id}", s.merchant(func(w http.ResponseWriter, r *http.Request, m uuid.UUID) {
		id, ok := s.pathID(w, r)
		if ok {
			s.respond(w, r)(s.svc.GetOrder(r.Context(), m, id))
		}
	}))
	s.mux.HandleFunc("POST /v1/payments", s.idempotent(func(ctx context.Context, k idempotency.Key, _ *http.Request) (payments.Response, error) {
		return s.svc.CreatePayment(ctx, k)
	}))
	s.mux.HandleFunc("GET /v1/payments/{id}", s.merchant(func(w http.ResponseWriter, r *http.Request, m uuid.UUID) {
		id, ok := s.pathID(w, r)
		if ok {
			s.respond(w, r)(s.svc.GetPayment(r.Context(), m, id))
		}
	}))
	s.mux.HandleFunc("POST /v1/payments/{id}/capture", s.idempotent(func(ctx context.Context, k idempotency.Key, r *http.Request) (payments.Response, error) {
		id, err := uuid.Parse(r.PathValue("id"))
		if err != nil {
			return payments.Response{}, notFound()
		}
		return s.svc.Capture(ctx, k, id)
	}))
	s.mux.HandleFunc("POST /v1/payments/{id}/refunds", s.idempotent(func(ctx context.Context, k idempotency.Key, r *http.Request) (payments.Response, error) {
		id, err := uuid.Parse(r.PathValue("id"))
		if err != nil {
			return payments.Response{}, notFound()
		}
		return s.svc.CreateRefund(ctx, k, id)
	}))
	s.mux.HandleFunc("POST /v1/webhooks/replay", s.idempotent(func(ctx context.Context, k idempotency.Key, _ *http.Request) (payments.Response, error) {
		return s.svc.ReplayWebhooks(ctx, k)
	}))
	s.mux.HandleFunc("GET /v1/refunds/{id}", s.merchant(func(w http.ResponseWriter, r *http.Request, m uuid.UUID) {
		id, ok := s.pathID(w, r)
		if ok {
			s.respond(w, r)(s.svc.GetRefund(r.Context(), m, id))
		}
	}))
	return s
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func notFound() *payments.Error {
	return &payments.Error{Status: http.StatusNotFound, Code: "not_found", Message: "not found"}
}

func (s *Server) pathID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		s.writeError(w, r, notFound())
		return uuid.Nil, false
	}
	return id, true
}

// merchant authenticates the bearer API key.
func (s *Server) merchant(h func(http.ResponseWriter, *http.Request, uuid.UUID)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		m, err := s.svc.Authenticate(r.Context(), strings.TrimSpace(key))
		if err != nil {
			if !errors.Is(err, payments.ErrUnauthorized) {
				s.log.Error("authenticate", "err", err)
			}
			s.writeError(w, r, &payments.Error{Status: http.StatusUnauthorized, Code: "unauthorized", Message: "missing or invalid API key"})
			return
		}
		h(w, r, m)
	}
}

// idempotent requires an Idempotency-Key and passes the raw body along.
func (s *Server) idempotent(h func(context.Context, idempotency.Key, *http.Request) (payments.Response, error)) http.HandlerFunc {
	return s.merchant(func(w http.ResponseWriter, r *http.Request, m uuid.UUID) {
		key := r.Header.Get("Idempotency-Key")
		if key == "" || len(key) > 255 {
			s.writeError(w, r, &payments.Error{Status: http.StatusBadRequest, Code: "idempotency_key_required",
				Message: "POST requests need an Idempotency-Key header of 1-255 characters"})
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
		if err != nil {
			s.writeError(w, r, &payments.Error{Status: http.StatusRequestEntityTooLarge, Code: "body_too_large", Message: err.Error()})
			return
		}
		resp, err := h(r.Context(), idempotency.Key{MerchantID: m, Key: key, Method: r.Method, Path: r.URL.Path, Body: body}, r)
		if err != nil {
			s.writeError(w, r, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.Status)
		w.Write(resp.Body)
	})
}

func (s *Server) admin(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tok, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if s.cfg.AdminToken == "" || subtle.ConstantTimeCompare([]byte(tok), []byte(s.cfg.AdminToken)) != 1 {
			s.writeError(w, r, &payments.Error{Status: http.StatusUnauthorized, Code: "unauthorized", Message: "admin token required"})
			return
		}
		h(w, r)
	}
}

func (s *Server) createMerchant(w http.ResponseWriter, r *http.Request) {
	var in payments.CreateMerchantInput
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody)).Decode(&in); err != nil {
		s.writeError(w, r, &payments.Error{Status: http.StatusBadRequest, Code: "bad_request", Message: err.Error()})
		return
	}
	out, err := s.svc.CreateMerchant(r.Context(), in)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

// runSettlement is POST /admin/settlements/run {"date": "YYYY-MM-DD"}: pay
// merchants for that IST business day. Safe to repeat.
func (s *Server) runSettlement(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Date string `json:"date"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody)).Decode(&in); err != nil {
		s.writeError(w, r, &payments.Error{Status: http.StatusBadRequest, Code: "bad_request", Message: err.Error()})
		return
	}
	day, err := time.ParseInLocation("2006-01-02", in.Date, payments.IST)
	if err != nil {
		s.writeError(w, r, &payments.Error{Status: http.StatusBadRequest, Code: "bad_request", Message: "date must be YYYY-MM-DD"})
		return
	}
	rep, err := s.svc.RunSettlement(r.Context(), day)
	if errors.Is(err, payments.ErrTooEarly) {
		s.writeError(w, r, &payments.Error{Status: http.StatusConflict, Code: "too_early", Message: "that day has not ended in IST"})
		return
	}
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

// bankCallback receives the bank's asynchronous answers. A bad signature is
// 401. A callback Kosh can't apply right now is 500, so the bank retries.
func (s *Server) bankCallback(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
		return
	}
	if s.cfg.BankCallbackSecret == "" || !bank.VerifyCallback(s.cfg.BankCallbackSecret, r.Header.Get(bank.HeaderCallbackSignature), body) {
		http.Error(w, "bad signature", http.StatusUnauthorized)
		return
	}
	var cb bank.Callback
	if err := json.Unmarshal(body, &cb); err != nil {
		http.Error(w, "bad callback", http.StatusBadRequest)
		return
	}
	err = s.svc.HandleBankCallback(r.Context(), cb)
	switch {
	case errors.Is(err, payments.ErrUnknownCallback):
		http.Error(w, "unknown id", http.StatusNotFound)
	case err != nil:
		s.log.Error("bank callback", "kind", cb.Kind, "id", cb.ID, "err", err)
		http.Error(w, "retry later", http.StatusInternalServerError)
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

// respond adapts a (value, error) pair into a JSON response.
func (s *Server) respond(w http.ResponseWriter, r *http.Request) func(any, error) {
	return func(v any, err error) {
		if err != nil {
			s.writeError(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, v)
	}
}

func (s *Server) writeError(w http.ResponseWriter, r *http.Request, err error) {
	var apiErr *payments.Error
	if !errors.As(err, &apiErr) {
		s.log.Error("request failed", "method", r.Method, "path", r.URL.Path, "err", err)
		apiErr = &payments.Error{Status: http.StatusInternalServerError, Code: "internal_error",
			Message: "something went wrong; retry with the same Idempotency-Key"}
	}
	writeJSON(w, apiErr.Status, apiErr.Body())
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
