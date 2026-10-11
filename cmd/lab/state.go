package main

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

type ledgerView struct {
	PostedDr  int64 `json:"posted_dr"`
	PostedCr  int64 `json:"posted_cr"`
	PendingDr int64 `json:"pending_dr"`
	PendingCr int64 `json:"pending_cr"`
	Entries   int64 `json:"entries"`
	Balanced  bool  `json:"balanced"`
}

type stateView struct {
	Now        time.Time        `json:"now"`
	Processes  []procView       `json:"processes"`
	Traffic    trafficView      `json:"traffic"`
	Bank       string           `json:"bank"`
	Merchant   string           `json:"merchant_endpoint"`
	Ledger     ledgerView       `json:"ledger"`
	Payments   map[string]int64 `json:"payments"`
	Outbox     map[string]any   `json:"outbox"`
	Webhooks   map[string]int64 `json:"webhooks"`
	Recon      map[string]int64 `json:"recon"`
	Payouts    map[string]int64 `json:"payouts"`
	Invariants invariantsView   `json:"invariants"`
	Events     []event          `json:"events"`
	Error      string           `json:"error,omitempty"`
}

func (l *lab) state(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	pool := l.db()
	v := stateView{Now: time.Now(), Payments: map[string]int64{}, Outbox: map[string]any{},
		Webhooks: map[string]int64{}, Recon: map[string]int64{}, Payouts: map[string]int64{}}
	withLogs := r.URL.Query().Get("logs") == "1"
	for _, name := range order {
		v.Processes = append(v.Processes, l.procs[name].view(withLogs))
	}
	v.Traffic = l.traffic.view()
	l.mu.Lock()
	v.Bank, v.Merchant, v.Invariants = l.bankPreset, l.mode, l.inv
	v.Events = append([]event(nil), l.events...)
	l.mu.Unlock()

	if err := pool.QueryRow(ctx, `SELECT COALESCE(sum(posted_dr), 0), COALESCE(sum(posted_cr), 0),
		COALESCE(sum(pending_dr), 0), COALESCE(sum(pending_cr), 0), (SELECT count(*) FROM journal_entries) FROM accounts`).Scan(
		&v.Ledger.PostedDr, &v.Ledger.PostedCr, &v.Ledger.PendingDr, &v.Ledger.PendingCr, &v.Ledger.Entries); err != nil {
		v.Error = err.Error()
	}
	v.Ledger.Balanced = v.Ledger.PostedDr == v.Ledger.PostedCr && v.Ledger.PendingDr == v.Ledger.PendingCr

	counts := func(sql string, into map[string]int64) {
		rows, err := pool.Query(ctx, sql)
		if err != nil {
			return
		}
		defer rows.Close()
		for rows.Next() {
			var (
				k string
				n int64
			)
			if rows.Scan(&k, &n) == nil {
				into[k] = n
			}
		}
	}
	counts(`SELECT state, count(*) FROM payments GROUP BY state`, v.Payments)
	counts(`SELECT state, count(*) FROM payouts GROUP BY state`, v.Payouts)
	counts(`SELECT CASE WHEN delivered_at IS NOT NULL THEN 'delivered' ELSE 'backlog' END, count(*)
		FROM webhook_deliveries GROUP BY 1`, v.Webhooks)
	counts(`SELECT CASE WHEN status = 'matched' THEN 'matched'
	                    WHEN resolved_at IS NULL THEN 'open' ELSE 'resolved' END, count(*)
		FROM recon_results GROUP BY 1`, v.Recon)
	var (
		unpublished int64
		oldest      float64
		published   int64
	)
	pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE published_at IS NULL), count(*) FILTER (WHERE published_at IS NOT NULL),
		COALESCE(EXTRACT(EPOCH FROM now() - min(created_at) FILTER (WHERE published_at IS NULL)), 0) FROM outbox`).Scan(
		&unpublished, &published, &oldest)
	v.Outbox["unpublished"], v.Outbox["published"], v.Outbox["oldest_seconds"] = unpublished, published, oldest

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(v)
}
