// Package metrics holds Kosh's Prometheus metrics. Every binary serves
// /metrics; each records only the metrics it owns.
package metrics

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry holds every Kosh metric plus Go runtime and process metrics.
var Registry = prometheus.NewRegistry()

// Latency buckets from 1 ms to 10 s.
var latency = []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}

var (
	HTTPRequest = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "kosh_http_request_seconds", Help: "API request latency by route and status code.", Buckets: latency,
	}, []string{"route", "code"})

	BankCall = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "kosh_bank_call_seconds", Help: "Bank call latency by operation and outcome.", Buckets: latency,
	}, []string{"op", "outcome"})

	LedgerPost = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "kosh_ledger_post_seconds", Help: "Time to write one journal entry inside its transaction.", Buckets: latency,
	})

	LedgerLockWait = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "kosh_ledger_lock_wait_seconds", Help: "Time spent acquiring account row locks (SELECT … FOR UPDATE).", Buckets: latency,
	})

	WebhookDelivery = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "kosh_webhook_delivery_seconds", Help: "Event creation to successful delivery.",
		Buckets: []float64{.05, .1, .25, .5, 1, 2, 5, 10, 30, 60, 300, 1800, 3600},
	})

	WebhookAttempts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kosh_webhook_attempts_total", Help: "Webhook delivery attempts by result.",
	}, []string{"result"})

	PollResolved = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kosh_poll_resolved_total", Help: "Items the worker's pollers resolved, by kind.",
	}, []string{"kind"})

	Payments = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kosh_payments", Help: "Payments by state.",
	}, []string{"state"})

	ReconOpenExceptions = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kosh_recon_open_exceptions", Help: "Open reconciliation exceptions.",
	})
)

func init() {
	Registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		HTTPRequest, BankCall, LedgerPost, LedgerLockWait, WebhookDelivery, WebhookAttempts,
		PollResolved, Payments, ReconOpenExceptions,
	)
}

// Handler serves the registry.
func Handler() http.Handler {
	return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{Registry: Registry})
}

// Serve starts a /metrics server on addr in the background. An empty addr
// does nothing.
func Serve(addr string, log *slog.Logger) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", Handler())
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && log != nil {
			log.Error("metrics server", "addr", addr, "err", err)
		}
	}()
}

// DBGauge registers a gauge computed by a SQL query on every scrape.
func DBGauge(pool *pgxpool.Pool, name, help, sql string) {
	Registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help}, func() float64 {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		var v float64
		if err := pool.QueryRow(ctx, sql).Scan(&v); err != nil {
			return -1
		}
		return v
	}))
}

// Since observes the seconds elapsed from start.
func Since(h prometheus.Observer, start time.Time) { h.Observe(time.Since(start).Seconds()) }

// Code formats an HTTP status code as a label.
func Code(c int) string { return strconv.Itoa(c) }
