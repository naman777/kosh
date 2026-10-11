// Command lab is Kosh's failure lab: it runs the whole system as child
// processes, drives traffic, and lets you break things on purpose while the
// dashboard (dashboard/) shows the ledger staying balanced.
//
//	LAB_ADDR            control API (default :8095)
//	LAB_BIN_DIR         where the Kosh binaries are (default: next to lab)
//	LAB_DATA            where the lab keeps its merchant credentials (default ./lab-data)
//	LAB_TRAFFIC         sales per second at start, and the default it returns to (default 5)
//	LAB_IDLE_AFTER      pause traffic when no dashboard has polled for this long (default 10m)
//	LAB_HEAL_AFTER      restart a killed or crashed process after this long (default 3m)
//	LAB_DEFAULTS_AFTER  return bank, merchant endpoint and traffic to defaults after this long (default 15m)
//	LAB_RESET_AT        wipe the lab's data every day at HH:MM, or "off" (default 04:00)
//	LAB_TZ              time zone for LAB_RESET_AT (default Asia/Kolkata)
//	KOSH_DATABASE_URL   Postgres URL (default: kosh_lab on the compose Postgres, created if missing)
//	KOSH_KAFKA_BROKERS  Redpanda brokers (default localhost:19092)
//
// API:
//
//	GET  /lab/state                       everything the dashboard shows
//	POST /lab/process/{name}/kill|start   api, worker, relay, dispatcher, banksim, samplemerchant
//	POST /lab/bank      {"preset": "healthy" | "flaky" | "late" | "outage"}
//	POST /lab/merchant  {"mode": "ok" | "fail" | "hang"}
//	POST /lab/traffic   {"rate": 0..100}
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/naman777/kosh/internal/bank"
	"github.com/naman777/kosh/internal/db"
	"github.com/naman777/kosh/internal/env"
	"github.com/naman777/kosh/internal/metrics"
	"github.com/naman777/kosh/internal/payments"
	"github.com/naman777/kosh/invariants"
)

const (
	apiURL      = "http://localhost:8090"
	bankURL     = "http://localhost:8091"
	merchantURL = "http://localhost:8092"
)

// order is how processes start: the bank and merchant before Kosh.
var order = []string{"banksim", "samplemerchant", "api", "worker", "relay", "dispatcher"}

type creds struct {
	MerchantID    string `json:"merchant_id"`
	APIKey        string `json:"api_key"`
	WebhookSecret string `json:"webhook_secret"`
}

type event struct {
	At   time.Time `json:"at"`
	What string    `json:"what"`
}

type lab struct {
	log      *slog.Logger
	pool     atomic.Pointer[pgxpool.Pool] // replaced by a nightly reset; use db()
	dbURL    string
	binDir   string
	procs    map[string]*proc
	traffic  *traffic
	hc       *http.Client
	cbSecret string
	up       upkeep
	wake     chan struct{} // runs the invariant checks now

	mu         sync.Mutex
	events     []event
	bankPreset string
	mode       string
	inv        invariantsView
	// What visitors changed, and when, so upkeep can undo it.
	wantRate                        int
	bankSince, modeSince, rateSince time.Time
	lastSeen                        time.Time
	paused                          bool // no one is watching: traffic stopped
	resetting                       bool
}

type invariantsView struct {
	OK        bool      `json:"ok"`
	CheckedAt time.Time `json:"checked_at"`
	Held      int       `json:"held"`
	Total     int       `json:"total"`
	Failures  []string  `json:"failures"`
	Error     string    `json:"error,omitempty"`
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("lab exiting", "err", err)
		os.Exit(1)
	}
}

func token() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dbURL := env.String("KOSH_DATABASE_URL", "postgres://kosh:kosh@localhost:5442/kosh_lab?sslmode=disable")
	if err := ensureDatabase(ctx, dbURL); err != nil {
		return err
	}
	pool, err := db.Open(ctx, dbURL)
	if err != nil {
		return err
	}
	if err := db.Migrate(ctx, pool); err != nil {
		pool.Close()
		return err
	}
	c, err := merchantCreds(ctx, pool)
	if err != nil {
		pool.Close()
		return err
	}

	up, err := upkeepFromEnv()
	if err != nil {
		return err
	}
	exe, _ := os.Executable()
	admin, cbSecret := token(), token()
	l := &lab{log: log, dbURL: dbURL, binDir: env.String("LAB_BIN_DIR", filepath.Dir(exe)),
		procs: map[string]*proc{}, hc: &http.Client{Timeout: 5 * time.Second}, mode: "ok",
		cbSecret: cbSecret, up: up, wake: make(chan struct{}, 1), lastSeen: time.Now()}
	l.pool.Store(pool)
	defer func() { l.db().Close() }()
	common := []string{
		"KOSH_DATABASE_URL=" + dbURL,
		"KOSH_KAFKA_BROKERS=" + env.String("KOSH_KAFKA_BROKERS", "localhost:19092"),
		"KOSH_ADMIN_TOKEN=" + admin,
		"KOSH_BANK_CALLBACK_SECRET=" + cbSecret,
		"KOSH_BANK_TIMEOUT=3s",
		"KOSH_WORKER_INTERVAL=500ms",
		"KOSH_WEBHOOK_SEND_TIMEOUT=3s",
	}
	extra := map[string][]string{
		"banksim":        {"BANKSIM_ADDR=:8091"},
		"samplemerchant": {"SAMPLE_ADDR=:8092", "SAMPLE_SECRET=" + c.WebhookSecret},
		"api":            {"KOSH_ADDR=:8090"},
		"worker":         {"KOSH_METRICS_ADDR=:9103"},
		"relay":          {"KOSH_METRICS_ADDR=:9101"},
		"dispatcher":     {"KOSH_METRICS_ADDR=:9102"},
	}
	for _, name := range order {
		l.procs[name] = &proc{name: name, env: append(append([]string{}, common...), extra[name]...)}
	}
	for _, name := range order {
		if err := l.procs[name].start(l.binDir); err != nil {
			return err
		}
		l.note("started " + name)
	}
	l.waitHealthy(ctx)
	l.setBank(ctx, "healthy")
	l.traffic = newTraffic(apiURL, c.APIKey)
	l.setTraffic(up.defaultRate)

	go l.checkLoop(ctx)
	go l.upkeepLoop(ctx)
	go l.resetLoop(ctx)
	metrics.Serve(":9104", log)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /lab/state", func(w http.ResponseWriter, r *http.Request) {
		l.touch()
		l.state(w, r)
	})
	mux.HandleFunc("POST /lab/process/{name}/{action}", func(w http.ResponseWriter, r *http.Request) {
		l.touch()
		l.processAction(w, r)
	})
	mux.HandleFunc("POST /lab/bank", func(w http.ResponseWriter, r *http.Request) {
		l.touch()
		var in struct{ Preset string }
		json.NewDecoder(r.Body).Decode(&in)
		if err := l.setBank(r.Context(), in.Preset); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /lab/merchant", func(w http.ResponseWriter, r *http.Request) {
		l.touch()
		var in struct{ Mode string }
		json.NewDecoder(r.Body).Decode(&in)
		if err := l.setMerchant(r.Context(), in.Mode); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /lab/traffic", func(w http.ResponseWriter, r *http.Request) {
		l.touch()
		var in struct{ Rate int }
		json.NewDecoder(r.Body).Decode(&in)
		l.setTraffic(max(0, min(in.Rate, 100)))
		w.WriteHeader(http.StatusNoContent)
	})
	addr := env.String("LAB_ADDR", ":8095")
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { <-ctx.Done(); srv.Shutdown(context.Background()) }()
	log.Info("lab running", "addr", addr, "bin_dir", l.binDir)
	err = srv.ListenAndServe()

	l.traffic.setRate(0)
	for _, p := range l.procs {
		p.kill()
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (l *lab) db() *pgxpool.Pool { return l.pool.Load() }

// ensureDatabase creates the lab's database if it doesn't exist, so the
// demo never shares (or trips over) a development database.
func ensureDatabase(ctx context.Context, raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	name := strings.TrimPrefix(u.Path, "/")
	u.Path = "/postgres"
	conn, err := pgx.Connect(ctx, u.String())
	if err != nil {
		return fmt.Errorf("lab: connect to create %s: %w", name, err)
	}
	defer conn.Close(ctx)
	var exists bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, name).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}
	_, err = conn.Exec(ctx, `CREATE DATABASE `+pgx.Identifier{name}.Sanitize())
	return err
}

// merchantCreds loads the lab merchant, creating it on first start. The API
// key is only shown once, so the lab keeps it in LAB_DATA.
func merchantCreds(ctx context.Context, pool *pgxpool.Pool) (creds, error) {
	dir := env.String("LAB_DATA", "lab-data")
	path := filepath.Join(dir, "merchant.json")
	var c creds
	if raw, err := os.ReadFile(path); err == nil && json.Unmarshal(raw, &c) == nil {
		var exists bool
		pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM merchants WHERE id::text = $1)`, c.MerchantID).Scan(&exists)
		if exists {
			return c, nil
		}
	}
	svc, err := payments.New(ctx, payments.Deps{Pool: pool, Bank: bank.NewFake()})
	if err != nil {
		return c, err
	}
	url := merchantURL + "/webhooks"
	m, err := svc.CreateMerchant(ctx, payments.CreateMerchantInput{Name: "Failure Lab Chai Co.", WebhookURL: &url})
	if err != nil {
		return c, err
	}
	c = creds{MerchantID: m.Merchant.ID.String(), APIKey: m.APIKey, WebhookSecret: m.WebhookSecret}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return c, err
	}
	raw, _ := json.Marshal(c)
	return c, os.WriteFile(path, raw, 0o600)
}

func (l *lab) note(what string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, event{At: time.Now(), What: what})
	if len(l.events) > 50 {
		l.events = l.events[len(l.events)-50:]
	}
	l.log.Info("lab", "event", what)
}

func (l *lab) waitHealthy(ctx context.Context) {
	for i := 0; i < 100 && ctx.Err() == nil; i++ {
		resp, err := l.hc.Get(apiURL + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (l *lab) put(ctx context.Context, url string, body any) error {
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(raw))
	resp, err := l.hc.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s: %d", url, resp.StatusCode)
	}
	return nil
}

// bankPresets are the moods the dashboard offers.
var bankPresets = map[string]map[string]any{
	"healthy": {},
	"flaky":   {"fail_rate": 0.2, "timeout_rate": 0.1, "pending_rate": 0.2, "final_after_ms": 4000, "max_duplicate_callbacks": 3},
	"late":    {"timeout_rate": 0.3, "pending_rate": 0.5, "final_after_ms": 15000, "hang_for_ms": 5000, "max_duplicate_callbacks": 2},
	"outage":  {"outage": true},
}

func (l *lab) setBank(ctx context.Context, preset string) error {
	p, ok := bankPresets[preset]
	if !ok {
		return fmt.Errorf("unknown bank preset %q", preset)
	}
	cfg := map[string]any{"latency_min_ms": 5, "latency_max_ms": 30, "hang_for_ms": 35000, "final_after_ms": 2000,
		"callback_url": apiURL + "/bank/callbacks", "callback_secret": l.cbSecret, "callback_delay_ms": 200, "max_duplicate_callbacks": 1}
	for k, v := range p {
		cfg[k] = v
	}
	if err := l.put(ctx, bankURL+"/admin/config", cfg); err != nil {
		return err
	}
	l.mu.Lock()
	l.bankPreset, l.bankSince = preset, time.Now()
	l.mu.Unlock()
	l.note("bank → " + preset)
	return nil
}

func (l *lab) setMerchant(ctx context.Context, mode string) error {
	if err := l.put(ctx, merchantURL+"/admin/mode", map[string]string{"mode": mode}); err != nil {
		return err
	}
	l.mu.Lock()
	l.mode, l.modeSince = mode, time.Now()
	l.mu.Unlock()
	l.note("merchant endpoint → " + mode)
	return nil
}

func (l *lab) processAction(w http.ResponseWriter, r *http.Request) {
	p, ok := l.procs[r.PathValue("name")]
	if !ok {
		http.Error(w, "unknown process", http.StatusNotFound)
		return
	}
	var err error
	switch r.PathValue("action") {
	case "kill":
		err = p.kill()
		l.note("killed " + p.name + " (SIGKILL)")
	case "start":
		err = p.start(l.binDir)
		l.note("started " + p.name)
	default:
		http.Error(w, "action must be kill or start", http.StatusBadRequest)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// checkLoop runs the invariants every few seconds (every 30 s when no one is
// watching). Invariant 8 ("every event
// is eventually delivered") is left out: while the lab is breaking things it
// is supposed to be temporarily false, and the backlog tile shows it.
func (l *lab) checkLoop(ctx context.Context) {
	ids := []int{1, 2, 3, 4, 5, 6, 7, 9, 10, 11}
	for ctx.Err() == nil {
		cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		rep, err := invariants.Check(cctx, l.db(), ids...)
		cancel()
		v := invariantsView{CheckedAt: time.Now(), Total: len(ids)}
		if err != nil {
			v.Error = err.Error()
		} else {
			for _, res := range rep.Results {
				if res.OK() {
					v.Held++
				} else {
					v.Failures = append(v.Failures, fmt.Sprintf("#%d %s (%d)", res.ID, res.Description, len(res.Violations)))
				}
			}
			v.OK = len(v.Failures) == 0
		}
		l.mu.Lock()
		l.inv = v
		l.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-l.wake:
		case <-time.After(l.checkInterval()):
		}
	}
}

func upkeepFromEnv() (upkeep, error) {
	up := upkeep{
		idleAfter:     env.Duration("LAB_IDLE_AFTER", 10*time.Minute),
		healAfter:     env.Duration("LAB_HEAL_AFTER", 3*time.Minute),
		defaultsAfter: env.Duration("LAB_DEFAULTS_AFTER", 15*time.Minute),
		defaultRate:   max(0, min(env.Int("LAB_TRAFFIC", 5), 100)),
		resetAt:       env.String("LAB_RESET_AT", "04:00"),
	}
	if up.resetAt == "off" {
		up.resetAt = ""
	}
	loc, err := time.LoadLocation(env.String("LAB_TZ", "Asia/Kolkata"))
	if err != nil {
		return up, fmt.Errorf("LAB_TZ: %w", err)
	}
	up.loc = loc
	if up.resetAt != "" {
		if _, err := nextReset(time.Now(), up.resetAt, loc); err != nil {
			return up, err
		}
	}
	return up, nil
}
