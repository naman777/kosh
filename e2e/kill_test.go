// Package e2e runs Kosh's real binaries as separate processes and kills
// them. It needs Redpanda: set KOSH_KAFKA_BROKERS (for example
// localhost:19092 with docker compose up) or the tests are skipped.
package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/naman777/kosh/internal/bank"
	"github.com/naman777/kosh/internal/clock"
	"github.com/naman777/kosh/internal/crash"
	"github.com/naman777/kosh/internal/idempotency"
	"github.com/naman777/kosh/internal/payments"
	"github.com/naman777/kosh/internal/testdb"
	"github.com/naman777/kosh/internal/webhooks"
	"github.com/naman777/kosh/invariants"
)

var (
	buildOnce sync.Once
	binDir    string
	buildErr  error
)

func binaries(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		binDir, buildErr = os.MkdirTemp("", "kosh-bin-")
		if buildErr != nil {
			return
		}
		goBin := filepath.Join(runtime.GOROOT(), "bin", "go")
		for _, name := range []string{"relay", "dispatcher"} {
			out := filepath.Join(binDir, name)
			if runtime.GOOS == "windows" {
				out += ".exe"
			}
			cmd := exec.Command(goBin, "build", "-o", out, "../cmd/"+name)
			if b, err := cmd.CombinedOutput(); err != nil {
				buildErr = fmt.Errorf("build %s: %v\n%s", name, err, b)
				return
			}
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return binDir
}

type proc struct {
	name string
	cmd  *exec.Cmd
	out  *bytes.Buffer
	done chan error
}

type harness struct {
	t     *testing.T
	ctx   context.Context
	pool  *pgxpool.Pool
	env   []string
	bin   string
	svc   *payments.Service
	m     uuid.UUID
	recv  *webhooks.Receiver
	seq   int
	procs []*proc
}

func (h *harness) start(name, crashpoint string) *proc {
	h.t.Helper()
	exe := filepath.Join(h.bin, name)
	if runtime.GOOS == "windows" {
		exe += ".exe"
	}
	cmd := exec.Command(exe)
	cmd.Env = append(append([]string{}, h.env...), "KOSH_CRASHPOINT="+crashpoint)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		h.t.Fatal(err)
	}
	p := &proc{name: name, cmd: cmd, out: &out, done: make(chan error, 1)}
	go func() { p.done <- cmd.Wait() }()
	h.procs = append(h.procs, p)
	return p
}

// waitCrash waits for p to exit with the crashpoint status.
func (h *harness) waitCrash(p *proc) {
	h.t.Helper()
	select {
	case err := <-p.done:
		var ee *exec.ExitError
		if !errors.As(err, &ee) || ee.ExitCode() != crash.ExitCode {
			h.t.Fatalf("%s exited with %v, want status %d\n%s", p.name, err, crash.ExitCode, p.out)
		}
	case <-time.After(60 * time.Second):
		h.t.Fatalf("%s never hit its crashpoint\n%s", p.name, p.out)
	}
}

func (h *harness) kill(p *proc) {
	_ = p.cmd.Process.Kill()
	<-p.done
}

func (h *harness) call(fn func(context.Context, idempotency.Key) (payments.Response, error), path string, body any) []byte {
	h.t.Helper()
	h.seq++
	raw, _ := json.Marshal(body)
	r, err := fn(h.ctx, idempotency.Key{MerchantID: h.m, Key: fmt.Sprint("e2e-", h.seq), Method: "POST", Path: path, Body: raw})
	if err != nil || r.Status >= 300 {
		h.t.Fatalf("%s: %v %d %s", path, err, r.Status, r.Body)
	}
	return r.Body
}

func (h *harness) sales(n int) {
	h.t.Helper()
	for i := 0; i < n; i++ {
		var o payments.OrderView
		json.Unmarshal(h.call(h.svc.CreateOrder, "/v1/orders", map[string]any{"amount": 10_000 + i}), &o)
		var p payments.PaymentView
		json.Unmarshal(h.call(h.svc.CreatePayment, "/v1/payments", map[string]any{"order_id": o.ID, "method": "upi"}), &p)
		h.call(func(ctx context.Context, k idempotency.Key) (payments.Response, error) {
			return h.svc.Capture(ctx, k, p.ID)
		},
			"/v1/payments/"+p.ID.String()+"/capture", map[string]any{})
	}
}

func (h *harness) count(sql string) int {
	var n int
	if err := h.pool.QueryRow(h.ctx, sql).Scan(&n); err != nil {
		h.t.Fatal(err)
	}
	return n
}

func (h *harness) eventually(what string, cond func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	for _, p := range h.procs {
		h.t.Logf("--- %s output ---\n%s", p.name, p.out)
	}
	h.t.Fatalf("timed out waiting for %s", what)
}

// TestKillRelayAndDispatcher kills both binaries at their worst moments and
// once with no warning at all, restarts them, and checks that every event
// still reaches the merchant and every invariant holds.
func TestKillRelayAndDispatcher(t *testing.T) {
	brokers := os.Getenv("KOSH_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("KOSH_KAFKA_BROKERS not set; start Redpanda with docker compose up and set it to localhost:19092")
	}
	ctx := context.Background()
	pool, url := testdb.NewWithURL(t)

	var handler http.Handler = http.NotFoundHandler()
	var hmu sync.Mutex
	merchantSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hmu.Lock()
		h := handler
		hmu.Unlock()
		h.ServeHTTP(w, r)
	}))
	defer merchantSrv.Close()

	svc, err := payments.New(ctx, payments.Deps{Pool: pool, Bank: bank.NewFake(),
		Config: payments.Config{BankTimeout: time.Second, RefundHoldTTL: time.Hour}})
	if err != nil {
		t.Fatal(err)
	}
	hookURL := merchantSrv.URL + "/webhooks"
	m, err := svc.CreateMerchant(ctx, payments.CreateMerchantInput{Name: "Kill Test", WebhookURL: &hookURL})
	if err != nil {
		t.Fatal(err)
	}
	recv := webhooks.NewReceiver(m.WebhookSecret, clock.Real{})
	hmu.Lock()
	handler = recv
	hmu.Unlock()

	var suffix [4]byte
	rand.Read(suffix[:])
	tag := hex.EncodeToString(suffix[:])
	h := &harness{t: t, ctx: ctx, pool: pool, bin: binaries(t), svc: svc, m: m.Merchant.ID, recv: recv,
		env: append(os.Environ(),
			"KOSH_DATABASE_URL="+url,
			"KOSH_KAFKA_BROKERS="+brokers,
			"KOSH_KAFKA_TOPIC=kosh.e2e."+tag,
			"KOSH_DISPATCH_GROUP=e2e-"+tag,
			"KOSH_WEBHOOK_SEND_TIMEOUT=1s",
		)}
	t.Cleanup(func() {
		for _, p := range h.procs {
			if p.cmd.ProcessState == nil {
				_ = p.cmd.Process.Kill()
			}
		}
	})

	h.sales(10)
	events := h.count(`SELECT count(*) FROM outbox`)

	// 1. The relay publishes a batch to Kafka and dies before marking it.
	h.waitCrash(h.start("relay", "relay.after_publish"))
	if n := h.count(`SELECT count(*) FROM outbox WHERE published_at IS NOT NULL`); n != 0 {
		t.Fatalf("%d rows marked published by a relay that crashed before marking", n)
	}

	// 2. A healthy relay republishes. The dispatcher sends the first batch
	//    and dies before recording any of it.
	relay := h.start("relay", "")
	h.eventually("relay to publish everything", func() bool {
		return h.count(`SELECT count(*) FROM outbox WHERE published_at IS NULL`) == 0
	})
	h.waitCrash(h.start("dispatcher", "dispatcher.after_send"))
	if recv.Stats().Unique == 0 {
		t.Fatal("dispatcher crashed before sending anything")
	}

	// 3. More traffic, then the relay is killed with no warning and restarted.
	h.sales(10)
	time.Sleep(100 * time.Millisecond)
	h.kill(relay)
	h.start("relay", "")
	events = h.count(`SELECT count(*) FROM outbox`)

	// 4. A healthy dispatcher catches up: duplicates allowed, losses not.
	h.start("dispatcher", "")
	h.eventually("every event to be delivered", func() bool {
		rep, err := invariants.Check(ctx, pool, 8)
		return err == nil && rep.OK() && h.count(`SELECT count(*) FROM outbox WHERE published_at IS NULL`) == 0
	})

	s := recv.Stats()
	t.Logf("%d events, merchant saw %d unique and %d duplicates", events, s.Unique, s.Duplicates)
	if s.Unique != events || s.Rejected != 0 {
		t.Fatalf("merchant stats %+v for %d events", s, events)
	}
	if s.Duplicates == 0 {
		t.Fatal("the dispatcher crash should have caused at least one duplicate")
	}
	rep, err := invariants.Check(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if err := rep.Err(); err != nil {
		t.Fatal(err)
	}
}
