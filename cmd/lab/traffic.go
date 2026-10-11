package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// traffic plays customers buying from the lab merchant: order → pay →
// capture, with an occasional partial refund. It retries like a well-behaved
// client: same Idempotency-Key, on 5xx, 409 or a dropped connection.
type traffic struct {
	base string
	hc   *http.Client

	mu   sync.Mutex
	key  string // the lab merchant's API key; replaced by a nightly reset
	rate int    // sales per second; 0 = stopped
	stop chan struct{}

	started, completed, failed, refunds, retries atomic.Int64
	seq                                          atomic.Int64
}

type trafficView struct {
	Rate      int   `json:"rate"`
	Started   int64 `json:"started"`
	Completed int64 `json:"completed"`
	Failed    int64 `json:"failed"`
	Refunds   int64 `json:"refunds"`
	Retries   int64 `json:"retries"`
}

func newTraffic(base, key string) *traffic {
	return &traffic{base: base, key: key, hc: &http.Client{Timeout: 10 * time.Second}}
}

func (t *traffic) view() trafficView {
	t.mu.Lock()
	rate := t.rate
	t.mu.Unlock()
	return trafficView{Rate: rate, Started: t.started.Load(), Completed: t.completed.Load(),
		Failed: t.failed.Load(), Refunds: t.refunds.Load(), Retries: t.retries.Load()}
}

// restart points traffic at a new merchant and zeroes the counters. Call it
// with traffic stopped.
func (t *traffic) restart(key string) {
	t.mu.Lock()
	t.key = key
	t.mu.Unlock()
	for _, c := range []*atomic.Int64{&t.started, &t.completed, &t.failed, &t.refunds, &t.retries} {
		c.Store(0)
	}
}

// setRate changes the arrival rate; 0 stops traffic.
func (t *traffic) setRate(rate int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stop != nil {
		close(t.stop)
		t.stop = nil
	}
	t.rate = rate
	if rate <= 0 {
		return
	}
	stop := make(chan struct{})
	t.stop = stop
	go func() {
		tick := time.NewTicker(time.Second / time.Duration(rate))
		defer tick.Stop()
		sem := make(chan struct{}, 200)
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				select {
				case sem <- struct{}{}:
					go func() { defer func() { <-sem }(); t.sale() }()
				default: // too many in flight: the API is struggling, shed load
				}
			}
		}
	}()
}

// call POSTs with retries and returns the decoded body of the final 2xx/4xx.
func (t *traffic) call(path, key string, body any) (int, map[string]any) {
	raw, _ := json.Marshal(body)
	t.mu.Lock()
	apiKey := t.key
	t.mu.Unlock()
	for attempt := 0; attempt < 6; attempt++ {
		if attempt > 0 {
			t.retries.Add(1)
			time.Sleep(time.Duration(200*attempt) * time.Millisecond)
		}
		req, _ := http.NewRequest(http.MethodPost, t.base+path, bytes.NewReader(raw))
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Idempotency-Key", key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := t.hc.Do(req)
		if err != nil {
			continue
		}
		var out map[string]any
		json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		if resp.StatusCode >= 500 || resp.StatusCode == http.StatusConflict {
			continue
		}
		return resp.StatusCode, out
	}
	return 0, nil
}

func (t *traffic) sale() {
	t.started.Add(1)
	id := fmt.Sprintf("lab-%d-%d", time.Now().UnixNano(), t.seq.Add(1))
	amount := 500 + rand.Int64N(500_000)
	code, order := t.call("/v1/orders", "o-"+id, map[string]any{"amount": amount, "receipt": id})
	if code != http.StatusCreated {
		t.failed.Add(1)
		return
	}
	method := []string{"upi", "card"}[rand.IntN(2)]
	code, pay := t.call("/v1/payments", "p-"+id, map[string]any{"order_id": order["id"], "method": method})
	if code != http.StatusCreated {
		t.failed.Add(1)
		return
	}
	if pay["status"] != "authorized" {
		t.completed.Add(1) // declined or pending: a legitimate outcome
		return
	}
	pid, _ := pay["id"].(string)
	if code, _ := t.call("/v1/payments/"+pid+"/capture", "c-"+id, nil); code != http.StatusOK {
		t.failed.Add(1)
		return
	}
	t.completed.Add(1)
	if rand.IntN(100) < 15 {
		t.call("/v1/payments/"+pid+"/refunds", "r-"+id, map[string]any{"amount": amount * 3 / 10})
		t.refunds.Add(1)
	}
}
