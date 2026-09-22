// Package crash provides named crashpoints for failure testing.
//
// Code calls crash.At(ctx, "relay.after_publish") at places where a process
// death would be interesting. Normally that is a no-op. When the environment
// variable KOSH_CRASHPOINT names the point, the process exits immediately
// with status 137, as if it had been SIGKILLed: no deferred calls, no
// rollbacks, no flushes. Tests can instead Arm a point in-process with a
// handler (usually a panic) so they don't need a subprocess.
package crash

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
)

// ExitCode is what a crashed process exits with (128 + SIGKILL).
const ExitCode = 137

// Crashed is the value a test handler panics with.
type Crashed struct{ Point string }

func (c Crashed) Error() string { return "crashed at " + c.Point }

type arm struct {
	remaining int // crashes left; < 0 means unlimited
	handler   func(point string)
}

var (
	mu    sync.Mutex
	armed = map[string]*arm{}
)

func init() {
	// KOSH_CRASHPOINT=name or name:count, comma separated.
	for _, spec := range strings.Split(os.Getenv("KOSH_CRASHPOINT"), ",") {
		spec = strings.TrimSpace(spec)
		if spec == "" {
			continue
		}
		name, count := spec, 1
		if n, c, ok := strings.Cut(spec, ":"); ok {
			name = n
			fmt.Sscan(c, &count)
		}
		armed[name] = &arm{remaining: count, handler: exit}
	}
}

func exit(point string) {
	fmt.Fprintf(os.Stderr, "kosh: crashpoint %s hit, exiting %d\n", point, ExitCode)
	os.Exit(ExitCode)
}

// Point crashes the process if name is armed.
func Point(name string) {
	mu.Lock()
	a, ok := armed[name]
	if !ok || a.remaining == 0 {
		mu.Unlock()
		return
	}
	if a.remaining > 0 {
		a.remaining--
	}
	h := a.handler
	mu.Unlock()
	h(name)
}

// Arm makes the next count hits of name call handler (count < 0: every hit).
// A nil handler panics with Crashed.
func Arm(name string, count int, handler func(point string)) {
	if handler == nil {
		handler = func(p string) { panic(Crashed{Point: p}) }
	}
	mu.Lock()
	defer mu.Unlock()
	armed[name] = &arm{remaining: count, handler: handler}
}

// Disarm removes every armed point.
func Disarm() {
	mu.Lock()
	defer mu.Unlock()
	armed = map[string]*arm{}
}

// Recover runs fn and reports whether it crashed at an armed point. Other
// panics propagate.
func Recover(fn func()) (point string, crashed bool) {
	defer func() {
		if r := recover(); r != nil {
			c, ok := r.(Crashed)
			if !ok {
				panic(r)
			}
			point, crashed = c.Point, true
		}
	}()
	fn()
	return "", false
}

type ctxKey struct{}

// Arms is a set of crashpoints armed for one context, so concurrent
// simulation runs in one process don't trigger each other's crashes.
type Arms struct {
	mu sync.Mutex
	m  map[string]int
}

// WithArms returns a context carrying a fresh, empty set of arms.
func WithArms(ctx context.Context) (context.Context, *Arms) {
	a := &Arms{m: map[string]int{}}
	return context.WithValue(ctx, ctxKey{}, a), a
}

// Arm makes the next count hits of name, under this context, panic with
// Crashed.
func (a *Arms) Arm(name string, count int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.m[name] = count
}

// Clear disarms everything.
func (a *Arms) Clear() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.m = map[string]int{}
}

func (a *Arms) take(name string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.m[name] > 0 {
		a.m[name]--
		return true
	}
	return false
}

// At is Point plus the arms carried by ctx, if any.
func At(ctx context.Context, name string) {
	if a, ok := ctx.Value(ctxKey{}).(*Arms); ok && a.take(name) {
		panic(Crashed{Point: name})
	}
	Point(name)
}
