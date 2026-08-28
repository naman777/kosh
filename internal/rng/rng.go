// Package rng puts randomness behind an interface so a simulation seed fixes
// every random choice (retry jitter, workload, faults).
package rng

import (
	"math/rand/v2"
	"sync"
)

// RNG is the randomness Kosh code may use.
type RNG interface {
	Int64N(n int64) int64
	Float64() float64
}

// Locked is a seeded RNG safe for concurrent use.
type Locked struct {
	mu sync.Mutex
	r  *rand.Rand
}

// New returns a seeded RNG.
func New(seed uint64) *Locked { return &Locked{r: rand.New(rand.NewPCG(seed, 0x726e67))} }

// Int64N returns a value in [0, n).
func (l *Locked) Int64N(n int64) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.r.Int64N(n)
}

// Float64 returns a value in [0, 1).
func (l *Locked) Float64() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.r.Float64()
}
