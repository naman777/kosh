// Package ids puts id generation behind an interface so a simulation seed
// fixes every id in a run.
package ids

import (
	"math/rand/v2"
	"sync"

	"github.com/google/uuid"
)

// Generator makes new ids.
type Generator interface {
	New() uuid.UUID
}

// Random makes time-ordered UUIDv7s, which keep B-tree inserts local.
type Random struct{}

// New returns a UUIDv7, falling back to v4 if the clock read fails.
func (Random) New() uuid.UUID {
	if id, err := uuid.NewV7(); err == nil {
		return id
	}
	return uuid.New()
}

// Seeded makes v4-shaped UUIDs from a seeded RNG. Safe for concurrent use.
type Seeded struct {
	mu sync.Mutex
	r  *rand.Rand
}

// NewSeeded returns a deterministic generator.
func NewSeeded(seed uint64) *Seeded {
	return &Seeded{r: rand.New(rand.NewPCG(seed, 0x6b6f7368))}
}

// New returns the next id in the sequence.
func (s *Seeded) New() uuid.UUID {
	s.mu.Lock()
	defer s.mu.Unlock()
	var id uuid.UUID
	for i := 0; i < 16; i += 8 {
		v := s.r.Uint64()
		for j := 0; j < 8; j++ {
			id[i+j] = byte(v >> (8 * j))
		}
	}
	id[6] = (id[6] & 0x0f) | 0x40 // version 4
	id[8] = (id[8] & 0x3f) | 0x80 // RFC 4122 variant
	return id
}
