// Package bus abstracts the event log between the relay and the dispatcher.
//
// Production uses Kafka (Redpanda). Tests and the simulation harness use Mem,
// which keeps Kafka's semantics that matter for correctness: an append-only
// log, consumer groups with committed offsets, and redelivery of everything
// after the last commit when a consumer restarts.
package bus

import (
	"context"
	"sync"
)

// Message is one event on the log.
type Message struct {
	Key     []byte // aggregate id, so one payment's events stay in order
	Value   []byte // outbox.Envelope JSON
	EventID int64
}

// Record is a consumed message plus whatever the consumer needs to commit it.
type Record struct {
	Message
	offset int64
	raw    any
}

// Publisher appends messages and returns once every one is durably stored.
type Publisher interface {
	Publish(ctx context.Context, msgs []Message) error
}

// Consumer reads a consumer group's messages.
type Consumer interface {
	// Poll blocks until at least one record is available or ctx ends.
	Poll(ctx context.Context) ([]Record, error)
	// Commit marks records (and everything before them) as processed.
	Commit(ctx context.Context, recs []Record) error
	Close()
}

// Mem is an in-memory, single-partition log.
type Mem struct {
	mu        sync.Mutex
	log       []Message
	committed map[string]int64
	wake      chan struct{}
}

// NewMem returns an empty log.
func NewMem() *Mem { return &Mem{committed: map[string]int64{}, wake: make(chan struct{})} }

// Publish implements Publisher.
func (m *Mem) Publish(_ context.Context, msgs []Message) error {
	m.mu.Lock()
	m.log = append(m.log, msgs...)
	close(m.wake)
	m.wake = make(chan struct{})
	m.mu.Unlock()
	return nil
}

// Len returns how many messages have been published, duplicates included.
func (m *Mem) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.log)
}

// Consumer joins group, starting from its last committed offset, the way a
// restarted Kafka consumer does.
func (m *Mem) Consumer(group string) Consumer {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &memConsumer{m: m, group: group, pos: m.committed[group]}
}

type memConsumer struct {
	m     *Mem
	group string
	pos   int64
}

func (c *memConsumer) Poll(ctx context.Context) ([]Record, error) {
	for {
		c.m.mu.Lock()
		if c.pos < int64(len(c.m.log)) {
			var out []Record
			for ; c.pos < int64(len(c.m.log)) && len(out) < 100; c.pos++ {
				out = append(out, Record{Message: c.m.log[c.pos], offset: c.pos})
			}
			c.m.mu.Unlock()
			return out, nil
		}
		wake := c.m.wake
		c.m.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (c *memConsumer) Commit(_ context.Context, recs []Record) error {
	if len(recs) == 0 {
		return nil
	}
	c.m.mu.Lock()
	defer c.m.mu.Unlock()
	if next := recs[len(recs)-1].offset + 1; next > c.m.committed[c.group] {
		c.m.committed[c.group] = next
	}
	return nil
}

func (c *memConsumer) Close() {}
