package ledger

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/google/uuid"
)

// DefaultShards is how many shards each hot system account gets in a new
// database.
const DefaultShards = 16

// ShardedCodes are the system accounts every payment touches. Each is split
// into interchangeable shards so payments stop queueing on one row lock
// (ADR 0003). Reports add the shards up (GetSystemAccount).
var ShardedCodes = map[string]bool{
	BankReceivable:      true,
	UnallocatedReceipts: true,
	FeeRevenue:          true,
	GSTPayable:          true,
}

// Chart maps account codes to their account ids, shard by shard.
type Chart struct {
	ids map[string][]AccountID
}

// One returns an unsharded system account.
func (c *Chart) One(code string) AccountID { return c.ids[code][0] }

// For returns the shard of code that key (a payment id) maps to. Every
// entry for one payment uses the same shard, so authorization, capture,
// refunds and reconciliation all agree on where its money is.
func (c *Chart) For(code string, key uuid.UUID) AccountID {
	ids := c.ids[code]
	if len(ids) == 1 {
		return ids[0]
	}
	h := fnv.New32a()
	h.Write(key[:])
	return ids[h.Sum32()%uint32(len(ids))]
}

// Shards is the shard count of the sharded accounts.
func (c *Chart) Shards() int { return len(c.ids[BankReceivable]) }

// EnsureChart creates the system accounts and returns the chart. The shard
// count is fixed when a database is first set up (want, or DefaultShards if
// want ≤ 0) and read back on every later start: changing it would move
// existing payments to different shards.
func (l *Ledger) EnsureChart(ctx context.Context, q Querier, want int) (*Chart, error) {
	var n int
	if err := q.QueryRow(ctx, `SELECT count(*) FROM accounts WHERE code = $1 AND owner_id IS NULL`, BankReceivable).Scan(&n); err != nil {
		return nil, err
	}
	if n == 0 {
		n = want
		if n <= 0 {
			n = DefaultShards
		}
	}
	c := &Chart{ids: map[string][]AccountID{}}
	for _, spec := range SystemAccounts {
		shards := 1
		if ShardedCodes[spec.Code] {
			shards = n
		}
		for i := 0; i < shards; i++ {
			s := spec
			s.Shard = i
			id, err := l.EnsureAccount(ctx, q, s)
			if err != nil {
				return nil, err
			}
			c.ids[spec.Code] = append(c.ids[spec.Code], id)
		}
	}
	return c, nil
}

// GetSystemAccount returns a system account with its shards added up. The
// returned ID is shard 0's.
func (l *Ledger) GetSystemAccount(ctx context.Context, q Querier, code string) (Account, error) {
	var (
		a      Account
		normal string
		shards int
	)
	err := q.QueryRow(ctx, `
		SELECT min(id), code, normal_side, bit_or(flags),
		       sum(posted_dr), sum(posted_cr), sum(pending_dr), sum(pending_cr), count(*)
		FROM accounts WHERE code = $1 AND owner_id IS NULL
		GROUP BY code, normal_side`, code).Scan(
		&a.ID, &a.Code, &normal, &a.Flags, &a.PostedDr, &a.PostedCr, &a.PendingDr, &a.PendingCr, &shards)
	if err != nil {
		return Account{}, fmt.Errorf("ledger: system account %s: %w", code, err)
	}
	a.Normal = NormalSide(normal)
	return a, nil
}
