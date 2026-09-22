package sim_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/naman777/kosh/internal/testdb"
	"github.com/naman777/kosh/sim"
)

// raceEnabled is set by race_test.go under -race, where seeds run ~10× slower.
var raceEnabled bool

func seedCount() int {
	if n, err := strconv.Atoi(os.Getenv("KOSH_SIM_SEEDS")); err == nil && n > 0 {
		return n
	}
	if testing.Short() || raceEnabled {
		return 4
	}
	return 20
}

// TestSeeds runs seeds 1..N (N = KOSH_SIM_SEEDS, default 20; CI uses 200).
// Each seed gets its own database and runs in parallel with the others.
func TestSeeds(t *testing.T) {
	for seed := uint64(1); seed <= uint64(seedCount()); seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			t.Parallel()
			rep := sim.Run(context.Background(), testdb.New(t), sim.Config{Seed: seed})
			if rep.Failed() {
				f := rep.Failure
				t.Fatalf("seed %d failed at step %d (%s): %s\nlast steps:\n  %s\nreplay: %s",
					seed, f.Step, f.Action, f.Err, strings.Join(f.Tail, "\n  "), rep.Replay())
			}
			t.Log(rep.Summary())
		})
	}
}

// TestReplayIsDeterministic: the same seed twice gives the same trace.
func TestReplayIsDeterministic(t *testing.T) {
	const seed = 4242
	a := sim.Run(context.Background(), testdb.New(t), sim.Config{Seed: seed, Verbose: true})
	b := sim.Run(context.Background(), testdb.New(t), sim.Config{Seed: seed, Verbose: true})
	if a.Failed() || b.Failed() {
		t.Fatalf("seed failed: %+v %+v", a.Failure, b.Failure)
	}
	if a.TraceHash != b.TraceHash {
		for i := range min(len(a.Trace), len(b.Trace)) {
			if a.Trace[i] != b.Trace[i] {
				t.Fatalf("runs diverge at line %d:\n  %s\n  %s", i, a.Trace[i], b.Trace[i])
			}
		}
		t.Fatalf("trace hashes differ (%d vs %d lines)", len(a.Trace), len(b.Trace))
	}
	t.Logf("seed %d replays identically: %s (%d trace lines)", seed, a.TraceHash, len(a.Trace))
}
