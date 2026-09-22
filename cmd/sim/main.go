// Command sim runs Kosh's deterministic simulation harness.
//
//	go run ./cmd/sim -seed 42 -v            # replay one seed, printing its trace
//	go run ./cmd/sim -seeds 10000 -parallel 8
//
// Each seed gets a fresh database cloned from the migrated template
// (KOSH_TEST_DATABASE_URL, default: the compose Postgres). A failing seed
// prints the step that broke, the last steps before it, and the command to
// replay it.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/naman777/kosh/internal/testdb"
	"github.com/naman777/kosh/sim"
)

func main() {
	seed := flag.Uint64("seed", 0, "run just this seed")
	seeds := flag.Int("seeds", 100, "how many seeds to run")
	first := flag.Uint64("start", 1, "first seed")
	parallel := flag.Int("parallel", 8, "seeds to run at once")
	steps := flag.Int("steps", 60, "steps per seed")
	verbose := flag.Bool("v", false, "print the full trace (with -seed)")
	flag.Parse()

	ctx := context.Background()
	runOne := func(s uint64, verbose bool) (sim.Report, error) {
		db, err := testdb.Clone(ctx, 6)
		if err != nil {
			return sim.Report{}, err
		}
		defer db.Drop()
		return sim.Run(ctx, db.Pool, sim.Config{Seed: s, Steps: *steps, Verbose: verbose}), nil
	}

	if *seed != 0 {
		rep, err := runOne(*seed, *verbose)
		if err != nil {
			fmt.Fprintln(os.Stderr, "sim:", err)
			os.Exit(2)
		}
		for _, l := range rep.Trace {
			fmt.Println(l)
		}
		fmt.Println(rep.Summary())
		if rep.Failed() {
			printFailure(rep)
			os.Exit(1)
		}
		return
	}

	var (
		next     = *first
		mu       sync.Mutex
		done     atomic.Int64
		failures []sim.Report
		totals   = map[string]int{}
		reached  = map[string]int{}
		wg       sync.WaitGroup
		began    = time.Now()
		last     = *first + uint64(*seeds)
	)
	for w := 0; w < *parallel; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mu.Lock()
				s := next
				next++
				mu.Unlock()
				if s >= last {
					return
				}
				rep, err := runOne(s, false)
				if err != nil {
					fmt.Fprintf(os.Stderr, "seed %d: setup: %v\n", s, err)
					rep = sim.Report{Seed: s, Failure: &sim.Failure{Err: "setup: " + err.Error()}}
				}
				mu.Lock()
				for k, v := range rep.Counts {
					totals[k] += v
					reached[k]++
				}
				if rep.Failed() {
					failures = append(failures, rep)
				}
				mu.Unlock()
				if rep.Failed() {
					printFailure(rep)
				}
				if n := done.Add(1); n%100 == 0 || n == int64(*seeds) {
					el := time.Since(began)
					fmt.Printf("%d/%d seeds, %d failed, %s elapsed, %.1f seeds/s\n",
						n, *seeds, len(failures), el.Round(time.Second), float64(n)/el.Seconds())
				}
			}
		}()
	}
	wg.Wait()

	keys := make([]string, 0, len(totals))
	for k := range totals {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Println("\ncoverage (total, seeds that reached it):")
	for _, k := range keys {
		fmt.Printf("  %-36s %7d  %5d seeds\n", k, totals[k], reached[k])
	}
	fmt.Printf("\n%d seeds (%d–%d), %d steps each, in %s: ", *seeds, *first, last-1, *steps, time.Since(began).Round(time.Second))
	if len(failures) == 0 {
		fmt.Println("all passed, every invariant held")
		return
	}
	fmt.Printf("%d FAILED\n", len(failures))
	for _, f := range failures {
		fmt.Printf("  seed %d: %s → %s\n", f.Seed, oneLine(f.Failure.Err), f.Replay())
	}
	os.Exit(1)
}

func printFailure(rep sim.Report) {
	f := rep.Failure
	fmt.Printf("\nseed %d FAILED at step %d (%s): %s\n", rep.Seed, f.Step, f.Action, f.Err)
	for _, l := range f.Tail {
		fmt.Println("   ", l)
	}
	fmt.Println("  replay:", rep.Replay())
}

func oneLine(s string) string {
	s, _, _ = strings.Cut(s, "\n")
	if len(s) > 120 {
		s = s[:120] + "…"
	}
	return s
}
