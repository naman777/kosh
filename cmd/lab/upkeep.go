package main

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/naman777/kosh/internal/db"
)

// The live demo runs on a small shared VM, so the lab looks after itself:
//
//   - Traffic runs only while someone is watching. A dashboard polls
//     /lab/state every second; after idleAfter with no poll, traffic stops and
//     the invariant checks slow down. The next poll resumes both.
//   - Whatever a visitor breaks heals: a dead process restarts after
//     healAfter, and the bank, the merchant's endpoint and the traffic rate
//     return to their defaults after defaultsAfter.
//   - The lab's data is wiped every night at resetAt, so the database (and the
//     invariant queries, which scan whole tables) stay small.
type upkeep struct {
	idleAfter     time.Duration
	healAfter     time.Duration
	defaultsAfter time.Duration
	defaultRate   int
	resetAt       string // "HH:MM" in loc, or "" for never
	loc           *time.Location
}

// touch records a visitor. A paused lab resumes traffic and checks at once.
func (l *lab) touch() {
	l.mu.Lock()
	l.lastSeen = time.Now()
	resume := l.paused && !l.resetting
	if resume {
		l.paused = false
	}
	rate := l.wantRate
	l.mu.Unlock()
	if resume {
		l.traffic.setRate(rate)
		l.note("visitor arrived: traffic resumed")
		l.wakeChecks()
	}
}

// setTraffic is a visitor choosing a rate.
func (l *lab) setTraffic(rate int) {
	l.mu.Lock()
	l.wantRate, l.rateSince = rate, time.Now()
	paused := l.paused || l.resetting
	l.mu.Unlock()
	if !paused {
		l.traffic.setRate(rate)
	}
	l.note(fmt.Sprintf("traffic → %d sales/s", rate))
}

func (l *lab) wakeChecks() {
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

// checkInterval is how often the invariants run: often while someone is
// watching, rarely when no one is.
func (l *lab) checkInterval() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.paused {
		return 30 * time.Second
	}
	return 5 * time.Second
}

// upkeepLoop pauses an unwatched lab and heals a broken one.
func (l *lab) upkeepLoop(ctx context.Context) {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		l.pauseIfIdle()
		l.heal(ctx)
	}
}

func (l *lab) pauseIfIdle() {
	l.mu.Lock()
	pause := !l.paused && !l.resetting && time.Since(l.lastSeen) >= l.up.idleAfter
	if pause {
		l.paused = true
	}
	l.mu.Unlock()
	if pause {
		l.traffic.setRate(0)
		l.note(fmt.Sprintf("no one watching for %s: traffic paused", l.up.idleAfter))
	}
}

func (l *lab) heal(ctx context.Context) {
	l.mu.Lock()
	if l.resetting {
		l.mu.Unlock()
		return
	}
	bank := l.bankPreset != "healthy" && time.Since(l.bankSince) >= l.up.defaultsAfter
	mode := l.mode != "ok" && time.Since(l.modeSince) >= l.up.defaultsAfter
	rate := l.wantRate != l.up.defaultRate && time.Since(l.rateSince) >= l.up.defaultsAfter
	l.mu.Unlock()

	for _, name := range order {
		p := l.procs[name]
		if down, since := p.down(); down && !since.IsZero() && time.Since(since) >= l.up.healAfter {
			if err := p.start(l.binDir); err != nil {
				l.note(fmt.Sprintf("restarting %s failed: %v", name, err))
				continue
			}
			l.note(fmt.Sprintf("%s was down for %s: restarted it", name, l.up.healAfter))
		}
	}
	if bank {
		if err := l.setBank(ctx, "healthy"); err == nil {
			l.note(fmt.Sprintf("bank back to healthy after %s", l.up.defaultsAfter))
		}
	}
	if mode {
		if err := l.setMerchant(ctx, "ok"); err == nil {
			l.note(fmt.Sprintf("merchant endpoint back to ok after %s", l.up.defaultsAfter))
		}
	}
	if rate {
		l.setTraffic(l.up.defaultRate)
	}
}

// nextReset is the first resetAt ("HH:MM" in loc) after now.
func nextReset(now time.Time, at string, loc *time.Location) (time.Time, error) {
	t, err := time.Parse("15:04", at)
	if err != nil {
		return time.Time{}, fmt.Errorf("LAB_RESET_AT %q: want HH:MM", at)
	}
	local := now.In(loc)
	next := time.Date(local.Year(), local.Month(), local.Day(), t.Hour(), t.Minute(), 0, 0, loc)
	if !next.After(local) {
		next = next.AddDate(0, 0, 1)
	}
	return next, nil
}

func (l *lab) resetLoop(ctx context.Context) {
	if l.up.resetAt == "" {
		return
	}
	for {
		next, err := nextReset(time.Now(), l.up.resetAt, l.up.loc)
		if err != nil {
			l.log.Error("nightly reset disabled", "err", err)
			return
		}
		l.log.Info("nightly reset scheduled", "at", next)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Until(next)):
		}
		if err := l.reset(ctx); err != nil {
			l.note("nightly reset failed: " + err.Error())
		}
	}
}

// reset wipes the lab's data and starts over: stop traffic and every
// process, drop and recreate the lab's database (ledger tables are
// append-only, so it can't be emptied in place), create the merchant again,
// and start everything with defaults. If the wipe fails, the old data stays
// and the lab still comes back up.
func (l *lab) reset(ctx context.Context) error {
	l.mu.Lock()
	l.resetting = true
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		l.resetting = false
		l.mu.Unlock()
	}()
	l.note("nightly reset: clearing the lab's data")
	l.traffic.setRate(0)
	for i := len(order) - 1; i >= 0; i-- {
		l.procs[order[i]].kill()
	}
	for i := 0; i < 50 && l.anyRunning(); i++ {
		time.Sleep(100 * time.Millisecond)
	}

	err := l.recreate(ctx)
	for _, name := range order {
		if err := l.procs[name].start(l.binDir); err != nil {
			l.note(fmt.Sprintf("starting %s failed: %v", name, err))
		}
	}
	l.waitHealthy(ctx)
	if err := l.setBank(ctx, "healthy"); err != nil {
		l.note("bank preset after reset: " + err.Error())
	}
	l.mu.Lock()
	l.mode, l.wantRate = "ok", l.up.defaultRate
	paused := l.paused
	l.mu.Unlock()
	if !paused {
		l.traffic.setRate(l.up.defaultRate)
	}
	l.wakeChecks()
	if err != nil {
		return err
	}
	l.note("nightly reset done: fresh ledger, new merchant")
	return nil
}

func (l *lab) anyRunning() bool {
	for _, p := range l.procs {
		if down, _ := p.down(); !down {
			return true
		}
	}
	return false
}

// recreate swaps in a fresh, migrated database with a new merchant.
func (l *lab) recreate(ctx context.Context) error {
	if err := dropDatabase(ctx, l.dbURL); err != nil {
		return err
	}
	if err := ensureDatabase(ctx, l.dbURL); err != nil {
		return err
	}
	pool, err := db.Open(ctx, l.dbURL)
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
	old := l.pool.Swap(pool)
	go old.Close() // its connections were terminated by the drop
	l.procs["samplemerchant"].setEnv("SAMPLE_SECRET", c.WebhookSecret)
	l.traffic.restart(c.APIKey)
	return nil
}

// dropDatabase drops the database raw names, disconnecting anyone still on it.
func dropDatabase(ctx context.Context, raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	name := strings.TrimPrefix(u.Path, "/")
	u.Path = "/postgres"
	conn, err := pgx.Connect(ctx, u.String())
	if err != nil {
		return fmt.Errorf("lab: connect to drop %s: %w", name, err)
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, `DROP DATABASE IF EXISTS `+pgx.Identifier{name}.Sanitize()+` WITH (FORCE)`)
	return err
}
