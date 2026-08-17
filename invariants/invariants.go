// Package invariants checks Kosh's correctness rules against a live database.
//
// Each rule is one SQL file named NN_name.sql. The first comment line is the
// rule in plain English; the query returns one row per violation, so an
// empty result means the rule holds. Tests, the simulation harness and CI
// all call Check.
package invariants

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

//go:embed *.sql
var files embed.FS

// Invariant is one rule.
type Invariant struct {
	ID          int
	Name        string // from the file name, e.g. "entries_balance"
	Description string // the file's first comment line
	SQL         string
}

// Result is the outcome of one invariant.
type Result struct {
	Invariant
	Columns    []string
	Violations [][]any
}

// OK reports whether the invariant held.
func (r Result) OK() bool { return len(r.Violations) == 0 }

// Report is the outcome of a Check.
type Report struct {
	Results []Result
}

// OK reports whether every invariant held.
func (r Report) OK() bool {
	for _, res := range r.Results {
		if !res.OK() {
			return false
		}
	}
	return true
}

// Err returns nil if every invariant held, or an error listing violations.
func (r Report) Err() error {
	if r.OK() {
		return nil
	}
	return fmt.Errorf("invariant violations:\n%s", r.String())
}

// String renders failures (up to 10 rows each), or a one-line pass summary.
func (r Report) String() string {
	var b strings.Builder
	failed := 0
	for _, res := range r.Results {
		if res.OK() {
			continue
		}
		failed++
		fmt.Fprintf(&b, "  #%d %s — %s (%d violations)\n", res.ID, res.Name, res.Description, len(res.Violations))
		for i, row := range res.Violations {
			if i == 10 {
				fmt.Fprintf(&b, "      … %d more\n", len(res.Violations)-10)
				break
			}
			parts := make([]string, len(row))
			for j, v := range row {
				parts[j] = fmt.Sprintf("%s=%v", res.Columns[j], v)
			}
			fmt.Fprintf(&b, "      %s\n", strings.Join(parts, " "))
		}
	}
	if failed == 0 {
		return fmt.Sprintf("all %d invariants hold", len(r.Results))
	}
	return b.String()
}

// All returns every invariant, ordered by id.
func All() ([]Invariant, error) {
	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		return nil, err
	}
	var out []Invariant
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}
		num, rest, ok := strings.Cut(strings.TrimSuffix(name, ".sql"), "_")
		if !ok {
			return nil, fmt.Errorf("invariant file %q: want NN_name.sql", name)
		}
		id, err := strconv.Atoi(num)
		if err != nil {
			return nil, fmt.Errorf("invariant file %q: %w", name, err)
		}
		body, err := fs.ReadFile(files, name)
		if err != nil {
			return nil, err
		}
		sql := string(body)
		desc, _, _ := strings.Cut(sql, "\n")
		out = append(out, Invariant{
			ID:          id,
			Name:        rest,
			Description: strings.TrimSpace(strings.TrimPrefix(desc, "--")),
			SQL:         sql,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// Querier is satisfied by *pgxpool.Pool, *pgx.Conn and pgx.Tx.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Check runs the given invariants (all of them when ids is empty).
func Check(ctx context.Context, q Querier, ids ...int) (Report, error) {
	all, err := All()
	if err != nil {
		return Report{}, err
	}
	want := map[int]bool{}
	for _, id := range ids {
		want[id] = true
	}
	var rep Report
	for _, inv := range all {
		if len(want) > 0 && !want[inv.ID] {
			continue
		}
		res, err := run(ctx, q, inv)
		if err != nil {
			return Report{}, fmt.Errorf("invariant #%d %s: %w", inv.ID, inv.Name, err)
		}
		rep.Results = append(rep.Results, res)
	}
	return rep, nil
}

func run(ctx context.Context, q Querier, inv Invariant) (Result, error) {
	rows, err := q.Query(ctx, inv.SQL)
	if err != nil {
		return Result{}, err
	}
	defer rows.Close()
	res := Result{Invariant: inv}
	for _, fd := range rows.FieldDescriptions() {
		res.Columns = append(res.Columns, fd.Name)
	}
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return Result{}, err
		}
		res.Violations = append(res.Violations, vals)
	}
	return res, rows.Err()
}
