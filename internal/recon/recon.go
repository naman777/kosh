// Package recon reconciles the bank's settlement files against the ledger.
//
// Every line of a file is matched to a bank-confirmed payment or refund
// (by bank reference, then by Kosh's own id) and classified:
//
//	matched            amounts and fee agree
//	fee_variance       gross agrees, the bank's fee differs from its schedule
//	amount_mismatch    gross differs
//	duplicate          the item was already settled
//	missing_in_ledger  no ledger item for the line
//	missing_in_bank    (no line) an item confirmed before the file's date never showed up
//
// Reconciling a file posts one ledger entry: cash into bank_settlement, the
// receivable cleared at what Kosh expected, fees at the bank's published
// schedule, and every difference into recon_suspense. A non-matched result
// is an exception; resolving it moves its difference out of suspense with
// an adjusting entry. Nothing is ever edited.
package recon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/naman777/kosh/internal/bank"
	"github.com/naman777/kosh/internal/banksim"
	"github.com/naman777/kosh/internal/clock"
	"github.com/naman777/kosh/internal/ids"
	"github.com/naman777/kosh/internal/ledger"
	"github.com/naman777/kosh/internal/money"
)

// Statuses.
const (
	Matched         = "matched"
	FeeVariance     = "fee_variance"
	AmountMismatch  = "amount_mismatch"
	Duplicate       = "duplicate"
	MissingInLedger = "missing_in_ledger"
	MissingInBank   = "missing_in_bank"
)

// Resolutions.
const (
	WriteOff      = "write_off"      // accept the difference as a loss (or gain)
	BankCorrected = "bank_corrected" // the bank paid or reclaimed the difference
	MatchedLate   = "matched_late"   // a missing item turned up in a later file
)

var (
	// ErrFileConflict means a different file was already ingested for the date.
	ErrFileConflict = errors.New("recon: a different settlement file already exists for this date")
	// ErrNotOpen means the result is matched or already resolved.
	ErrNotOpen = errors.New("recon: not an open exception")
	// ErrBadResolution means the action doesn't apply to this exception.
	ErrBadResolution = errors.New("recon: resolution does not apply to this exception")
)

// IST is the zone business dates are cut in.
var IST = banksim.IST

// Engine reconciles. It is safe for concurrent use; a file is reconciled
// under a row lock, once.
type Engine struct {
	pool   *pgxpool.Pool
	clock  clock.Clock
	ids    ids.Generator
	ledger *ledger.Ledger
	sys    map[string]ledger.AccountID
}

// New builds an Engine and makes sure the system accounts exist.
func New(ctx context.Context, pool *pgxpool.Pool, c clock.Clock, g ids.Generator) (*Engine, error) {
	l := ledger.New(c)
	sys, err := l.EnsureSystemAccounts(ctx, pool)
	if err != nil {
		return nil, err
	}
	return &Engine{pool: pool, clock: c, ids: g, ledger: l, sys: sys}, nil
}

// Ingest parses and stores the settlement file for business date
// (YYYY-MM-DD). The same file twice is a no-op; a different file for a date
// already ingested is refused.
func (e *Engine) Ingest(ctx context.Context, date string, raw []byte) (uuid.UUID, error) {
	day, err := time.ParseInLocation("2006-01-02", date, IST)
	if err != nil {
		return uuid.Nil, fmt.Errorf("recon: bad date %q", date)
	}
	lines, err := banksim.ParseCSV(bytes.NewReader(raw))
	if err != nil {
		return uuid.Nil, err
	}
	sum := sha256.Sum256(raw)

	var id uuid.UUID
	err = pgx.BeginFunc(ctx, e.pool, func(tx pgx.Tx) error {
		var existing []byte
		err := tx.QueryRow(ctx, `SELECT id, checksum FROM settlement_files WHERE business_date = $1::date`, date).Scan(&id, &existing)
		if err == nil {
			if !bytes.Equal(existing, sum[:]) {
				return ErrFileConflict
			}
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		id = e.ids.New()
		if _, err := tx.Exec(ctx, `
			INSERT INTO settlement_files (id, business_date, value_date, checksum, line_count, ingested_at)
			VALUES ($1, $2::date, $3::date, $4, $5, $6)`,
			id, date, day.AddDate(0, 0, 1).Format("2006-01-02"), sum[:], len(lines), e.clock.Now()); err != nil {
			return err
		}
		rows := make([][]any, len(lines))
		for i, l := range lines {
			rows[i] = []any{id, l.LineNo, l.BankRef, l.Reference, int64(l.Gross), int64(l.Fee), int64(l.Tax), int64(l.Net), l.ValueDate}
		}
		_, err = tx.CopyFrom(ctx, pgx.Identifier{"settlement_lines"},
			[]string{"file_id", "line_no", "bank_ref", "reference", "gross", "fee", "tax", "net", "value_date"},
			pgx.CopyFromRows(rows))
		return err
	})
	return id, err
}

// item is a bank-confirmed payment or refund. Refunds carry negative gross.
type item struct {
	kind        string
	id          uuid.UUID
	bankRef     string
	gross       money.Paise
	expectedFee money.Paise
	settled     bool
	writtenOff  bool // reported missing earlier and written off
	missingOpen bool // reported missing earlier, still open
}

// result is one recon_results row to insert.
type result struct {
	lineNo   *int
	bankRef  string
	it       *item
	status   string
	expGross *money.Paise
	actGross *money.Paise
	expFee   *money.Paise
	actFee   *money.Paise
	suspense money.Paise
}

// Report summarizes a reconciled file.
type Report struct {
	FileID   uuid.UUID      `json:"file_id"`
	Date     string         `json:"business_date"`
	Lines    int            `json:"lines"`
	ByStatus map[string]int `json:"by_status"`
	Cash     money.Paise    `json:"cash"` // net cash the file brought in
}

// Reconcile matches a file's lines against the ledger and posts the
// settlement entry. Reconciling a file twice returns the first report.
func (e *Engine) Reconcile(ctx context.Context, fileID uuid.UUID) (Report, error) {
	rep := Report{FileID: fileID, ByStatus: map[string]int{}}
	err := pgx.BeginFunc(ctx, e.pool, func(tx pgx.Tx) error {
		var (
			date       time.Time
			reconciled *time.Time
		)
		if err := tx.QueryRow(ctx, `SELECT business_date, reconciled_at FROM settlement_files WHERE id = $1 FOR UPDATE`,
			fileID).Scan(&date, &reconciled); err != nil {
			return err
		}
		rep.Date = date.Format("2006-01-02")
		if reconciled != nil {
			return e.loadReport(ctx, tx, &rep)
		}
		return e.reconcile(ctx, tx, fileID, rep.Date, &rep)
	})
	return rep, err
}

func (e *Engine) reconcile(ctx context.Context, tx pgx.Tx, fileID uuid.UUID, date string, rep *Report) error {
	now := e.clock.Now()
	lines, err := loadLines(ctx, tx, fileID)
	if err != nil {
		return err
	}
	var refs, references []string
	for _, l := range lines {
		refs = append(refs, l.BankRef)
		references = append(references, l.Reference)
	}
	byRef, byID, err := e.loadItems(ctx, tx, `(x.bank_ref = ANY($1) OR x.id::text = ANY($2))`, refs, references)
	if err != nil {
		return err
	}

	deltas := map[ledger.AccountID]money.Paise{}
	add := func(code string, v money.Paise) { deltas[e.sys[code]] += v }
	var results []result
	for i := range lines {
		l := &lines[i]
		lineNo := l.LineNo
		actFee := l.Fee + l.Tax
		r := result{lineNo: &lineNo, bankRef: l.BankRef, actGross: &l.Gross, actFee: &actFee}
		it := byRef[l.BankRef]
		if it == nil {
			if id, err := uuid.Parse(l.Reference); err == nil {
				it = byID[id]
			}
		}
		add(ledger.BankSettlement, l.Net)
		switch {
		case it == nil:
			r.status, r.suspense = MissingInLedger, -l.Net
		case it.settled:
			r.it, r.status, r.suspense = it, Duplicate, -l.Net
		default:
			r.it, r.expGross, r.expFee = it, &it.gross, &it.expectedFee
			r.suspense = it.gross - it.expectedFee - l.Net
			switch {
			case l.Gross != it.gross:
				r.status = AmountMismatch
			case actFee != it.expectedFee:
				r.status = FeeVariance
			default:
				r.status = Matched
			}
			it.settled = true
			// Clear what Kosh expected the bank to owe. If the item had
			// been written off as missing, the cash is a recovery instead.
			if it.writtenOff {
				add(ledger.ReconWriteoff, -it.gross)
			} else {
				add(ledger.BankReceivable, -it.gross)
			}
			add(ledger.BankCharges, it.expectedFee)
		}
		add(ledger.ReconSuspense, r.suspense)
		results = append(results, r)
	}
	if err := e.insertResults(ctx, tx, fileID, results, now); err != nil {
		return err
	}
	// Missing items that just turned up close their exceptions.
	for _, it := range byRef {
		if it.settled && it.missingOpen {
			if _, err := tx.Exec(ctx, `
				UPDATE recon_results SET resolution = 'matched_late', resolved_at = $3
				WHERE item_kind = $1 AND item_id = $2 AND status = 'missing_in_bank' AND resolved_at IS NULL`,
				it.kind, it.id, now); err != nil {
				return err
			}
		}
	}

	// Anything the bank confirmed before this file's day and still hasn't
	// settled is missing_in_bank: by T+2 it is either matched or here.
	cutoff, _ := time.ParseInLocation("2006-01-02", date, IST)
	missing, err := e.loadMissing(ctx, tx, cutoff)
	if err != nil {
		return err
	}
	var missingResults []result
	for _, it := range missing {
		missingResults = append(missingResults, result{bankRef: it.bankRef, it: it, status: MissingInBank,
			expGross: &it.gross, expFee: &it.expectedFee})
	}
	if err := e.insertResults(ctx, tx, fileID, missingResults, now); err != nil {
		return err
	}

	var entry *uuid.UUID
	if ps := signedPostings(deltas); len(ps) >= 2 {
		id := e.ids.New()
		if err := e.ledger.Post(ctx, tx, ledger.Entry{ID: id, Kind: "bank_settlement", RefID: fileID, Postings: ps}); err != nil {
			return fmt.Errorf("recon: post settlement: %w", err)
		}
		entry = &id
	}
	if _, err := tx.Exec(ctx, `UPDATE settlement_files SET reconciled_at = $2, settlement_entry = $3 WHERE id = $1`,
		fileID, now, entry); err != nil {
		return err
	}
	rep.Lines = len(lines)
	rep.Cash = deltas[e.sys[ledger.BankSettlement]]
	for _, r := range append(results, missingResults...) {
		rep.ByStatus[r.status]++
	}
	return nil
}

func (e *Engine) loadReport(ctx context.Context, tx pgx.Tx, rep *Report) error {
	rows, err := tx.Query(ctx, `SELECT status, count(*) FROM recon_results WHERE file_id = $1 GROUP BY status`, rep.FileID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			s string
			n int
		)
		if err := rows.Scan(&s, &n); err != nil {
			return err
		}
		rep.ByStatus[s] = n
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return tx.QueryRow(ctx, `SELECT count(*), COALESCE(SUM(net), 0) FROM settlement_lines WHERE file_id = $1`,
		rep.FileID).Scan(&rep.Lines, &rep.Cash)
}

func loadLines(ctx context.Context, tx pgx.Tx, fileID uuid.UUID) ([]banksim.Line, error) {
	rows, err := tx.Query(ctx, `
		SELECT line_no, bank_ref, reference, gross, fee, tax, net
		FROM settlement_lines WHERE file_id = $1 ORDER BY line_no`, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []banksim.Line
	for rows.Next() {
		var l banksim.Line
		if err := rows.Scan(&l.LineNo, &l.BankRef, &l.Reference, &l.Gross, &l.Fee, &l.Tax, &l.Net); err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

// itemsSQL lists bank-confirmed payments and refunds with their settlement
// state. %s is a filter on x (id, bank_ref, confirmed).
const itemsSQL = `
WITH x AS (
  SELECT 'payment' AS kind, id, bank_ref, amount AS gross, method, bank_confirmed_at AS confirmed
  FROM payments WHERE bank_ref IS NOT NULL
  UNION ALL
  SELECT 'refund', id, bank_ref, -amount, '', bank_confirmed_at
  FROM refunds WHERE bank_ref IS NOT NULL
)
SELECT x.kind, x.id, x.bank_ref, x.gross, x.method,
       EXISTS (SELECT 1 FROM recon_results r WHERE r.item_kind = x.kind AND r.item_id = x.id
               AND r.status IN ('matched', 'fee_variance', 'amount_mismatch')) AS settled,
       EXISTS (SELECT 1 FROM recon_results r WHERE r.item_kind = x.kind AND r.item_id = x.id
               AND r.status = 'missing_in_bank' AND r.resolution = 'write_off') AS written_off,
       EXISTS (SELECT 1 FROM recon_results r WHERE r.item_kind = x.kind AND r.item_id = x.id
               AND r.status = 'missing_in_bank' AND r.resolved_at IS NULL) AS missing_open
FROM x WHERE %s
ORDER BY x.confirmed, x.id`

func (e *Engine) loadItems(ctx context.Context, tx pgx.Tx, filter string, args ...any) (map[string]*item, map[uuid.UUID]*item, error) {
	rows, err := tx.Query(ctx, fmt.Sprintf(itemsSQL, filter), args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	byRef, byID := map[string]*item{}, map[uuid.UUID]*item{}
	for rows.Next() {
		var (
			it     item
			method string
		)
		if err := rows.Scan(&it.kind, &it.id, &it.bankRef, &it.gross, &method, &it.settled, &it.writtenOff, &it.missingOpen); err != nil {
			return nil, nil, err
		}
		if it.kind == "payment" {
			fee, tax := bank.Fee(method, it.gross)
			it.expectedFee = fee + tax
		}
		p := &it
		byRef[it.bankRef], byID[it.id] = p, p
	}
	return byRef, byID, rows.Err()
}

func (e *Engine) loadMissing(ctx context.Context, tx pgx.Tx, cutoff time.Time) ([]*item, error) {
	_, byID, err := e.loadItems(ctx, tx, `x.confirmed < $1`, cutoff)
	if err != nil {
		return nil, err
	}
	var out []*item
	for _, it := range byID {
		if !it.settled && !it.writtenOff && !it.missingOpen {
			out = append(out, it)
		}
	}
	// Deterministic order for the simulation harness.
	sort.Slice(out, func(i, j int) bool { return out[i].id.String() < out[j].id.String() })
	return out, nil
}

func (e *Engine) insertResults(ctx context.Context, tx pgx.Tx, fileID uuid.UUID, rs []result, now time.Time) error {
	for _, r := range rs {
		var (
			kind *string
			id   *uuid.UUID
		)
		if r.it != nil {
			kind, id = &r.it.kind, &r.it.id
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO recon_results (file_id, line_no, bank_ref, item_kind, item_id, status,
			                           expected_gross, actual_gross, expected_fee, actual_fee, suspense, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
			fileID, r.lineNo, r.bankRef, kind, id, r.status, r.expGross, r.actGross, r.expFee, r.actFee, int64(r.suspense), now); err != nil {
			return fmt.Errorf("recon: insert %s result: %w", r.status, err)
		}
	}
	return nil
}

// signedPostings turns per-account signed debits into postings: positive
// debits, negative credits, zeros dropped, in account order.
func signedPostings(deltas map[ledger.AccountID]money.Paise) []ledger.Posting {
	var ps []ledger.Posting
	for acct, v := range deltas {
		switch {
		case v > 0:
			ps = append(ps, ledger.Posting{Account: acct, Side: ledger.Debit, Amount: v})
		case v < 0:
			ps = append(ps, ledger.Posting{Account: acct, Side: ledger.Credit, Amount: -v})
		}
	}
	sort.Slice(ps, func(i, j int) bool { return ps[i].Account < ps[j].Account })
	return ps
}
