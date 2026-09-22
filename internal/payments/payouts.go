package payments

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/naman777/kosh/internal/bank"
	"github.com/naman777/kosh/internal/crash"
	"github.com/naman777/kosh/internal/ledger"
	"github.com/naman777/kosh/internal/money"
	"github.com/naman777/kosh/internal/outbox"
)

// IST is the zone settlement days are cut in.
var IST = time.FixedZone("IST", 5*3600+30*60)

// MinPayout is the smallest amount worth a bank transfer.
const MinPayout = money.Paise(100)

// Payout states.
const (
	PayoutPending   = "pending"
	PayoutProcessed = "processed"
	PayoutFailed    = "failed"
)

// Payout is a payouts row.
type Payout struct {
	ID             uuid.UUID
	MerchantID     uuid.UUID
	SettlementDate time.Time
	Amount         money.Paise
	State          string
	HoldEntryID    uuid.UUID
	BankRef        *string
	FailureReason  *string
	Version        int
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

const payoutCols = `id, merchant_id, settlement_date, amount, state, hold_entry_id, bank_ref, failure_reason, version, created_at, updated_at`

func scanPayout(row pgx.Row) (Payout, error) {
	var p Payout
	err := row.Scan(&p.ID, &p.MerchantID, &p.SettlementDate, &p.Amount, &p.State, &p.HoldEntryID,
		&p.BankRef, &p.FailureReason, &p.Version, &p.CreatedAt, &p.UpdatedAt)
	return p, err
}

// PayoutView is the API representation of a payout.
type PayoutView struct {
	ID             uuid.UUID `json:"id"`
	Entity         string    `json:"entity"`
	SettlementDate string    `json:"settlement_date"`
	Amount         int64     `json:"amount"`
	Currency       string    `json:"currency"`
	State          string    `json:"status"`
	BankRef        *string   `json:"bank_ref"`
	FailureReason  *string   `json:"failure_reason"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// View renders the payout.
func (p Payout) View() PayoutView {
	return PayoutView{ID: p.ID, Entity: "payout", SettlementDate: p.SettlementDate.Format("2006-01-02"),
		Amount: int64(p.Amount), Currency: "INR", State: p.State, BankRef: p.BankRef, FailureReason: p.FailureReason,
		CreatedAt: p.CreatedAt.UTC(), UpdatedAt: p.UpdatedAt.UTC()}
}

func (s *Service) emitPayout(ctx context.Context, tx pgx.Tx, p Payout) error {
	typ := "payout." + p.State
	if p.Version == 1 {
		typ = "payout.created"
	}
	_, err := outbox.Write(ctx, tx, s.clock.Now(), outbox.Event{
		AggregateType: "payout", AggregateID: p.ID, Version: p.Version, MerchantID: p.MerchantID,
		Type: typ, Payload: p.View(),
	})
	return err
}

// SettlementReport summarizes one batch run.
type SettlementReport struct {
	Date      string      `json:"settlement_date"`
	Merchants int         `json:"merchants_considered"`
	Payouts   int         `json:"payouts_created"`
	Amount    money.Paise `json:"amount"`
}

// ErrTooEarly means the settlement date's day has not ended yet in IST.
var ErrTooEarly = errors.New("payments: settlement date has not ended yet")

// RunSettlement pays every merchant what it earned up to the end of date
// (IST), on T+1. It is safe to run any number of times: there is at most one
// payout per merchant per date. A merchant skipped one day (a failed or
// missed run) is paid the backlog the next day, because eligibility is
// "everything available except today's credits".
func (s *Service) RunSettlement(ctx context.Context, date time.Time) (SettlementReport, error) {
	day := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, IST)
	cutoff := day.AddDate(0, 0, 1)
	rep := SettlementReport{Date: day.Format("2006-01-02")}
	if s.clock.Now().Before(cutoff) {
		return rep, ErrTooEarly
	}

	// Dates go to Postgres as text: a time.Time would be compared as a UTC
	// timestamp, and IST midnight is 18:30 the previous day in UTC.
	rows, err := s.pool.Query(ctx, `
		SELECT m.id FROM merchants m
		WHERE NOT EXISTS (SELECT 1 FROM payouts p WHERE p.merchant_id = m.id AND p.settlement_date = $1::date)
		ORDER BY m.id`, rep.Date)
	if err != nil {
		return rep, err
	}
	var merchants []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return rep, err
		}
		merchants = append(merchants, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return rep, err
	}

	for _, m := range merchants {
		rep.Merchants++
		p, ok, err := s.createPayout(ctx, m, rep.Date, cutoff)
		if err != nil {
			return rep, err
		}
		if !ok {
			continue
		}
		rep.Payouts++
		rep.Amount += p.Amount
		crash.At(ctx, "payouts.after_hold")
		if err := s.submitPayout(ctx, p); err != nil {
			return rep, err
		}
	}
	return rep, nil
}

// createPayout is phase A: compute what is eligible, hold it, and record
// the payout, in one transaction.
func (s *Service) createPayout(ctx context.Context, merchantID uuid.UUID, day string, cutoff time.Time) (Payout, bool, error) {
	var (
		p  Payout
		ok bool
	)
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		_, payable, err := s.merchantPayable(ctx, tx, merchantID)
		if err != nil {
			return err
		}
		// Lock the payable, then compute in a *separate* statement. Under READ
		// COMMITTED a SELECT … FOR UPDATE that waits on a lock re-reads the
		// row's new balance but evaluates its subqueries against its old
		// snapshot, so a capture committing meanwhile would be counted in the
		// balance and missed by "credits since cutoff". Once we hold the lock
		// no posting to this account is in flight, and the next statement's
		// fresh snapshot sees everything. (Caught by TestBankChaos.)
		if _, err := tx.Exec(ctx, `SELECT 1 FROM accounts WHERE id = $1 FOR UPDATE`, payable); err != nil {
			return err
		}
		var eligible money.Paise
		if err := tx.QueryRow(ctx, `
			SELECT a.posted_cr - a.posted_dr - a.pending_dr
			     - COALESCE((SELECT SUM(ps.amount) FROM postings ps
			                 JOIN journal_entries e ON e.id = ps.entry_id
			                 WHERE ps.account_id = a.id AND ps.side = 'cr'
			                   AND e.state = 'posted' AND e.created_at >= $2), 0)
			FROM accounts a WHERE a.id = $1`, payable, cutoff).Scan(&eligible); err != nil {
			return err
		}
		if eligible < MinPayout {
			return nil
		}
		now := s.clock.Now()
		id, holdID := s.ids.New(), s.ids.New()
		if err := s.ledger.Hold(ctx, tx, ledger.Entry{ID: holdID, Kind: "payout", RefID: id, Postings: []ledger.Posting{
			{Account: payable, Side: ledger.Debit, Amount: eligible},
			{Account: s.sys[ledger.PayoutClearing], Side: ledger.Credit, Amount: eligible},
		}}, now.Add(s.cfg.PayoutHoldTTL)); err != nil {
			return err
		}
		p, err = scanPayout(tx.QueryRow(ctx, `
			INSERT INTO payouts (id, merchant_id, settlement_date, amount, state, hold_entry_id, created_at, updated_at, next_poll_at)
			VALUES ($1, $2, $3::date, $4, 'pending', $5, $6, $6, $7)
			ON CONFLICT (merchant_id, settlement_date) DO NOTHING
			RETURNING `+payoutCols, id, merchantID, day, eligible, holdID, now, now.Add(s.cfg.CreatedGrace)))
		if errors.Is(err, pgx.ErrNoRows) {
			// A concurrent run got here first. Roll back our hold with it.
			return errPayoutExists
		}
		if err != nil {
			return err
		}
		ok = true
		return s.emitPayout(ctx, tx, p)
	})
	if errors.Is(err, errPayoutExists) {
		return Payout{}, false, nil
	}
	return p, ok, err
}

var errPayoutExists = errors.New("payout exists")

// submitPayout is phases B and C: ask the bank, then apply the answer. An
// unknown outcome leaves the payout pending for the poller.
func (s *Service) submitPayout(ctx context.Context, p Payout) error {
	bctx, cancel := context.WithTimeout(ctx, s.cfg.BankTimeout)
	res, err := s.bank.Payout(bctx, bank.PayoutRequest{PayoutID: p.ID, Amount: p.Amount, Beneficiary: p.MerchantID})
	cancel()
	if err != nil {
		return nil
	}
	crash.At(ctx, "payouts.after_bank")
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		_, err := s.resolvePayout(ctx, tx, p.ID, res)
		return err
	})
}

// resolvePayout applies the bank's answer to a pending payout. Success
// commits the hold and moves the money out of Kosh's bank account; failure
// voids the hold, so tomorrow's batch picks the money up again. Resolved
// payouts are returned unchanged.
func (s *Service) resolvePayout(ctx context.Context, tx pgx.Tx, id uuid.UUID, res bank.Result) (Payout, error) {
	p, err := scanPayout(tx.QueryRow(ctx, `SELECT `+payoutCols+` FROM payouts WHERE id = $1 FOR UPDATE`, id))
	if err != nil {
		return Payout{}, err
	}
	if p.State != PayoutPending {
		return p, nil
	}
	switch res.Status {
	case bank.Success:
		if err := s.ledger.CommitConfirmed(ctx, tx, p.HoldEntryID); err != nil {
			return Payout{}, fmt.Errorf("payments: commit payout hold: %w", err)
		}
		if err := s.ledger.Post(ctx, tx, ledger.Entry{ID: s.ids.New(), Kind: "payout_sent", RefID: p.ID, Postings: []ledger.Posting{
			{Account: s.sys[ledger.PayoutClearing], Side: ledger.Debit, Amount: p.Amount},
			{Account: s.sys[ledger.BankSettlement], Side: ledger.Credit, Amount: p.Amount},
		}}); err != nil {
			return Payout{}, err
		}
		ref := res.BankRef
		return s.setPayoutState(ctx, tx, id, PayoutProcessed, &ref, nil)
	case bank.Failed:
		if err := s.ledger.VoidHold(ctx, tx, p.HoldEntryID); err != nil {
			return Payout{}, fmt.Errorf("payments: void payout hold: %w", err)
		}
		reason := res.Reason
		if reason == "" {
			reason = "rejected"
		}
		return s.setPayoutState(ctx, tx, id, PayoutFailed, nil, &reason)
	default:
		return p, nil
	}
}

func (s *Service) setPayoutState(ctx context.Context, tx pgx.Tx, id uuid.UUID, state string, bankRef, reason *string) (Payout, error) {
	p, err := scanPayout(tx.QueryRow(ctx, `
		UPDATE payouts
		SET state = $2, version = version + 1, updated_at = $3,
		    bank_ref = COALESCE($4, bank_ref), failure_reason = COALESCE($5, failure_reason),
		    next_poll_at = NULL
		WHERE id = $1 AND state = 'pending'
		RETURNING `+payoutCols, id, state, s.clock.Now(), bankRef, reason))
	if err != nil {
		return Payout{}, err
	}
	return p, s.emitPayout(ctx, tx, p)
}

// PollPayouts resolves pending payouts. One the bank never received is
// submitted now. One the bank still calls pending stays pending, however
// long: failing a payout the bank later completes would pay the merchant
// twice. Its hold (PayoutHoldTTL) is the backstop.
func (s *Service) PollPayouts(ctx context.Context, limit int) (int, error) {
	due, err := s.claimDue(ctx, "payouts", "state = 'pending'", limit)
	if err != nil {
		return 0, err
	}
	n := 0
	var errs []error
	for _, d := range due {
		res, err := s.bankStatus(ctx, d.id)
		if err != nil {
			continue
		}
		if res.Status == bank.Failed && res.Reason == bank.ReasonUnknownID {
			p, err := scanPayout(s.pool.QueryRow(ctx, `SELECT `+payoutCols+` FROM payouts WHERE id = $1`, d.id))
			if err == nil {
				err = s.submitPayout(ctx, p)
			}
			if err != nil {
				errs = append(errs, fmt.Errorf("payout %s: %w", d.id, err))
				continue
			}
			n++
			continue
		}
		if err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
			_, err := s.resolvePayout(ctx, tx, d.id, res)
			return err
		}); err != nil {
			errs = append(errs, fmt.Errorf("payout %s: %w", d.id, err))
			continue
		}
		n++
	}
	return n, errors.Join(errs...)
}

// GetPayout reads one of a merchant's payouts.
func (s *Service) GetPayout(ctx context.Context, merchantID, id uuid.UUID) (PayoutView, error) {
	p, err := scanPayout(s.pool.QueryRow(ctx, `SELECT `+payoutCols+` FROM payouts WHERE id = $1 AND merchant_id = $2`, id, merchantID))
	if errors.Is(err, pgx.ErrNoRows) {
		return PayoutView{}, errorf(http.StatusNotFound, "not_found", "payout %s not found", id)
	}
	if err != nil {
		return PayoutView{}, err
	}
	return p.View(), nil
}
