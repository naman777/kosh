package ledger

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Chart of accounts. System accounts have no owner; merchant accounts are
// owned by the merchant's id.
const (
	// BankReceivable (asset) is money the bank owes Kosh for captured payments.
	BankReceivable = "bank_receivable"
	// BankSettlement (asset) is cash that has landed in Kosh's bank account.
	BankSettlement = "bank_settlement"
	// FeeRevenue (revenue) is Kosh's fee on each capture.
	FeeRevenue = "fee_revenue"
	// GSTPayable (liability) is GST collected on fees, owed to the government.
	GSTPayable = "gst_payable"
	// PayoutClearing (liability) holds merchant money between payout
	// initiation and bank confirmation.
	PayoutClearing = "payout_clearing"
	// UnallocatedReceipts (liability) is customer money the bank has
	// collected for an authorized payment that is not yet captured: it
	// belongs to nobody until capture assigns it to the merchant.
	UnallocatedReceipts = "unallocated_receipts"
	// CustomerRefundPayable (liability) is money Kosh owes customers whose
	// payment succeeded at the bank after Kosh had marked it failed.
	CustomerRefundPayable = "customer_refund_payable"
	// BankCharges (expense) is what the bank deducts from settlements.
	BankCharges = "bank_charges"
	// ReconSuspense holds settlement differences until someone resolves them:
	// a debit balance is money the bank owes Kosh, a credit balance the reverse.
	ReconSuspense = "recon_suspense"
	// ReconWriteoff (expense) absorbs differences resolved as written off.
	ReconWriteoff = "recon_writeoff"
	// MerchantPayable (liability) is what Kosh owes a merchant. It may never
	// go negative, counting holds.
	MerchantPayable = "merchant_payable"
)

// AccountSpec describes an account to create.
type AccountSpec struct {
	Code    string
	OwnerID uuid.NullUUID
	Normal  NormalSide
	Flags   Flags
	Shard   int // 0 unless the account is sharded (see chart.go)
}

// SystemAccounts is the fixed part of the chart of accounts.
var SystemAccounts = []AccountSpec{
	{Code: BankReceivable, Normal: NormalDebit},
	{Code: BankSettlement, Normal: NormalDebit},
	{Code: FeeRevenue, Normal: NormalCredit},
	{Code: GSTPayable, Normal: NormalCredit},
	{Code: PayoutClearing, Normal: NormalCredit},
	{Code: CustomerRefundPayable, Normal: NormalCredit, Flags: DebitsMustNotExceedCredits},
	{Code: UnallocatedReceipts, Normal: NormalCredit, Flags: DebitsMustNotExceedCredits},
	{Code: BankCharges, Normal: NormalDebit},
	{Code: ReconSuspense, Normal: NormalDebit},
	{Code: ReconWriteoff, Normal: NormalDebit},
}

// MerchantPayableSpec is the per-merchant payable account.
func MerchantPayableSpec(merchantID uuid.UUID) AccountSpec {
	return AccountSpec{
		Code:    MerchantPayable,
		OwnerID: uuid.NullUUID{UUID: merchantID, Valid: true},
		Normal:  NormalCredit,
		Flags:   DebitsMustNotExceedCredits,
	}
}

// EnsureAccount creates the account if it does not exist and returns its id.
// It is idempotent: (code, owner, shard) is unique.
func (l *Ledger) EnsureAccount(ctx context.Context, q Querier, s AccountSpec) (AccountID, error) {
	var id AccountID
	err := q.QueryRow(ctx, `
		INSERT INTO accounts (code, owner_id, normal_side, flags, shard)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT DO NOTHING
		RETURNING id`, s.Code, s.OwnerID, string(s.Normal), int32(s.Flags), s.Shard).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("ledger: create account %s: %w", s.Code, err)
	}
	err = q.QueryRow(ctx, `
		SELECT id FROM accounts WHERE code = $1 AND owner_id IS NOT DISTINCT FROM $2 AND shard = $3`,
		s.Code, s.OwnerID, s.Shard).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("ledger: find account %s: %w", s.Code, err)
	}
	return id, nil
}

// EnsureSystemAccounts creates shard 0 of every system account and returns
// their ids by code. Services use EnsureChart, which knows about shards.
func (l *Ledger) EnsureSystemAccounts(ctx context.Context, q Querier) (map[string]AccountID, error) {
	out := make(map[string]AccountID, len(SystemAccounts))
	for _, s := range SystemAccounts {
		id, err := l.EnsureAccount(ctx, q, s)
		if err != nil {
			return nil, err
		}
		out[s.Code] = id
	}
	return out, nil
}

// GetAccount reads one account without locking it.
func (l *Ledger) GetAccount(ctx context.Context, q Querier, id AccountID) (Account, error) {
	var (
		a      Account
		normal string
	)
	err := q.QueryRow(ctx, `
		SELECT id, code, owner_id, normal_side, flags, posted_dr, posted_cr, pending_dr, pending_cr
		FROM accounts WHERE id = $1`, id).Scan(
		&a.ID, &a.Code, &a.OwnerID, &normal, &a.Flags,
		&a.PostedDr, &a.PostedCr, &a.PendingDr, &a.PendingCr)
	if errors.Is(err, pgx.ErrNoRows) {
		return Account{}, ErrAccountNotFound
	}
	if err != nil {
		return Account{}, err
	}
	a.Normal = NormalSide(normal)
	return a, nil
}

// EntryState returns an entry's state.
func (l *Ledger) EntryState(ctx context.Context, q Querier, id uuid.UUID) (string, error) {
	var s string
	err := q.QueryRow(ctx, `SELECT state FROM journal_entries WHERE id = $1`, id).Scan(&s)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrEntryNotFound
	}
	return s, err
}
