package banksim

import (
	"encoding/csv"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/naman777/kosh/internal/bank"
	"github.com/naman777/kosh/internal/money"
)

// IST is the zone settlement dates are cut in.
var IST = time.FixedZone("IST", 5*3600+30*60)

// SettlementFaults are per-line probabilities of corrupting a settlement
// file. At most one fault is applied to a line, so the truth stays simple.
type SettlementFaults struct {
	FeeVarianceRate float64 `json:"fee_variance_rate"` // bank deducts a fee off its own schedule
	MissingRate     float64 `json:"missing_rate"`      // a settled payment is left out
	DuplicateRate   float64 `json:"duplicate_rate"`    // a line appears twice
	MismatchRate    float64 `json:"mismatch_rate"`     // gross differs from the authorized amount
}

// Line is one row of a settlement file. Payments are positive, refunds
// negative. Net = Gross − Fee − Tax.
type Line struct {
	LineNo    int
	BankRef   string
	Reference string // Kosh's payment or refund id
	Gross     money.Paise
	Fee       money.Paise
	Tax       money.Paise
	Net       money.Paise
	ValueDate string
}

// Fault records one injected corruption, so reconciliation can be scored.
type Fault struct {
	BankRef string `json:"bank_ref"`
	Kind    string `json:"kind"` // fee_variance | missing | duplicate | amount_mismatch
}

// SettledItem is a successful, final bank movement: what a settlement file
// is built from.
type SettledItem struct {
	Kind    string // bank.KindPayment | bank.KindRefund
	ID      uuid.UUID
	Amount  money.Paise
	Method  string
	BankRef string
	FinalAt time.Time
}

// BuildSettlement builds the file for business date (YYYY-MM-DD, IST) from
// items, in the order given: every payment and refund that became final
// that day. The same inputs always give the same file, faults included, so
// the HTTP simulator and the simulation harness produce identical files.
func BuildSettlement(items []SettledItem, date string, seed uint64, cfg SettlementFaults) ([]Line, []Fault, error) {
	day, err := time.ParseInLocation("2006-01-02", date, IST)
	if err != nil {
		return nil, nil, err
	}
	valueDate := day.AddDate(0, 0, 1).Format("2006-01-02")

	h := fnv.New64a()
	h.Write([]byte(date))
	rng := rand.New(rand.NewPCG(seed, h.Sum64()))

	var (
		lines  []Line
		faults []Fault
	)
	emit := func(l Line) {
		l.LineNo = len(lines) + 1
		l.Net = l.Gross - l.Fee - l.Tax
		lines = append(lines, l)
	}
	for _, r := range items {
		if (r.Kind != bank.KindPayment && r.Kind != bank.KindRefund) || r.FinalAt.In(IST).Format("2006-01-02") != date {
			continue
		}
		l := Line{BankRef: r.BankRef, Reference: r.ID.String(), Gross: r.Amount, ValueDate: valueDate}
		if r.Kind == bank.KindRefund {
			l.Gross = -r.Amount
		} else {
			l.Fee, l.Tax = bank.Fee(r.Method, r.Amount)
		}
		x := rng.Float64()
		switch {
		case x < cfg.MissingRate:
			faults = append(faults, Fault{BankRef: l.BankRef, Kind: "missing"})
		case x < cfg.MissingRate+cfg.DuplicateRate:
			faults = append(faults, Fault{BankRef: l.BankRef, Kind: "duplicate"})
			emit(l)
			emit(l)
		case x < cfg.MissingRate+cfg.DuplicateRate+cfg.MismatchRate && r.Kind == bank.KindPayment:
			faults = append(faults, Fault{BankRef: l.BankRef, Kind: "amount_mismatch"})
			l.Gross += money.Paise(1 + rng.Int64N(1_000))
			emit(l)
		case x < cfg.MissingRate+cfg.DuplicateRate+cfg.MismatchRate+cfg.FeeVarianceRate && r.Kind == bank.KindPayment:
			faults = append(faults, Fault{BankRef: l.BankRef, Kind: "fee_variance"})
			l.Fee += money.Paise(1 + rng.Int64N(500))
			emit(l)
		default:
			emit(l)
		}
	}
	return lines, faults, nil
}

var csvHeader = []string{"line_no", "bank_ref", "reference", "gross", "fee", "tax", "net", "value_date"}

// WriteCSV writes a settlement file.
func WriteCSV(w io.Writer, lines []Line) error {
	cw := csv.NewWriter(w)
	if err := cw.Write(csvHeader); err != nil {
		return err
	}
	for _, l := range lines {
		if err := cw.Write([]string{strconv.Itoa(l.LineNo), l.BankRef, l.Reference,
			strconv.FormatInt(int64(l.Gross), 10), strconv.FormatInt(int64(l.Fee), 10),
			strconv.FormatInt(int64(l.Tax), 10), strconv.FormatInt(int64(l.Net), 10), l.ValueDate}); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

// ParseCSV reads a settlement file, rejecting anything malformed: a bank
// file that doesn't add up is an incident, not something to guess about.
func ParseCSV(r io.Reader) ([]Line, error) {
	recs, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, err
	}
	if len(recs) == 0 || len(recs[0]) != len(csvHeader) || recs[0][0] != csvHeader[0] {
		return nil, errors.New("settlement: missing or wrong header")
	}
	lines := make([]Line, 0, len(recs)-1)
	for i, rec := range recs[1:] {
		if len(rec) != len(csvHeader) {
			return nil, fmt.Errorf("settlement: line %d has %d fields", i+1, len(rec))
		}
		var l Line
		nums := make([]int64, 5)
		for j, k := range []int{0, 3, 4, 5, 6} {
			if nums[j], err = strconv.ParseInt(rec[k], 10, 64); err != nil {
				return nil, fmt.Errorf("settlement: line %d field %s: %w", i+1, csvHeader[k], err)
			}
		}
		l.LineNo, l.Gross, l.Fee, l.Tax, l.Net = int(nums[0]), money.Paise(nums[1]), money.Paise(nums[2]), money.Paise(nums[3]), money.Paise(nums[4])
		l.BankRef, l.Reference, l.ValueDate = rec[1], rec[2], rec[7]
		if l.LineNo != i+1 {
			return nil, fmt.Errorf("settlement: line %d numbered %d", i+1, l.LineNo)
		}
		if l.Net != l.Gross-l.Fee-l.Tax {
			return nil, fmt.Errorf("settlement: line %d: net %d ≠ gross − fee − tax", l.LineNo, l.Net)
		}
		if l.BankRef == "" {
			return nil, fmt.Errorf("settlement: line %d has no bank_ref", l.LineNo)
		}
		lines = append(lines, l)
	}
	return lines, nil
}

// Settlement builds the file for business date from this simulator's
// records that are final and successful by now.
func (s *Server) Settlement(date string) ([]Line, []Fault, error) {
	s.mu.Lock()
	cfg := s.cfg.Settlement
	now := s.clock.Now()
	var items []SettledItem
	for _, id := range s.order {
		r := s.records[id]
		if r.Final.Status != bank.Success || r.FinalAt.After(now) {
			continue
		}
		items = append(items, SettledItem{Kind: r.Kind, ID: r.ID, Amount: r.Amount, Method: r.Method,
			BankRef: r.Final.BankRef, FinalAt: r.FinalAt})
	}
	s.mu.Unlock()
	return BuildSettlement(items, date, s.seed, cfg)
}

func (s *Server) serveSettlement(w http.ResponseWriter, r *http.Request) {
	lines, _, err := s.Settlement(r.PathValue("date"))
	if err != nil {
		http.Error(w, "date must be YYYY-MM-DD", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "text/csv")
	WriteCSV(w, lines)
}

func (s *Server) serveFaults(w http.ResponseWriter, r *http.Request) {
	_, faults, err := s.Settlement(r.PathValue("date"))
	if err != nil {
		http.Error(w, "date must be YYYY-MM-DD", http.StatusBadRequest)
		return
	}
	if faults == nil {
		faults = []Fault{}
	}
	writeJSON(w, faults)
}
