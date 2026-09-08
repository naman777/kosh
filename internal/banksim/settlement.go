package banksim

import (
	"encoding/csv"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"

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

// Settlement builds the file for business date (YYYY-MM-DD, IST): every
// payment and refund that became final and successful that day. The same
// date always yields the same file, faults included.
func (s *Server) Settlement(date string) ([]Line, []Fault, error) {
	day, err := time.ParseInLocation("2006-01-02", date, IST)
	if err != nil {
		return nil, nil, err
	}
	valueDate := day.AddDate(0, 0, 1).Format("2006-01-02")

	s.mu.Lock()
	cfg := s.cfg.Settlement
	now := s.clock.Now()
	var recs []record
	for _, id := range s.order {
		r := s.records[id]
		if r.Final.Status != bank.Success || (r.Kind != bank.KindPayment && r.Kind != bank.KindRefund) {
			continue
		}
		if r.FinalAt.After(now) || r.FinalAt.In(IST).Format("2006-01-02") != date {
			continue
		}
		recs = append(recs, *r)
	}
	s.mu.Unlock()

	h := fnv.New64a()
	h.Write([]byte(date))
	rng := rand.New(rand.NewPCG(s.seed, h.Sum64()))

	var (
		lines  []Line
		faults []Fault
	)
	emit := func(l Line) {
		l.LineNo = len(lines) + 1
		l.Net = l.Gross - l.Fee - l.Tax
		lines = append(lines, l)
	}
	for _, r := range recs {
		l := Line{BankRef: r.Final.BankRef, Reference: r.ID.String(), Gross: r.Amount, ValueDate: valueDate}
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

func (s *Server) serveSettlement(w http.ResponseWriter, r *http.Request) {
	lines, _, err := s.Settlement(r.PathValue("date"))
	if err != nil {
		http.Error(w, "date must be YYYY-MM-DD", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "text/csv")
	cw := csv.NewWriter(w)
	cw.Write([]string{"line_no", "bank_ref", "reference", "gross", "fee", "tax", "net", "value_date"})
	for _, l := range lines {
		cw.Write([]string{strconv.Itoa(l.LineNo), l.BankRef, l.Reference,
			fmt.Sprint(int64(l.Gross)), fmt.Sprint(int64(l.Fee)), fmt.Sprint(int64(l.Tax)), fmt.Sprint(int64(l.Net)), l.ValueDate})
	}
	cw.Flush()
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
