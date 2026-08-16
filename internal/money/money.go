// Package money is the only place amounts are created or combined.
//
// Every amount in Kosh is a whole number of paise (1 rupee = 100 paise) held
// in an int64. Floats never touch money. Arithmetic that could overflow
// returns ErrOverflow instead of wrapping.
//
// # Rounding rules
//
// Percentages are expressed in basis points (1 bps = 0.01%). A percentage of
// an amount is computed exactly with 128-bit intermediate math and then
// rounded half-up to the nearest paisa:
//
//	fee = round_half_up(amount × fee_bps / 10,000)
//	gst = round_half_up(fee × 1,800 / 10,000)      // 18% GST on the fee
//	net = amount − fee − gst
//
// The merchant's net line absorbs all rounding, so fee + gst + net always
// equals the amount exactly and a capture entry always balances. Fee and GST
// are each rounded once, from exact values, and never from an already
// rounded intermediate other than the fee itself (GST is legally charged on
// the fee as invoiced).
package money

import (
	"errors"
	"math"
	"math/bits"
	"strconv"
	"strings"
)

// Paise is an amount of Indian rupees in paise.
type Paise int64

// BpsDenominator is 100% in basis points.
const BpsDenominator = 10_000

// GSTBps is the GST rate charged on Kosh's fee: 18%.
const GSTBps = 1_800

var (
	// ErrOverflow means the result would not fit in an int64.
	ErrOverflow = errors.New("money: overflow")
	// ErrNegative means an operation that requires a non-negative amount got a negative one.
	ErrNegative = errors.New("money: negative amount")
	// ErrBadRate means a basis-point rate was outside [0, 10000].
	ErrBadRate = errors.New("money: rate out of range")
	// ErrFeeExceedsAmount means fee plus GST would be more than the amount.
	ErrFeeExceedsAmount = errors.New("money: fee and GST exceed amount")
)

// Add returns p + q, or ErrOverflow.
func (p Paise) Add(q Paise) (Paise, error) {
	s := p + q
	if (q > 0 && s < p) || (q < 0 && s > p) {
		return 0, ErrOverflow
	}
	return s, nil
}

// Sub returns p − q, or ErrOverflow.
func (p Paise) Sub(q Paise) (Paise, error) {
	d := p - q
	if (q > 0 && d > p) || (q < 0 && d < p) {
		return 0, ErrOverflow
	}
	return d, nil
}

// MulBps returns round_half_up(p × bps / 10,000) for p ≥ 0 and 0 ≤ bps ≤ 10,000.
// The product is computed in 128 bits, so it is exact for every int64 amount.
func (p Paise) MulBps(bps int64) (Paise, error) {
	if p < 0 {
		return 0, ErrNegative
	}
	if bps < 0 || bps > BpsDenominator {
		return 0, ErrBadRate
	}
	hi, lo := bits.Mul64(uint64(p), uint64(bps))
	// Add half the denominator for half-up rounding.
	lo, carry := bits.Add64(lo, BpsDenominator/2, 0)
	hi += carry
	if hi >= BpsDenominator {
		return 0, ErrOverflow
	}
	q, _ := bits.Div64(hi, lo, BpsDenominator)
	if q > math.MaxInt64 {
		return 0, ErrOverflow
	}
	return Paise(q), nil
}

// Breakdown splits a captured amount into the merchant's net, Kosh's fee and
// the GST on that fee. Fee + GST + Net == Amount always.
type Breakdown struct {
	Amount Paise
	Fee    Paise
	GST    Paise
	Net    Paise
}

// Fees computes the breakdown for amount at feeBps using the package's
// rounding rules. amount must be positive.
func Fees(amount Paise, feeBps int64) (Breakdown, error) {
	if amount <= 0 {
		return Breakdown{}, ErrNegative
	}
	fee, err := amount.MulBps(feeBps)
	if err != nil {
		return Breakdown{}, err
	}
	gst, err := fee.MulBps(GSTBps)
	if err != nil {
		return Breakdown{}, err
	}
	// fee ≤ amount and gst ≤ fee, so these cannot overflow.
	net := amount - fee - gst
	if net < 0 {
		return Breakdown{}, ErrFeeExceedsAmount
	}
	return Breakdown{Amount: amount, Fee: fee, GST: gst, Net: net}, nil
}

// Rupees builds an amount from whole rupees, for tests and examples.
func Rupees(r int64) Paise { return Paise(r * 100) }

// String formats with Indian digit grouping: ₹1,00,000.50, -₹0.05.
func (p Paise) String() string {
	neg := p < 0
	// Work in uint64 so math.MinInt64 negates cleanly.
	u := uint64(p)
	if neg {
		u = -u
	}
	rupees := strconv.FormatUint(u/100, 10)
	paise := u % 100

	var b strings.Builder
	if neg {
		b.WriteByte('-')
	}
	b.WriteString("₹")
	b.WriteString(groupIndian(rupees))
	b.WriteByte('.')
	if paise < 10 {
		b.WriteByte('0')
	}
	b.WriteString(strconv.FormatUint(paise, 10))
	return b.String()
}

// groupIndian puts a comma after the last three digits, then every two:
// 12345678 → 1,23,45,678.
func groupIndian(d string) string {
	if len(d) <= 3 {
		return d
	}
	head, tail := d[:len(d)-3], d[len(d)-3:]
	var parts []string
	for len(head) > 2 {
		parts = append([]string{head[len(head)-2:]}, parts...)
		head = head[:len(head)-2]
	}
	if head != "" {
		parts = append([]string{head}, parts...)
	}
	return strings.Join(parts, ",") + "," + tail
}
