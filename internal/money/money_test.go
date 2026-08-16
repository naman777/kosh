package money

import (
	"errors"
	"math"
	"math/rand/v2"
	"testing"
)

func TestFeesReadmeExample(t *testing.T) {
	// ₹1,000 at 2% plus 18% GST on the fee: the worked example in the README.
	b, err := Fees(Rupees(1000), 200)
	if err != nil {
		t.Fatal(err)
	}
	want := Breakdown{Amount: 100_000, Fee: 2_000, GST: 360, Net: 97_640}
	if b != want {
		t.Fatalf("Fees = %+v, want %+v", b, want)
	}
}

func TestFeesRounding(t *testing.T) {
	cases := []struct {
		amount   Paise
		bps      int64
		fee, gst Paise
		name     string
	}{
		{name: "one paisa rounds fee to zero", amount: 1, bps: 200, fee: 0, gst: 0},
		{name: "exact half rounds up", amount: 25, bps: 200, fee: 1, gst: 0},           // 0.5 → 1
		{name: "just under half rounds down", amount: 24, bps: 200, fee: 0, gst: 0},    // 0.48 → 0
		{name: "fee rounds up, gst down", amount: 13_889, bps: 200, fee: 278, gst: 50}, // 277.78→278, 50.04→50
		{name: "gst exact half rounds up", amount: 1_250, bps: 200, fee: 25, gst: 5},   // gst 4.5→5
		{name: "odd amount", amount: 99_999, bps: 199, fee: 1_990, gst: 358},           // 1989.98→1990, 358.2→358
		{name: "zero fee", amount: 50_000, bps: 0, fee: 0, gst: 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b, err := Fees(c.amount, c.bps)
			if err != nil {
				t.Fatal(err)
			}
			if b.Fee != c.fee || b.GST != c.gst {
				t.Fatalf("Fees(%d, %d) fee=%d gst=%d, want fee=%d gst=%d", c.amount, c.bps, b.Fee, b.GST, c.fee, c.gst)
			}
			if b.Fee+b.GST+b.Net != b.Amount {
				t.Fatalf("lines do not sum: %+v", b)
			}
		})
	}
}

func TestFeesAlwaysSum(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for i := 0; i < 200_000; i++ {
		amount := Paise(r.Int64N(math.MaxInt64/2) + 1)
		if i%2 == 0 {
			amount = Paise(r.Int64N(10_000_000) + 1) // realistic sizes too
		}
		bps := r.Int64N(5_000)
		b, err := Fees(amount, bps)
		if err != nil {
			t.Fatalf("Fees(%d, %d): %v", amount, bps, err)
		}
		if b.Fee+b.GST+b.Net != amount || b.Net < 0 || b.Fee < 0 || b.GST < 0 {
			t.Fatalf("bad breakdown %+v", b)
		}
	}
}

func TestFeesErrors(t *testing.T) {
	if _, err := Fees(0, 200); !errors.Is(err, ErrNegative) {
		t.Fatalf("zero amount: %v", err)
	}
	if _, err := Fees(100, 10_001); !errors.Is(err, ErrBadRate) {
		t.Fatalf("bad rate: %v", err)
	}
	if _, err := Fees(100, 10_000); !errors.Is(err, ErrFeeExceedsAmount) {
		t.Fatalf("100%% fee plus GST: %v", err)
	}
}

func TestMulBpsMaxAmount(t *testing.T) {
	got, err := Paise(math.MaxInt64).MulBps(BpsDenominator)
	if err != nil || got != math.MaxInt64 {
		t.Fatalf("MaxInt64 × 100%% = %d, %v", got, err)
	}
	got, err = Paise(math.MaxInt64).MulBps(200)
	if err != nil {
		t.Fatal(err)
	}
	// 9223372036854775807 × 0.02 = 184467440737095516.14 → ...516
	if got != 184_467_440_737_095_516 {
		t.Fatalf("got %d", got)
	}
}

func TestAddSubOverflow(t *testing.T) {
	if _, err := Paise(math.MaxInt64).Add(1); !errors.Is(err, ErrOverflow) {
		t.Fatal("Add should overflow")
	}
	if _, err := Paise(math.MinInt64).Sub(1); !errors.Is(err, ErrOverflow) {
		t.Fatal("Sub should overflow")
	}
	if s, err := Paise(5).Sub(7); err != nil || s != -2 {
		t.Fatalf("5-7 = %d, %v", s, err)
	}
}

func TestString(t *testing.T) {
	cases := map[Paise]string{
		0:             "₹0.00",
		5:             "₹0.05",
		100_000:       "₹1,000.00",
		97_640:        "₹976.40",
		10_000_050:    "₹1,00,000.50",
		1_234_567_890: "₹1,23,45,678.90",
		-5:            "-₹0.05",
		math.MinInt64: "-₹92,23,37,20,36,85,47,758.08",
	}
	for p, want := range cases {
		if got := p.String(); got != want {
			t.Errorf("Paise(%d).String() = %q, want %q", int64(p), got, want)
		}
	}
}
