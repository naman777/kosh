package main

import (
	"testing"
	"time"
)

func TestNextReset(t *testing.T) {
	ist, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		now, at, want string // now and want in IST
	}{
		{"2026-10-18 03:59", "04:00", "2026-10-18 04:00"},
		{"2026-10-18 04:00", "04:00", "2026-10-19 04:00"}, // exactly now: tomorrow
		{"2026-10-18 13:00", "04:00", "2026-10-19 04:00"},
		{"2026-12-31 23:30", "04:00", "2027-01-01 04:00"},
	}
	for _, c := range cases {
		now, _ := time.ParseInLocation("2006-01-02 15:04", c.now, ist)
		got, err := nextReset(now.UTC(), c.at, ist)
		if err != nil {
			t.Fatal(err)
		}
		if want, _ := time.ParseInLocation("2006-01-02 15:04", c.want, ist); !got.Equal(want) {
			t.Errorf("nextReset(%s, %s) = %s, want %s", c.now, c.at, got.In(ist), c.want)
		}
	}
	if _, err := nextReset(time.Now(), "4am", ist); err == nil {
		t.Error("a bad LAB_RESET_AT should be an error")
	}
}
