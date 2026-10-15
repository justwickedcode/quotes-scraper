package crawler

import (
	"testing"
)

func TestAdaptGoodreadsPace(t *testing.T) {
	delay, streak := goodreadsStartDelay, 0

	// Speeds up by one step after goodreadsSpeedUpAfter fast fetches.
	for i := 0; i < goodreadsSpeedUpAfter; i++ {
		delay, streak = adaptGoodreadsPace(delay, streak, false)
	}
	if want := goodreadsStartDelay - goodreadsSpeedUpStep; delay != want || streak != 0 {
		t.Fatalf("after %d fast fetches: delay=%s streak=%d, want %s 0", goodreadsSpeedUpAfter, delay, streak, want)
	}

	// Never goes below the floor.
	for i := 0; i < 1000; i++ {
		delay, streak = adaptGoodreadsPace(delay, streak, false)
	}
	if delay != goodreadsMinDelay {
		t.Fatalf("delay=%s, want floor %s", delay, goodreadsMinDelay)
	}

	// A stall doubles it and resets the streak.
	delay, streak = adaptGoodreadsPace(delay, 5, true)
	if delay != 2*goodreadsMinDelay || streak != 0 {
		t.Fatalf("after stall: delay=%s streak=%d", delay, streak)
	}

	// Never goes above the ceiling.
	for i := 0; i < 10; i++ {
		delay, _ = adaptGoodreadsPace(delay, 0, true)
	}
	if delay != goodreadsMaxDelay {
		t.Fatalf("delay=%s, want ceiling %s", delay, goodreadsMaxDelay)
	}
}
