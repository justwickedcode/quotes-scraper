package crawler

import (
	"context"
	"testing"
	"time"
)

func TestSourceThrottleBackOffAndWait(t *testing.T) {
	var th sourceThrottle
	ctx := context.Background()

	start := time.Now()
	if th.wait(ctx) {
		t.Fatal("wait reported cancellation with no pause set")
	}
	if time.Since(start) > 10*time.Millisecond {
		t.Fatal("wait slept with no pause set")
	}

	th.backOff(50*time.Millisecond, false)
	th.backOff(10*time.Millisecond, false) // must not shorten the pause
	start = time.Now()
	th.wait(ctx)
	if d := time.Since(start); d < 40*time.Millisecond {
		t.Fatalf("wait returned after %s, want ~50ms", d)
	}
	if th.takeRateLimited() {
		t.Fatal("rateLimited set without a 429")
	}

	th.backOff(0, true)
	if !th.takeRateLimited() {
		t.Fatal("rateLimited not recorded")
	}
	if th.takeRateLimited() {
		t.Fatal("takeRateLimited did not clear the flag")
	}
}

func TestSourceThrottleWaitCancelled(t *testing.T) {
	var th sourceThrottle
	th.backOff(time.Hour, false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !th.wait(ctx) {
		t.Fatal("wait should report cancellation")
	}
}
