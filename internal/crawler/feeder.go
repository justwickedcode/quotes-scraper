package crawler

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	"quotes-crawler/internal/scoring"
)

// Discovery runs on its own goroutine per source (runFeeder), separate from the fetch worker
// (runWorker). They used to share one loop: every Wikiquote discovery call took a slot from the
// edition's own fetch cooldown, and discovery only ran once the queue had dropped below 20 URLs,
// so a source alternated between "fetch" and "go find more" and sat idle whenever a discovery
// call came back empty. Now the worker does nothing but pop and fetch, and the feeder keeps the
// queue topped up ahead of it.

// Buffer levels, the same for every source (requested explicitly): the feeder tops a source's
// queue up toward frontierTarget whenever it is below it. Once the queue falls to
// frontierLowWater, the source enters refill mode: the worker stops fetching and the feeder runs
// discovery at the worker's own pace, using the request budget the worker isn't, until the queue
// is back at frontierMin. Refill also ends after refillMaxMisses discovery calls in a row add
// nothing, or after refillMaxDuration, so a source that has genuinely run dry goes back to
// fetching what it has instead of stalling on discovery.
const (
	frontierTarget    = 30
	frontierMin       = 20
	frontierLowWater  = 10
	refillMaxMisses   = 3
	refillMaxDuration = 2 * time.Minute

	// refillCooldown: after a refill ends without restoring the buffer, the worker gets at least
	// this long to fetch before the next refill can start, so a dry source doesn't flip straight
	// back into refill mode and never fetch at all.
	refillCooldown = 1 * time.Minute

	// wikiquoteFeederDelay is the gap between two discovery API calls for one edition outside
	// refill mode, when the worker is fetching from the same host at the same time. In refill
	// mode the gap is the worker's own pace instead (see runFeeder's refillDelay).
	wikiquoteFeederDelay = 10 * time.Second

	// goodreadsSearchStock: on top of the buffer levels above, the feeder keeps this many
	// unfetched keyword or author-name search first pages queued for Goodreads, so there is
	// always a fresh entry point waiting when the pagination chains ahead of it come back as
	// duplicates.
	goodreadsSearchStock = 25

	// feederIdleInterval is how long a feeder sleeps when its source's buffer is full, or when
	// its discovery had nothing to add this round. Short, so a drained buffer is noticed quickly.
	feederIdleInterval = 3 * time.Second
)

// sourceThrottle is the only state a source's worker and feeder share: a pause both observe
// after a 429 on either one, since they hit the same host, and the refill-mode flag. rateLimited carries a feeder's 429
// over to the worker so it drops its experimental pace too (see abandonExperimentalPace).
type sourceThrottle struct {
	mu          sync.Mutex
	until       time.Time
	rateLimited bool
	refilling   bool // set by the feeder in refill mode; the worker holds off fetching meanwhile
}

func (t *sourceThrottle) setRefilling(v bool) {
	t.mu.Lock()
	t.refilling = v
	t.mu.Unlock()
}

func (t *sourceThrottle) isRefilling() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.refilling
}

// backOff pauses the source for at least d from now. It never shortens an existing pause.
func (t *sourceThrottle) backOff(d time.Duration, rateLimited bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if u := time.Now().Add(d); u.After(t.until) {
		t.until = u
	}
	t.rateLimited = t.rateLimited || rateLimited
}

// wait sleeps until any pause has passed. It returns true if ctx was cancelled meanwhile.
func (t *sourceThrottle) wait(ctx context.Context) bool {
	t.mu.Lock()
	d := time.Until(t.until)
	t.mu.Unlock()
	if d <= 0 {
		return false
	}
	return sleepCtx(ctx, d)
}

// takeRateLimited reports whether a 429 was recorded since the last call, and clears it.
func (t *sourceThrottle) takeRateLimited() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	r := t.rateLimited
	t.rateLimited = false
	return r
}

// runFeeder keeps source's queue between the buffer levels above. size reports how many URLs
// are queued; extraNeed, if non-nil, can ask for more even when the queue is full (Goodreads'
// search stock). delay paces discovery calls that made a real request; refillDelay replaces it in
// refill mode. Refill mode pauses the worker only when networked is true: Goodreads discovery
// never touches goodreads.com, so there's no shared request budget to hand over and its worker
// keeps fetching. topUp keeps its one-request-per-call contract (see topUpResult).
func (c *Crawler) runFeeder(ctx context.Context, source string, networked bool, delay, refillDelay time.Duration, throttle *sourceThrottle,
	size func(ctx context.Context) (int64, error), extraNeed func(ctx context.Context) (bool, error), topUp func(ctx context.Context) topUpResult) {
	// Same reason as runWorker's startup stagger: fourteen Wikiquote feeders starting in the same
	// instant would send their first discovery calls together.
	if networked {
		if sleepCtx(ctx, time.Duration(rand.Float64()*float64(15*time.Second))) {
			return
		}
	}

	var refilling bool
	var refillStart, noRefillUntil time.Time
	var misses int
	endRefill := func(n int64, why string) {
		refilling = false
		throttle.setRefilling(false)
		if n < frontierMin {
			noRefillUntil = time.Now().Add(refillCooldown)
		}
		log.Printf("[%s] refill done (%s, %d queued) — back to fetching", source, why, n)
	}
	defer throttle.setRefilling(false)

	for ctx.Err() == nil {
		n, err := size(ctx)
		if err != nil {
			log.Printf("[%s] feeder: could not check frontier size: %s\n", source, err)
			if sleepCtx(ctx, errorBackoff) {
				return
			}
			continue
		}

		switch {
		case !refilling && n <= frontierLowWater && time.Now().After(noRefillUntil):
			refilling, refillStart, misses = true, time.Now(), 0
			throttle.setRefilling(networked)
			log.Printf("[%s] only %d URL(s) queued — refilling before fetching more", source, n)
		case refilling && n >= frontierMin:
			endRefill(n, "buffer restored")
		case refilling && misses >= refillMaxMisses:
			endRefill(n, "discovery found nothing new")
		case refilling && time.Since(refillStart) > refillMaxDuration:
			endRefill(n, "time limit")
		}

		need := refilling || n < frontierTarget
		if !need && extraNeed != nil {
			if need, err = extraNeed(ctx); err != nil {
				log.Printf("[%s] feeder: %s\n", source, err)
			}
		}
		if !need {
			if sleepCtx(ctx, feederIdleInterval) {
				return
			}
			continue
		}

		if throttle.wait(ctx) {
			return
		}
		result := topUp(ctx)
		switch {
		case result.added:
			misses = 0
		case !result.networkFailed:
			misses++
		}

		var pause time.Duration
		switch {
		case result.networkFailed:
			pause = stallPenaltyFor(source)
			if result.rateLimited {
				throttle.backOff(pause, true)
			}
		case result.calledNetwork && refilling:
			pause = jitter(refillDelay, 0.2)
		case result.calledNetwork:
			pause = jitter(delay, 0.2)
		case result.added || refilling:
			// DB-only seeding (Goodreads) or refilling: check again straight away. The miss
			// counter ends a refill that keeps finding nothing.
			continue
		default:
			pause = feederIdleInterval
		}
		if sleepCtx(ctx, pause) {
			return
		}
	}
}

// frontierSizeFunc is the size function every feeder uses: the source's queued URL count.
func (c *Crawler) frontierSizeFunc(source string) func(ctx context.Context) (int64, error) {
	return func(ctx context.Context) (int64, error) {
		return c.store.FrontierSize(ctx, source)
	}
}

// goodreadsNeedsSearches is the Goodreads feeder's extraNeed: fewer than goodreadsSearchStock
// search first pages waiting.
func (c *Crawler) goodreadsNeedsSearches(ctx context.Context) (bool, error) {
	p := scoring.GoodreadsPriority("https://www.goodreads.com/quotes/search?q=x", 0)
	searches, err := c.store.FrontierCountByPriority(ctx, scoring.SourceGoodreads, p, p)
	return searches < goodreadsSearchStock, err
}
