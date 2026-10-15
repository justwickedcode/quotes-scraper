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

const (
	// wikiquoteFrontierTarget: the feeder keeps at least this many URLs queued per Wikiquote
	// edition. One category or allpages call adds up to 500, and the worker drains one URL per
	// ~5s, so once the buffer is full the feeder makes a discovery call only every ~40 minutes.
	wikiquoteFrontierTarget = 1000

	// wikiquoteFeederDelay is the minimum gap between two discovery API calls for one edition.
	// These calls go to the same host as the edition's page fetches, so the feeder keeps its own
	// pace on top of the worker's; a 429 on either side pauses both (see sourceThrottle).
	wikiquoteFeederDelay = 10 * time.Second

	// goodreadsSearchStock: the feeder keeps this many unfetched keyword or author-name search
	// first pages queued for Goodreads, so there is always a fresh entry point waiting when the
	// pagination chains ahead of it come back as duplicates.
	goodreadsSearchStock = 25

	// goodreadsFrontierFloor: below this many queued Goodreads URLs in total, the feeder adds
	// seeds regardless of how many searches are already waiting.
	goodreadsFrontierFloor = 200

	// feederIdleInterval is how long a feeder sleeps when its source's buffer is full, or when
	// its discovery had nothing to add this round.
	feederIdleInterval = 15 * time.Second
)

// sourceThrottle is the only state a source's worker and feeder share: a pause both observe
// after a 429 on either one, since they hit the same host. rateLimited carries a feeder's 429
// over to the worker so it drops its experimental pace too (see abandonExperimentalPace).
type sourceThrottle struct {
	mu          sync.Mutex
	until       time.Time
	rateLimited bool
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

// runFeeder calls topUp whenever needsMore says the source's queue is short, pacing calls that
// made a real request by delay and backing off on failure. topUp keeps its one-request-per-call
// contract (see topUpResult), so delay is exactly the gap between discovery requests.
func (c *Crawler) runFeeder(ctx context.Context, source string, delay time.Duration, throttle *sourceThrottle,
	needsMore func(ctx context.Context) (bool, error), topUp func(ctx context.Context) topUpResult) {
	// Same reason as runWorker's startup stagger: fourteen Wikiquote feeders starting in the same
	// instant would send their first discovery calls together.
	if source != scoring.SourceGoodreads {
		if sleepCtx(ctx, time.Duration(rand.Float64()*float64(15*time.Second))) {
			return
		}
	}

	for ctx.Err() == nil {
		need, err := needsMore(ctx)
		if err != nil {
			log.Printf("[%s] feeder: could not check frontier size: %s\n", source, err)
			if sleepCtx(ctx, errorBackoff) {
				return
			}
			continue
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
		var pause time.Duration
		switch {
		case result.networkFailed:
			pause = stallPenaltyFor(source)
			if result.rateLimited {
				throttle.backOff(pause, true)
			}
		case result.calledNetwork:
			pause = jitter(delay, 0.2)
		case result.added:
			// DB-only seeding (Goodreads): check again straight away.
			continue
		default:
			pause = feederIdleInterval
		}
		if sleepCtx(ctx, pause) {
			return
		}
	}
}

// wikiquoteNeedsMore is a Wikiquote feeder's needsMore: queue below wikiquoteFrontierTarget.
func (c *Crawler) wikiquoteNeedsMore(source string) func(ctx context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		n, err := c.store.FrontierSize(ctx, source)
		return n < wikiquoteFrontierTarget, err
	}
}

// goodreadsNeedsMore is the Goodreads feeder's needsMore: fewer than goodreadsSearchStock search
// first pages waiting, or the whole queue below goodreadsFrontierFloor.
func (c *Crawler) goodreadsNeedsMore(ctx context.Context) (bool, error) {
	total, err := c.store.FrontierSize(ctx, scoring.SourceGoodreads)
	if err != nil {
		return false, err
	}
	if total < goodreadsFrontierFloor {
		return true, nil
	}
	p := scoring.GoodreadsPriority("https://www.goodreads.com/quotes/search?q=x", 0)
	searches, err := c.store.FrontierCountByPriority(ctx, scoring.SourceGoodreads, p, p)
	return searches < goodreadsSearchStock, err
}
