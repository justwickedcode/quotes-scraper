package crawler

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"quotes-crawler/internal/db"
	"quotes-crawler/internal/dedup"
	"quotes-crawler/internal/fetcher"
	"quotes-crawler/internal/models"
	"quotes-crawler/internal/parser"
	"quotes-crawler/internal/scoring"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// defaultMinSourceDelay is the minimum time between two fetches of the same source, used
	// for any source without a specific entry in minSourceDelayBySource.
	defaultMinSourceDelay = 3 * time.Second

	// idlePollInterval is how long a source's worker sleeps when it has nothing to do at all —
	// on cooldown or genuinely out of pending work, with no new work to top up. Short, because
	// something is likely to free up soon (its cooldown ticking down), but not zero, to avoid
	// busy-spinning Redis/Postgres.
	idlePollInterval = 2 * time.Second

	// slowFetchWarn logs a warning when a single fetch takes longer than this — the visible,
	// live signal for "this request just got soft-throttled" instead of only finding out via
	// manual curl testing after the fact.
	slowFetchWarn = 10 * time.Second

	// errorBackoff is a short pause after a Redis/Postgres error before retrying the loop.
	// Without it, a persistent outage (Redis restarting, Postgres unreachable) spins the loop
	// as fast as the CPU allows, hammering both the log and the struggling dependency with
	// reconnect attempts — the same "too aggressive" problem as no fetch rate limiting, just
	// against our own infrastructure instead of a crawl target.
	errorBackoff = 1 * time.Second

	// goodreadsStallPenalty is how long Goodreads specifically is treated as unavailable after a
	// slow fetch (see slowFetchWarn) or an outright failure — matches its own observed stall
	// duration (~60s, still 200 OK, just slow), long enough to actually mean something rather
	// than a slightly-longer version of the normal per-source delay.
	goodreadsStallPenalty = 60 * time.Second

	// wikiquoteStallPenalty is the equivalent for any Wikiquote edition — deliberately much
	// shorter than Goodreads', lowered explicitly ("why backing off for 1 entire minute, why for
	// so long?"). Reusing Goodreads' 60s here was never separately calibrated for Wikiquote's
	// own failure mode: a 429 is an explicit, short-lived rejection that clears once request
	// volume drops, not Goodreads' persistent minute-long soft-throttle. With fourteen
	// independent editions running, there's no need for the one that stalled to sit out nearly
	// as long — `abandonExperimentalPace` already provides the lasting protection (permanently
	// widening *that* source's ongoing pace the instant a real 429 is seen); this penalty is
	// only about the timing of the very next retry. Bumped 10s → 15s the same session — a little
	// more margin against getting rate-limited again on the very next attempt.
	wikiquoteStallPenalty = 15 * time.Second

	// wikiquoteDeadStreakThreshold: after this many consecutive Wikiquote pages in a row with
	// 0 quotes, the current discovery category is treated as low-yield and skipped early
	// rather than ground through to its end. Categories aren't perfectly pure — some
	// non-biographical pages (year articles, topic pages like "Black people") are members
	// too, confirmed live — so this is a second layer of defense on top of category-based
	// discovery, not a replacement for it.
	wikiquoteDeadStreakThreshold = 5

	// wikiquoteEmptyStreakBeforeAllPages: after this many consecutive category-walk calls in a
	// row add zero new URLs (the curated walk, including its recursive subcategory descent, has
	// nothing new right then), discovery switches to walking the edition's full article list
	// (list=allpages) until that pass ends — see wikiquoteTopUpFunc. Kept low (it used to be 10)
	// because the smaller localized editions exhaust their few curated categories quickly —
	// confirmed live, French once had a 94-minute gap with zero new quotes.
	wikiquoteEmptyStreakBeforeAllPages = 2

	// goodreadsEmptyStreakBeforeRandom: after this many consecutive tag-cycling attempts in a row add nothing new (every tag in
	// goodreadsTags already seeded at least once — see topUpGoodreadsTag), fall back to
	// topUpGoodreadsRandomPages once. Goodreads' curated tag list is much shorter than any
	// Wikiquote edition's category list (19 tags vs. tens of categories), so once it's dry it
	// stays dry every single call from then on, not just occasionally — there's no need to wait
	// out a long streak to be confident that's really what's happening.
	goodreadsEmptyStreakBeforeRandom = 3

	// wikiquoteRandomModeSpan: once a full allpages pass over an edition queues nothing new, its
	// feeder switches to list=random batches (see wikiquoteTopUpFunc) instead of pausing, and
	// stays there this long before restarting the category walk and the next allpages pass.
	// Requested explicitly over the earlier 6h pause ("go aggressive ... with /random").
	wikiquoteRandomModeSpan = 1 * time.Hour

	// goodreadsRandomPageMax bounds which page of Goodreads' own /quotes "popular quotes" listing
	// topUpGoodreadsRandomPages picks from — confirmed live the listing paginates to page 100
	// (its own next_page chain stops there), and that the exact same div.quote/quoteText/
	// authorOrTitle/tags-footer markup GoodreadsParser already handles for tag and author pages
	// renders here too, unchanged. This is Goodreads' answer to "what happens once even the
	// organically-growing tag graph and the curated fallback list are both fully known": a
	// broad, non-curated batch of ~30 quotes and ~90 tag/author links per page, cutting across
	// whatever's currently popular site-wide rather than the ever-shrinking curated list.
	goodreadsRandomPageMax = 100

	// maxURLRetries bounds how many total attempts a URL gets on a fetch failure before it's
	// marked permanently failed. Previously a single failed fetch (a transient network blip, a
	// momentary 5xx) meant that URL was gone from the crawl forever — no second chance, ever.
	// Only applied to fetch failures, not parse failures or an unknown source: those are
	// near-deterministic given the same page content, so retrying them would just fail the
	// same way again — a fetch failure is the one case that's plausibly transient.
	maxURLRetries = 3

	// wikiquoteExperimentalDelay is a deliberately narrower per-source delay than the proven-safe
	// minSourceDelayBySource entry (6s) — requested explicitly: with fourteen independent
	// Wikiquote editions each running their own goroutine against their own subdomain, any one
	// of them getting rate-limited just means that one source alone permanently widens back to
	// its safe pace for the rest of the run (see Crawler.abandonExperimentalPace) while the
	// other thirteen keep going unaffected — there's no reason for every source to sit at the
	// same cautious pace a single-stream world needed. Every Wikiquote-family worker starts here,
	// not just EN/DE (the original two this was first tried on). Tried 2s briefly at the user's
	// request to be more aggressive, reverted to 4s the same session, then settled on 5s per the
	// user's own live observation of fewer rate-limit hits at that specific pace. Automatically
	// and permanently abandoned per-source the instant a real 429 is observed at this pace — not
	// something a human needs to be watching logs to catch.
	wikiquoteExperimentalDelay = 5 * time.Second

	// goodreadsExperimentalDelay: Goodreads is the highest-quality source and never runs out of
	// content, so it starts at half its proven-safe 20s pace — requested explicitly ("goodreads
	// should be first class priority"). Goodreads throttles by tarpitting (a slow 200 after
	// ~60s), not with 429s, so the worker drops back to the safe pace on the first stalled fetch
	// of any kind, not only on a 429 (see runWorker/abandonExperimentalPace).
	goodreadsExperimentalDelay = 10 * time.Second
)

// minSourceDelayBySource lets sources that have actually shown throttling be treated more
// cautiously than ones that haven't, rather than applying one flat delay to everything.
// Goodreads is not just marginally more sensitive: confirmed directly (see README) it
// soft-throttles (~60s stalls, still 200 OK) roughly every 10-13 requests regardless of
// pacing, so it gets a meaningfully longer cooldown here — the crawler being polite about
// this shouldn't be diluted by pooling it with Wikiquote's much better-behaved profile
// (only 429s under a genuine zero-delay burst, never throttled at any pace that's been
// tested here). German Wikiquote gets the same treatment as English (same MediaWiki
// software, no throttling observed) — no evidence yet that it needs different treatment.
var minSourceDelayBySource = map[string]time.Duration{
	scoring.SourceGoodreads:   20 * time.Second,
	scoring.SourceWikiquoteEN: 6 * time.Second,
	scoring.SourceWikiquoteDE: 6 * time.Second,
	scoring.SourceWikiquoteFR: 6 * time.Second,
	scoring.SourceWikiquoteES: 6 * time.Second,
	scoring.SourceWikiquoteIT: 6 * time.Second,
	scoring.SourceWikiquotePT: 6 * time.Second,
	scoring.SourceWikiquotePL: 6 * time.Second,
	scoring.SourceWikiquoteSV: 6 * time.Second,
	scoring.SourceWikiquoteRO: 6 * time.Second,
	scoring.SourceWikiquoteCS: 6 * time.Second,
	scoring.SourceWikiquoteHU: 6 * time.Second,
	scoring.SourceWikiquoteDA: 6 * time.Second,
	scoring.SourceWikiquoteNO: 6 * time.Second,
	scoring.SourceWikiquoteFI: 6 * time.Second,
}

func minSourceDelayFor(source string) time.Duration {
	if d, ok := minSourceDelayBySource[source]; ok {
		return d
	}
	return defaultMinSourceDelay
}

// stallPenaltyFor returns goodreadsStallPenalty for Goodreads, wikiquoteStallPenalty for every
// Wikiquote edition — see those constants' doc comments for why the two differ.
func stallPenaltyFor(source string) time.Duration {
	if source == scoring.SourceGoodreads {
		return goodreadsStallPenalty
	}
	return jitter(wikiquoteStallPenalty, 0.5)
}

// stallPenaltyDisplayFor returns the *nominal*, unjittered value for log messages — calling
// stallPenaltyFor twice for one event (once to log it, once to actually apply it) would print a
// different random number than the one really used, since each call draws its own jitter.
func stallPenaltyDisplayFor(source string) string {
	if source == scoring.SourceGoodreads {
		return goodreadsStallPenalty.String()
	}
	return wikiquoteStallPenalty.String() + " (jittered)"
}

// jitter returns d randomly varied by up to ±fraction (e.g. 0.5 → anywhere from 0.5x to 1.5x
// d). Added after live evidence that 12 of 14 independent Wikiquote workers, all sharing the
// same fixed wikiquoteStallPenalty, were retrying in near lockstep against what's almost
// certainly a single rate limit shared across the whole wikiquote.org family — a 429 on one
// tends to mean several others are about to get one too, and a fixed retry delay means they all
// come back for another try at the same moment, repeatedly. Only applied to the Wikiquote stall
// penalty, not Goodreads (a single stream — there's no herd to desynchronize) and not the base
// per-request pace (jittering *that* would just make individual sources slower on average for
// no benefit; the goal here is spreading retries apart, not slowing down the normal case).
func jitter(d time.Duration, fraction float64) time.Duration {
	delta := (rand.Float64()*2 - 1) * fraction
	return time.Duration(float64(d) * (1 + delta))
}

// sleepCtx sleeps for d, or returns early the moment ctx is cancelled — used everywhere
// runWorker would otherwise call time.Sleep, so a graceful shutdown signal doesn't have to wait
// out a full cooldown, stall penalty (up to 60s), or idle poll before actually stopping.
// Returns true if the context was what ended the sleep (caller should stop), false if the full
// duration elapsed normally.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() != nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return false
	case <-ctx.Done():
		return true
	}
}

// Log level thresholds, lowest (most restrictive) to highest (most verbose) — see
// configuredLogLevel/SetLogLevel. A message at level L is shown iff L <= configuredLogLevel, so
// "warn" shows both warnings and errors, "info" (the default) shows everything.
const (
	logLevelError = iota
	logLevelWarn
	logLevelInfo
)

// configuredLogLevel defaults to logLevelInfo — full output, exactly today's behavior — so local
// dev/tests are unaffected unless a deployment explicitly opts into a quieter level via
// SetLogLevel/LOG_LEVEL. A package-level var, not a Crawler field: every runWorker goroutine
// reads it, and it's only ever written once, before Run starts.
var configuredLogLevel = logLevelInfo

// SetLogLevel parses "error", "warn" (or "warning"), or "info" (case-insensitive; "" also means
// info, i.e. unset) and applies it — call once before Crawler.Run, from main.go based on
// LOG_LEVEL. Returns an error for anything else so a typo'd env var fails loudly at startup
// instead of silently doing nothing.
func SetLogLevel(level string) error {
	switch strings.ToLower(level) {
	case "", "info":
		configuredLogLevel = logLevelInfo
	case "warn", "warning":
		configuredLogLevel = logLevelWarn
	case "error":
		configuredLogLevel = logLevelError
	default:
		return fmt.Errorf("unknown log level %q (want error, warn, or info)", level)
	}
	return nil
}

// logWarn is log.Printf for a warning: the crawler tried something and skipped/backed off/gave
// up on it (a rejected quote, a low-yield category abandoned, a retry after a transient
// failure) — not a hard error, but the kind of thing an operator watching a quieted-down
// production log still wants to see. Suppressed only at logLevelError.
func logWarn(format string, args ...interface{}) {
	if configuredLogLevel < logLevelWarn {
		return
	}
	log.Printf(format, args...)
}

// logInfo is log.Printf for routine, high-volume narration only — one line per page
// fetched/quote saved/URL discovered, the kind of output that dominates log volume over a long
// run (see PRODUCTION.md). Suppressed at both logLevelWarn and logLevelError. Never used for an
// error, a warning, a stall, a rate-limit, or anything else that should survive a quieted-down
// production log — those all call log.Printf directly, unconditionally, elsewhere in this file.
func logInfo(format string, args ...interface{}) {
	if configuredLogLevel < logLevelInfo {
		return
	}
	log.Printf(format, args...)
}

// quoteMilestoneInterval controls how often the running total gets logged — every 1000 net-new
// quotes actually saved (duplicates don't count; see recordQuotesSaved).
const quoteMilestoneInterval = 1000

// summaryMilestoneInterval controls how often the source/language breakdown gets logged —
// every 10,000 net-new quotes, a coarser cadence than quoteMilestoneInterval since it costs two
// real GROUP BY queries (see Store.QuoteCountsBySourceAndLanguage) rather than an in-memory
// counter check.
const summaryMilestoneInterval = 10000

// totalQuoteCount is the live running total of quotes in the database, seeded once at startup
// from Store.CountQuotes (see Run) and kept current by recordQuotesSaved after every successful
// save from then on. A package-level atomic, not a Crawler field: every source's worker
// goroutine calls processURL concurrently (see Run spawning one goroutine per source), and an
// atomic add is exactly what's needed to keep a shared counter correct under that without a
// mutex.
var totalQuoteCount atomic.Int64

// recordQuotesSaved adds n newly-saved quotes to the running total and logs every
// quoteMilestoneInterval boundary the addition crosses — one log line per boundary, so a single
// large batch that jumps from, say, 1997 to 2004 still logs the 2000 milestone rather than
// silently skipping over it. Logged with a plain, unconditional log.Printf (like every other
// message in this file meant to survive a quieted-down production log) specifically so this
// still shows up even at LOG_LEVEL=error — a milestone isn't a warning or an error, but it's the
// one piece of "is this thing actually working" narration worth keeping at every log level.
//
// Also logs a source/language breakdown every summaryMilestoneInterval boundary crossed, same
// "every boundary, not just the last one" loop shape as the plain milestone above. A method on
// Crawler (not the package-level function this used to be), since the breakdown needs c.store —
// unlike the plain milestone, it can't be answered from the in-memory counter alone.
func (c *Crawler) recordQuotesSaved(ctx context.Context, n int) {
	if n <= 0 {
		return
	}
	after := totalQuoteCount.Add(int64(n))
	before := after - int64(n)

	first := (before/quoteMilestoneInterval + 1) * quoteMilestoneInterval
	for m := first; m <= after; m += quoteMilestoneInterval {
		log.Printf("Milestone: %s total quotes scraped so far", formatCount(m))
	}

	firstSummary := (before/summaryMilestoneInterval + 1) * summaryMilestoneInterval
	for m := firstSummary; m <= after; m += summaryMilestoneInterval {
		c.logQuoteSummary(ctx, fmt.Sprintf("at the %s milestone", formatCount(m)))
	}
}

// logQuoteSummary queries and logs the full corpus's breakdown by source and by language, for
// the summaryMilestoneInterval milestone atTotal — a point-in-time count straight from Postgres,
// not derived from the in-memory counter, so it's correct even though multiple sources' workers
// are saving concurrently and the real total may have already moved past atTotal by the time
// this query runs. A query failure is logged and otherwise ignored — missing one summary isn't
// worth treating as a reason to stop the crawl. label describes where this summary is coming
// from ("at startup", "at the 20,000 milestone", ...) since this is called both once up front
// from Run and repeatedly from recordQuotesSaved, and the two need visibly different log lines.
func (c *Crawler) logQuoteSummary(ctx context.Context, label string) {
	bySource, byLanguage, err := c.store.QuoteCountsBySourceAndLanguage(ctx)
	if err != nil {
		log.Printf("Could not load quote summary %s: %s\n", label, err)
		return
	}
	log.Printf("Summary %s:\n  by source:%s\n  by language:%s",
		label, formatCountsTable(bySource), formatCountsTable(byLanguage))
}

// countRow is one label/count pair, broken out as its own type purely so sortCountRows has
// something concrete to sort — map iteration order is random in Go, and the whole point here is
// a stable, biggest-first row order.
type countRow struct {
	label string
	count int64
}

// sortCountRows orders rows by count descending (the biggest sources/languages first — almost
// always what's actually worth scanning for first), falling back to the label alphabetically to
// break ties deterministically rather than leaving equal-count rows in whatever order map
// iteration happened to produce.
func sortCountRows(rows []countRow) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].count != rows[j].count {
			return rows[i].count > rows[j].count
		}
		return rows[i].label < rows[j].label
	})
}

// formatCountsTable renders a label->count map as a simple fixed-width table, one row per
// line, biggest count first (see sortCountRows) — e.g.:
//
//	goodreads      1,234,567
//	wikiquote_en     987,654
//	wikiquote_de     102,345
//
// Both columns are padded to the widest entry so every row lines up, which a flat
// comma-joined "a=1,234, b=5,678" line (the previous format) can't do at a glance once there
// are more than two or three sources. Each row is prefixed with its own "\n    " here, not
// joined with "\n" afterward, so the caller's surrounding log line (which already supplies the
// line break and section heading before the first row) doesn't end up with one format for the
// first row and another for the rest.
func formatCountsTable(counts map[string]int64) string {
	rows := make([]countRow, 0, len(counts))
	for label, count := range counts {
		rows = append(rows, countRow{label, count})
	}
	sortCountRows(rows)

	labelWidth := 0
	countStrs := make([]string, len(rows))
	countWidth := 0
	for i, r := range rows {
		if len(r.label) > labelWidth {
			labelWidth = len(r.label)
		}
		countStrs[i] = formatCount(r.count)
		if len(countStrs[i]) > countWidth {
			countWidth = len(countStrs[i])
		}
	}

	var b strings.Builder
	for i, r := range rows {
		fmt.Fprintf(&b, "\n    %-*s  %*s", labelWidth, r.label, countWidth, countStrs[i])
	}
	return b.String()
}

// formatCount renders n with thousands separators (1234567 -> "1,234,567"). Every count this
// package logs is meant to be read by a human watching logs scroll by live, and a bare run of
// 6-7 digits is noticeably harder to parse at a glance than the same number grouped — worth a
// few lines of manual grouping rather than pulling in golang.org/x/text/message/number (already
// an indirect dependency, but only transitively — promoting it to a direct one for comma
// formatting alone would be a disproportionate addition) just for this.
func formatCount(n int64) string {
	neg := n < 0
	s := strconv.FormatInt(n, 10)
	if neg {
		s = s[1:]
	}

	var groups []string
	for len(s) > 3 {
		groups = append([]string{s[len(s)-3:]}, groups...)
		s = s[:len(s)-3]
	}
	groups = append([]string{s}, groups...)

	out := strings.Join(groups, ",")
	if neg {
		out = "-" + out
	}
	return out
}

// topUpResult is what a source's top-up function reports back to runFeeder. calledNetwork
// distinguishes a real HTTP request to the source (Wikiquote's MediaWiki API calls) from a
// pure DB/Redis operation (Goodreads' seeding, which never calls goodreads.com itself) — only
// the former is paced by the feeder's delay. Found live, back when discovery ran inside the
// fetch loop: consecutive Wikiquote discovery calls (walking past several already-fully-known
// categories, each a fast "0 new URLs" response) were separated only by a flat 2s poll, not a
// real cooldown, and hit a 429 from Wikiquote's API as a direct result.
type topUpResult struct {
	added         bool
	calledNetwork bool
	networkFailed bool
	rateLimited   bool // specifically a 429, not just any networkFailed — see fetcher.IsRateLimited
}

type Crawler struct {
	store *db.Store
}

func New(store *db.Store) *Crawler {
	return &Crawler{store: store}
}

// recordWikiquoteYield tracks consecutive zero-quote fetches for one Wikiquote edition.
// Pure — takes the caller's current streak and returns the updated one — rather than a field
// on Crawler, because each edition now runs on its own dedicated worker goroutine (see
// runWorker) with its own local streak variable; there is no shared state left to keep in
// sync, and a shared map here would just reintroduce a data race across goroutines for no
// benefit. After wikiquoteDeadStreakThreshold in a row, it treats the current discovery
// category as low-yield and force-advances that edition's cursor to the next one — otherwise
// a category with a long stretch of non-biographical members (e.g. a topic page like "Black
// people" sitting inside Category:People, or worse, leftover pending URLs from before this
// category-based approach existed) would keep getting ground through one cooldown-interval
// fetch at a time for no benefit. Only the *discovery* cursor moves; URLs already sitting in
// the pending queue from the current low-yield batch still get fetched — there's no per-batch
// tag on frontier rows to selectively drop them, so this only prevents drawing *more* of the
// same bad category next time, not clearing what's already queued.
func (c *Crawler) recordWikiquoteYield(ctx context.Context, site wikiquoteSite, deadStreak int, gotQuotes bool) int {
	if gotQuotes {
		return 0
	}
	deadStreak++
	if deadStreak < wikiquoteDeadStreakThreshold {
		return deadStreak
	}

	raw, err := c.store.GetDiscoveryCursor(ctx, site.source)
	if err != nil {
		log.Printf("Could not load %s discovery cursor to skip a low-yield category: %s\n", site.source, err)
		return 0
	}
	cursor := decodeWikiquoteCursor(raw)
	// Same out-of-range guard topUpWikiquoteCategories already applies when *reading* a
	// cursor — needed here too, on the *write* path, or a run of dead-streak skips can push
	// CategoryIndex arbitrarily far past the curated list's length before the next top-up call
	// happens to catch and reset it. Purely cosmetic in the meantime (topUp's own guard means
	// nothing actually gets stuck), but it did make this log line print a useless "?" instead
	// of ever wrapping back to a real category name — found live, every one of those "?"s
	// traced back to exactly this.
	if cursor.CategoryIndex >= len(site.curatedCategories) {
		cursor = wikiquoteDiscoveryCursor{}
	}
	skipped := cursor.Current
	if skipped == "" && cursor.CategoryIndex < len(site.curatedCategories) {
		skipped = site.curatedCategories[cursor.CategoryIndex]
	}
	if skipped == "" {
		skipped = "?"
	}
	// Abandon the whole top-level branch, not just wherever we happened to be within it
	// (possibly deep in a subcategory) — a persistently low-yield area isn't worth resuming
	// elsewhere inside the same branch, only worth escaping entirely.
	next := wikiquoteDiscoveryCursor{CategoryIndex: cursor.CategoryIndex + 1}
	if err := c.store.SetDiscoveryCursor(ctx, site.source, next.encode()); err != nil {
		log.Printf("Could not save %s discovery cursor after skipping a low-yield category: %s\n", site.source, err)
		return 0
	}
	logWarn("%s: %d consecutive pages with 0 quotes — skipping the rest of category %q for next time", site.source, wikiquoteDeadStreakThreshold, skipped)
	return 0
}

// topUpWikiquoteCategories fetches the next batch of one of the edition's curated categories via
// the MediaWiki API and queues it. The feeder (see feeder.go) decides when that's needed. This is what keeps a Wikiquote
// edition from running dry mid-session — the fixed stopgap seed list (English only) only
// lasts ~20 pages, and SeedFrontier only runs once at startup, so without this the crawler
// would just sit idle once that list was consumed, even with real content still available to
// discover. German has no stopgap seed list at all — it bootstraps entirely from this the
// first time it's idle, the same as Goodreads' tag fallback does.
//
// Categories over blind alphabetical enumeration: filters out the large fraction of Wikiquote
// articles that are never going to have quotes (movies, TV shows, books, historical events)
// by only walking pages Wikiquote itself has tagged under an occupation — confirmed live to
// meaningfully reduce "0 quotes, 0 discovered URLs" dead-end fetches.
//
// Once every curated category is fully walked, the cursor wraps back to the first one and
// starts over — Wikiquote gains new articles over time, so a full pass eventually finds fresh
// content again; in the meantime it's a cheap no-op (ON CONFLICT DO NOTHING skips everything
// already known, so `added` stays 0 and the caller's dead-end fallback — see
// topUpGoodreadsTag — takes over instead of this looping forever on stale data).
func (c *Crawler) topUpWikiquoteCategories(ctx context.Context, site wikiquoteSite) topUpResult {
	raw, err := c.store.GetDiscoveryCursor(ctx, site.source)
	if err != nil {
		log.Printf("Could not load %s discovery cursor: %s\n", site.source, err)
		return topUpResult{}
	}
	cursor := decodeWikiquoteCursor(raw)
	if cursor.CategoryIndex >= len(site.curatedCategories) {
		logInfo("%s discovery: finished all %d curated categories (and their subcategories), restarting from the beginning", site.source, len(site.curatedCategories))
		cursor = wikiquoteDiscoveryCursor{}
	}

	category := cursor.Current
	if category == "" {
		category = site.curatedCategories[cursor.CategoryIndex]
	}

	// One call returns both category's own member pages and its direct subcategories — see
	// fetchWikiquoteCategoryMembers for why this is one request, not two.
	pages, subcats, nextCMContinue, err := fetchWikiquoteCategoryMembers(ctx, site, category, cursor.CMContinue)
	if err != nil {
		log.Printf("%s title discovery failed (category=%q): %s — backing off ~%s (jittered)\n", site.source, category, err, wikiquoteStallPenalty)
		return topUpResult{calledNetwork: true, networkFailed: true, rateLimited: fetcher.IsRateLimited(err)}
	}

	visited := make(map[string]bool, len(cursor.Visited))
	for _, v := range cursor.Visited {
		visited[v] = true
	}
	queue := append([]string{}, cursor.Queue...)
	var newlyQueued []string
	for _, sc := range subcats {
		if visited[sc] {
			continue
		}
		visited[sc] = true
		queue = append(queue, sc)
		newlyQueued = append(newlyQueued, sc)
	}
	if len(newlyQueued) > 0 {
		logInfo("%s discovery: category %q has %d new subcategory(ies) queued to explore (e.g. %q)", site.source, category, len(newlyQueued), newlyQueued[0])
	}
	updatedVisited := make([]string, 0, len(visited))
	for v := range visited {
		updatedVisited = append(updatedVisited, v)
	}

	var next wikiquoteDiscoveryCursor
	switch {
	case nextCMContinue != "":
		// Category still paginating — stay on it, carrying the (possibly just-grown) queue
		// forward so subcategories found on this page aren't lost before the category finishes.
		next = wikiquoteDiscoveryCursor{
			CategoryIndex: cursor.CategoryIndex,
			Current:       category,
			CMContinue:    nextCMContinue,
			Queue:         queue,
			Visited:       updatedVisited,
		}
	case len(queue) > 0:
		// This category (both its pages and its own subcategories) is fully known — descend
		// into the next queued subcategory instead of moving to the next top-level category,
		// which is what turns a fixed curated-category list into real coverage of Wikiquote's
		// actual category tree. Confirmed live: Category:Writers, already fully mined at the
		// top level, has real, on-topic subcategories like "Gay writers" and "Writers by
		// language" that a flat list never reaches.
		next = wikiquoteDiscoveryCursor{
			CategoryIndex: cursor.CategoryIndex,
			Current:       queue[0],
			Queue:         queue[1:],
			Visited:       updatedVisited,
		}
	default:
		// Nothing left anywhere in this top-level category's subtree — move on.
		logInfo("%s discovery: category %q and all its subcategories are fully explored, moving to the next curated category", site.source, category)
		next = wikiquoteDiscoveryCursor{CategoryIndex: cursor.CategoryIndex + 1}
	}
	if err := c.store.SetDiscoveryCursor(ctx, site.source, next.encode()); err != nil {
		log.Printf("Could not save %s discovery cursor: %s\n", site.source, err)
	}

	if len(pages) == 0 {
		logInfo("%s discovery: category %q returned no member pages (%d subcategories queued)", site.source, category, len(newlyQueued))
		return topUpResult{calledNetwork: true}
	}

	priority := scoring.CalculatePriority(site.source, 0, 0)
	urls := make([]string, len(pages))
	for i, title := range pages {
		urls[i] = wikiquoteTitleToURL(site, title)
	}

	// One batched INSERT + one batched ZADD for up to 500 URLs, instead of 500 of each awaited
	// one at a time — the discovery batch size is exactly the case that made the old per-title
	// loop expensive (~1000 sequential round-trips per top-up).
	inserted, err := c.store.SaveURLsBatch(ctx, urls, site.source, priority, 0)
	if err != nil {
		log.Printf("Could not save discovered %s URLs: %s\n", site.source, err)
		return topUpResult{calledNetwork: true}
	}
	if len(inserted) == 0 {
		logInfo("%s discovery: category %q — fetched %d pages, added 0 new URLs to the frontier (all already known)", site.source, category, len(pages))
		return topUpResult{calledNetwork: true}
	}
	if err := c.store.PushURLsBatch(ctx, site.source, inserted, priority); err != nil {
		log.Printf("Could not push discovered %s URLs to frontier: %s\n", site.source, err)
		return topUpResult{calledNetwork: true}
	}
	for _, u := range inserted {
		logInfo("Discovered URL [%s]: %s (via category %q)", site.source, u, category)
	}

	logInfo("%s discovery: category %q — fetched %d pages, added %d new URLs to the frontier", site.source, category, len(pages), len(inserted))
	return topUpResult{added: true, calledNetwork: true}
}

// topUpWikiquoteRandomPages queues a batch of random article titles (fetchWikiquoteRandomPages).
// It's the feeder's mode once a full allpages pass found nothing new: it picks up articles
// created since that pass, and any a pass missed. One request per call, same as every topUp.
func (c *Crawler) topUpWikiquoteRandomPages(ctx context.Context, site wikiquoteSite) topUpResult {
	titles, err := fetchWikiquoteRandomPages(ctx, site)
	if err != nil {
		log.Printf("%s random-page discovery failed: %s — backing off ~%s (jittered)\n", site.source, err, wikiquoteStallPenalty)
		return topUpResult{calledNetwork: true, networkFailed: true, rateLimited: fetcher.IsRateLimited(err)}
	}
	if len(titles) == 0 {
		return topUpResult{calledNetwork: true}
	}
	priority := scoring.CalculatePriority(site.source, 0, 0)
	urls := make([]string, len(titles))
	for i, title := range titles {
		urls[i] = wikiquoteTitleToURL(site, title)
	}
	inserted, err := c.store.SaveURLsBatch(ctx, urls, site.source, priority, 0)
	if err != nil {
		log.Printf("Could not save random %s URLs: %s\n", site.source, err)
		return topUpResult{calledNetwork: true}
	}
	if len(inserted) == 0 {
		logInfo("%s random: %d titles, all already known", site.source, len(titles))
		return topUpResult{calledNetwork: true}
	}
	if err := c.store.PushURLsBatch(ctx, site.source, inserted, priority); err != nil {
		log.Printf("Could not push random %s URLs to frontier: %s\n", site.source, err)
		return topUpResult{calledNetwork: true}
	}
	log.Printf("%s random: %d titles, %d new URLs queued", site.source, len(titles), len(inserted))
	return topUpResult{added: true, calledNetwork: true}
}

// wikiquoteAllPagesCursorKey is where an edition's allpages continuation token is kept, apart
// from its category-walk cursor (stored under the bare source name).
func wikiquoteAllPagesCursorKey(site wikiquoteSite) string {
	return site.source + ":allpages"
}

// topUpWikiquoteAllPages queues the next batch of the edition's full article list
// (fetchWikiquoteAllPages), resuming from the stored token. passDone reports that this batch was
// the last one; the token is then cleared, so the next pass starts again from the first title.
// One request per call, same as every other topUp.
func (c *Crawler) topUpWikiquoteAllPages(ctx context.Context, site wikiquoteSite) (result topUpResult, passDone bool) {
	key := wikiquoteAllPagesCursorKey(site)
	apContinue, err := c.store.GetDiscoveryCursor(ctx, key)
	if err != nil {
		log.Printf("Could not load %s allpages cursor: %s\n", site.source, err)
		return topUpResult{}, false
	}

	titles, next, err := fetchWikiquoteAllPages(ctx, site, apContinue)
	if err != nil {
		log.Printf("%s allpages discovery failed: %s — backing off ~%s (jittered)\n", site.source, err, wikiquoteStallPenalty)
		return topUpResult{calledNetwork: true, networkFailed: true, rateLimited: fetcher.IsRateLimited(err)}, false
	}
	if err := c.store.SetDiscoveryCursor(ctx, key, next); err != nil {
		log.Printf("Could not save %s allpages cursor: %s\n", site.source, err)
	}
	passDone = next == ""

	if len(titles) == 0 {
		return topUpResult{calledNetwork: true}, passDone
	}
	priority := scoring.CalculatePriority(site.source, 0, 0)
	urls := make([]string, len(titles))
	for i, title := range titles {
		urls[i] = wikiquoteTitleToURL(site, title)
	}
	inserted, err := c.store.SaveURLsBatch(ctx, urls, site.source, priority, 0)
	if err != nil {
		log.Printf("Could not save %s allpages URLs: %s\n", site.source, err)
		return topUpResult{calledNetwork: true}, passDone
	}
	if len(inserted) == 0 {
		logInfo("%s allpages: %d titles from %q, all already known", site.source, len(titles), titles[0])
		return topUpResult{calledNetwork: true}, passDone
	}
	if err := c.store.PushURLsBatch(ctx, site.source, inserted, priority); err != nil {
		log.Printf("Could not push %s allpages URLs to frontier: %s\n", site.source, err)
		return topUpResult{calledNetwork: true}, passDone
	}
	log.Printf("%s allpages: %d titles from %q, %d new URLs queued", site.source, len(titles), titles[0], len(inserted))
	return topUpResult{added: true, calledNetwork: true}, passDone
}

// goodreadsTags is a curated fallback list of additional Goodreads quote tags, used only once
// the single seeded tag ("inspirational") genuinely runs dry — its pagination is finite (its
// own "next" link disappears on the last page), unlike Wikiquote's much larger discoverable
// corpus. Confirmed live to exist and return real quote content during source research.
var goodreadsTags = []string{
	"inspirational", "life", "love", "wisdom", "happiness", "success", "philosophy",
	"motivational", "truth", "humor", "knowledge", "friendship", "books", "writing",
	"courage", "hope", "faith", "dreams", "change",
}

// topUpGoodreadsTag seeds the next tag from goodreadsTags (cycling back to the start once
// exhausted — a cheap no-op via ON CONFLICT DO NOTHING if that tag's pagination was already fully
// crawled). One of the Goodreads feeder's fallbacks, see goodreadsDiscovery.topUp.
// It never sets calledNetwork on its topUpResult: it only touches Postgres/Redis (seeding a tag
// URL for the worker to fetch later), never goodreads.com itself.
func (c *Crawler) topUpGoodreadsTag(ctx context.Context) topUpResult {
	idx := 0
	if raw, err := c.store.GetDiscoveryCursor(ctx, scoring.SourceGoodreads); err == nil && raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			idx = n % len(goodreadsTags)
		}
	}
	tag := goodreadsTags[idx]
	if err := c.store.SetDiscoveryCursor(ctx, scoring.SourceGoodreads, strconv.Itoa((idx+1)%len(goodreadsTags))); err != nil {
		log.Printf("Could not save Goodreads discovery cursor: %s\n", err)
	}

	tagURL := "https://www.goodreads.com/quotes/tag/" + tag
	priority := scoring.GoodreadsPriority(tagURL, 0)
	frontier := models.URLFrontier{URL: tagURL, Source: scoring.SourceGoodreads, Priority: priority}
	inserted, err := c.store.SaveURL(ctx, frontier)
	if err != nil {
		log.Printf("Could not save Goodreads tag seed: %s\n", err)
		return topUpResult{}
	}
	if !inserted {
		logInfo("Goodreads tag %q already fully known; will try the next one on the next dead end", tag)
		return topUpResult{}
	}
	if err := c.store.PushURL(ctx, scoring.SourceGoodreads, tagURL, priority); err != nil {
		log.Printf("Could not push Goodreads tag seed to frontier: %s\n", err)
		return topUpResult{}
	}
	logInfo("Discovered URL [goodreads]: %s (new tag seeded — the previous one had run dry)", tagURL)
	return topUpResult{added: true}
}

// topUpGoodreadsRandomPages is Goodreads' equivalent of topUpWikiquoteRandomPages: the dead-end
// fallback once even topUpGoodreadsTag's tag cycling stops finding anything new (every tag
// in goodreadsTags already seeded — see goodreadsEmptyStreakBeforeRandom). Rather than a
// MediaWiki list=random call (Goodreads has no equivalent API), this seeds a random page of
// https://www.goodreads.com/quotes — the site's own "popular quotes" listing, which paginates to
// goodreadsRandomPageMax and renders each quote with the exact same markup GoodreadsParser
// already parses for tag/author pages (confirmed live), so no parser changes are needed: the
// normal fetch loop picks this URL up like any other Goodreads URL, and its own NextURLs (tags
// and author links, same as any other page) feed straight back into the organic discovery this
// is backing up. Like topUpGoodreadsTag, this never touches goodreads.com itself (only
// Postgres/Redis via SaveURL/PushURL), so topUpResult.calledNetwork stays false here too.
func (c *Crawler) topUpGoodreadsRandomPages(ctx context.Context) topUpResult {
	page := rand.Intn(goodreadsRandomPageMax) + 1
	pageURL := "https://www.goodreads.com/quotes"
	if page > 1 {
		pageURL = fmt.Sprintf("https://www.goodreads.com/quotes?page=%d", page)
	}

	priority := scoring.GoodreadsPriority(pageURL, 0)
	frontier := models.URLFrontier{URL: pageURL, Source: scoring.SourceGoodreads, Priority: priority}
	inserted, err := c.store.SaveURL(ctx, frontier)
	if err != nil {
		log.Printf("Could not save Goodreads random-page seed: %s\n", err)
		return topUpResult{}
	}
	if !inserted {
		logInfo("Goodreads random page %d already known; will try another on the next dead end", page)
		return topUpResult{}
	}
	if err := c.store.PushURL(ctx, scoring.SourceGoodreads, pageURL, priority); err != nil {
		log.Printf("Could not push Goodreads random-page seed to frontier: %s\n", err)
		return topUpResult{}
	}
	logInfo("Discovered URL [goodreads]: %s (random popular-quotes page — tag cycling had run dry)", pageURL)
	return topUpResult{added: true}
}

// hardFailureRetryDelay is how long an outright-failed URL (a plain non-200 status or network
// error — NOT the slow-but-succeeded tarpit signature, and NOT a 429, both of which are
// signals of a source-wide problem and still force the whole worker to back off via
// stallPenaltyFor — see processURL) waits before it's eligible to be fetched again. Requested
// explicitly: a one-off failure on a specific page (e.g. a transient 503) shouldn't stop the
// worker from immediately continuing with everything else that's actually ready; it should just
// come back to that one page later, once it's had a real chance to have resolved itself.
const hardFailureRetryDelay = 10 * time.Minute

// failOrRetry handles a fetch failure for row: if it hasn't yet used up maxURLRetries attempts,
// it's requeued to pending with an incremented error_count and a correspondingly lower priority
// (scoring.CalculatePriority's existing errorCount term means a retried URL sinks below fresh
// ones in its own source's queue, rather than competing evenly with them). Once retries are
// exhausted, it's marked permanently failed via the existing MarkURLFailed, same as before this
// existed.
//
// delay controls *when* the retry becomes eligible: zero means immediately (pushed straight
// onto the live frontier, same as this always worked), non-zero schedules it via
// Store.ScheduleDelayedRetry instead — held in a separate per-source "delayed" set and only
// promoted onto the live frontier once delay has passed (see Store.PromoteDueRetries, polled
// from runWorker). The lower-priority score alone isn't a real guarantee of a delay: if a
// source's queue is otherwise thin (true of Goodreads much of the time — a single pagination
// chain plus occasional tag/random-page seeds), a merely-deprioritized URL could still be the
// only thing available and get popped again almost immediately. A real time-based hold is what
// hardFailureRetryDelay actually needs.
func (c *Crawler) failOrRetry(ctx context.Context, source string, row models.URLFrontier, delay time.Duration) {
	nextErrorCount := row.ErrorCount + 1
	if nextErrorCount >= maxURLRetries {
		if err := c.store.MarkURLFailed(ctx, row.URL); err != nil {
			log.Printf("[%s] Could not mark the URL failed: %s\n", source, err)
		}
		log.Printf("[%s] Giving up on %s after %d attempts", source, row.URL, nextErrorCount)
		return
	}

	priority := scoring.PriorityFor(source, row.URL, int(row.Depth), int(nextErrorCount))

	if delay <= 0 {
		if err := c.store.RetryURL(ctx, row.URL, nextErrorCount, priority); err != nil {
			log.Printf("[%s] Could not requeue %s for retry: %s\n", source, row.URL, err)
			return
		}
		if err := c.store.PushURL(ctx, source, row.URL, priority); err != nil {
			log.Printf("[%s] Could not push %s back to the frontier for retry: %s\n", source, row.URL, err)
			return
		}
		logWarn("[%s] Will retry %s (attempt %d of %d)", source, row.URL, nextErrorCount+1, maxURLRetries)
		return
	}

	if err := c.store.ScheduleDelayedRetry(ctx, source, row.URL, nextErrorCount, priority, time.Now().Add(delay)); err != nil {
		log.Printf("[%s] Could not schedule a delayed retry for %s: %s\n", source, row.URL, err)
		return
	}
	logWarn("[%s] Will retry %s in ~%s (attempt %d of %d) — continuing with other work meanwhile", source, row.URL, delay, nextErrorCount+1, maxURLRetries)
}

// processURL fetches, parses, and saves everything for one URL — identical work regardless of
// which source's worker goroutine calls it. Returns gotQuotes (fed into a Wikiquote worker's
// dead-streak circuit breaker), newQuotes (how many quotes were actually saved, i.e. not
// duplicates — fed into Goodreads' dry-streak search trigger, see goodreads_discovery.go), stalled (true if
// the fetch was slow-but-succeeded or hit a 429 — both real evidence of a source-wide problem,
// telling the caller to back this source's whole cooldown off to stallPenaltyFor(source) instead
// of just its normal minDelay for the next iteration), and rateLimited (specifically a 429, as
// opposed to any other failure — see fetcher.IsRateLimited — which tells a worker running at
// wikiquoteExperimentalDelay to permanently abandon that pace).
//
// A plain outright failure that *isn't* a 429 (a 503, a timeout, a connection reset, ...) is
// deliberately NOT treated as stalled: there's no particular reason to believe a single bad
// page means the next, different page from the same source is about to fail too, so the worker
// just schedules that one URL for a delayed retry (hardFailureRetryDelay) and immediately
// continues with whatever else is ready — see failOrRetry.
func (c *Crawler) processURL(ctx context.Context, source string, url string) (gotQuotes bool, newQuotes int, stalled bool, rateLimited bool) {
	// Defense-in-depth against MediaWiki red links (see parser.resolveWikiquoteLink, which
	// already stops new ones from ever being queued): this still catches any row that was
	// queued *before* that fix shipped and is only now being popped off the frontier. Without
	// this, every one of those stale rows would burn a real network round-trip, a guaranteed
	// 404, the full stall backoff, and all maxURLRetries attempts — for a URL whose own query
	// string already says it can never resolve to content. Short-circuiting straight to
	// MarkURLFailed (no fetch, no retry, no backoff) flushes that backlog for free the moment
	// each row is next popped, instead of paying the same wasted cost repeatedly.
	if strings.Contains(url, "redlink=1") {
		if err := c.store.MarkURLFailed(ctx, url); err != nil {
			log.Printf("[%s] Could not mark red-link URL %s failed: %s\n", source, url, err)
		}
		log.Printf("[%s] Skipping known-dead red link (queued before the discovery fix): %s", source, url)
		return false, 0, false, false
	}

	row, err := c.store.GetURLByURL(ctx, url)
	if err != nil {
		log.Printf("[%s] Could not look up URL %s: %s\n", source, url, err)
		return false, 0, false, false
	}

	if err := c.store.MarkURLInProgress(ctx, url); err != nil {
		log.Printf("[%s] Could not mark the URL: %s\n", source, err)
		return false, 0, false, false
	}

	fetchStart := time.Now()
	html, err := fetcher.Fetch(ctx, url)
	if elapsed := time.Since(fetchStart); elapsed > slowFetchWarn {
		log.Printf("Slow fetch: %s (source=%s) took %s — likely soft rate-limited by the source; backing off ~%s", url, source, elapsed.Round(time.Second), stallPenaltyDisplayFor(source))
		stalled = true
	}
	if err != nil {
		rateLimited = fetcher.IsRateLimited(err)
		if rateLimited {
			log.Printf("Fetch failed for %s (source=%s): %s — backing off ~%s", url, source, err, stallPenaltyDisplayFor(source))
			stalled = true
			c.failOrRetry(ctx, source, row, 0)
		} else {
			log.Printf("Fetch failed for %s (source=%s): %s — scheduling a retry in ~%s, continuing with other work meanwhile", url, source, err, hardFailureRetryDelay)
			c.failOrRetry(ctx, source, row, hardFailureRetryDelay)
		}
		return false, 0, stalled, rateLimited
	}

	var p parser.Parser
	switch source {
	case scoring.SourceGoodreads:
		p = &parser.GoodreadsParser{}
	case scoring.SourceWikiquoteEN:
		p = &parser.WikiquoteParser{}
	case scoring.SourceWikiquoteDE:
		p = &parser.GermanWikiquoteParser{}
	case scoring.SourceWikiquoteFR:
		p = &parser.LocalizedWikiquoteParser{Source: scoring.SourceWikiquoteFR, Language: "fr", ExcludedHeadingPrefixes: parser.WikiquoteFRExcludedHeadingPrefixes}
	case scoring.SourceWikiquoteES:
		p = &parser.LocalizedWikiquoteParser{Source: scoring.SourceWikiquoteES, Language: "es", ExcludedHeadingPrefixes: parser.WikiquoteESExcludedHeadingPrefixes}
	case scoring.SourceWikiquoteIT:
		p = &parser.LocalizedWikiquoteParser{Source: scoring.SourceWikiquoteIT, Language: "it", ExcludedHeadingPrefixes: parser.WikiquoteITExcludedHeadingPrefixes}
	case scoring.SourceWikiquotePT:
		p = &parser.LocalizedWikiquoteParser{Source: scoring.SourceWikiquotePT, Language: "pt", ExcludedHeadingPrefixes: parser.WikiquotePTExcludedHeadingPrefixes}
	case scoring.SourceWikiquotePL:
		p = &parser.LocalizedWikiquoteParser{Source: scoring.SourceWikiquotePL, Language: "pl", ExcludedHeadingPrefixes: parser.WikiquotePLExcludedHeadingPrefixes}
	case scoring.SourceWikiquoteSV:
		p = &parser.LocalizedWikiquoteParser{Source: scoring.SourceWikiquoteSV, Language: "sv", ExcludedHeadingPrefixes: parser.WikiquoteSVExcludedHeadingPrefixes}
	case scoring.SourceWikiquoteRO:
		p = &parser.LocalizedWikiquoteParser{Source: scoring.SourceWikiquoteRO, Language: "ro", ExcludedHeadingPrefixes: parser.WikiquoteROExcludedHeadingPrefixes}
	case scoring.SourceWikiquoteCS:
		p = &parser.LocalizedWikiquoteParser{Source: scoring.SourceWikiquoteCS, Language: "cs", ExcludedHeadingPrefixes: parser.WikiquoteCSExcludedHeadingPrefixes}
	case scoring.SourceWikiquoteHU:
		p = &parser.LocalizedWikiquoteParser{Source: scoring.SourceWikiquoteHU, Language: "hu", ExcludedHeadingPrefixes: parser.WikiquoteHUExcludedHeadingPrefixes}
	case scoring.SourceWikiquoteDA:
		p = &parser.LocalizedWikiquoteParser{Source: scoring.SourceWikiquoteDA, Language: "da", ExcludedHeadingPrefixes: parser.WikiquoteDAExcludedHeadingPrefixes}
	case scoring.SourceWikiquoteNO:
		p = &parser.LocalizedWikiquoteParser{Source: scoring.SourceWikiquoteNO, Language: "no", ExcludedHeadingPrefixes: parser.WikiquoteNOExcludedHeadingPrefixes}
	case scoring.SourceWikiquoteFI:
		p = &parser.LocalizedWikiquoteParser{Source: scoring.SourceWikiquoteFI, Language: "fi", ExcludedHeadingPrefixes: parser.WikiquoteFIExcludedHeadingPrefixes}
	default:
		log.Printf("Unknown source: %s\n", source)
		if err := c.store.MarkURLFailed(ctx, url); err != nil {
			log.Printf("[%s] Could not mark the URL: %s\n", source, err)
		}
		return false, 0, stalled, false
	}

	result, err := p.Parse(html)
	if err != nil {
		log.Printf("[%s] Could not parse the URL: %s\n", source, err)
		if err := c.store.MarkURLFailed(ctx, url); err != nil {
			log.Printf("[%s] Could not mark the URL: %s\n", source, err)
		}
		return false, 0, stalled, false
	}
	logInfo("Parsed %s (source=%s): found %d quotes, %d discovered URLs", url, source, len(result.Quotes), len(result.NextURLs))

	// Quality-filtered first, saved second: filtering is pure/local (no DB round trip either
	// way), but batching the actual save matters — a single Goodreads page yields ~30 quotes,
	// previously 30 separate INSERT round trips (measured live: ~99ms) collapsed into one
	// (measured live: ~30ms, and the gap only widens with real network latency to Postgres,
	// unlike this same-host localhost comparison).
	var candidates []models.Quote
	for _, quote := range result.Quotes {
		quote.SourceURL = url
		if !dedup.IsLatinScript(quote.Text) {
			logWarn("Skipped non-Latin-script quote [%s]: %q — %s", quote.Source, truncate(quote.Text, 40), quote.Author)
			continue
		}
		if dedup.IsTooShort(quote.Text) {
			logWarn("Skipped quote under %d characters [%s]: %q — %s", dedup.MinQuoteLength, quote.Source, quote.Text, quote.Author)
			continue
		}
		if dedup.LooksLikeDictionaryEntry(quote.Text) {
			logWarn("Skipped dictionary-style entry [%s]: %q — %s", quote.Source, truncate(quote.Text, 40), quote.Author)
			continue
		}
		if dedup.LooksLikeWikiDiscussion(quote.Text) {
			logWarn("Skipped wiki discussion/signature content [%s]: %q — %s", quote.Source, truncate(quote.Text, 40), quote.Author)
			continue
		}
		if !dedup.MatchesClaimedLanguage(quote.Text, quote.Language) {
			logWarn("Skipped quote not actually in its claimed language %q [%s]: %q — %s", quote.Language, quote.Source, truncate(quote.Text, 40), quote.Author)
			continue
		}
		candidates = append(candidates, quote)
	}

	newlySaved := 0
	if len(candidates) > 0 {
		saved, err := c.store.SaveQuotes(ctx, candidates)
		if err != nil {
			log.Printf("Could not save quotes: %s\n", err)
		} else {
			for i, quote := range candidates {
				if saved[i] {
					newlySaved++
					logInfo("Saved quote [%s]: %q — %s", quote.Source, truncate(quote.Text, 60), quote.Author)
				} else {
					logWarn("Skipped duplicate [%s]: %q — %s", quote.Source, truncate(quote.Text, 60), quote.Author)
				}
			}
			c.recordQuotesSaved(ctx, newlySaved)
		}
	}

	if len(result.NextURLs) > 0 {
		// Batched INSERT + batched ZADD per distinct priority, same as the discovery top-up
		// paths — citation-link extraction can surface dozens of URLs per page, not the 0-1 a
		// per-URL round-trip loop was originally sized for. Goodreads scores each URL by its
		// shape (see scoring.GoodreadsPriority), so one page's links span a few priorities.
		staleChain := false
		if source == scoring.SourceGoodreads && len(candidates) > 0 && newlySaved == 0 {
			_, page := scoring.GoodreadsPage(url)
			staleChain = page > 1
		}
		byPriority := make(map[float64][]string)
		for _, next := range result.NextURLs {
			priority := scoring.PriorityFor(source, next, int(row.Depth)+1, 0)
			if staleChain {
				if _, page := scoring.GoodreadsPage(next); page > 1 {
					priority += scoring.GoodreadsStaleChainPenalty
				}
			}
			byPriority[priority] = append(byPriority[priority], next)
		}
		for priority, urls := range byPriority {
			inserted, err := c.store.SaveURLsBatch(ctx, urls, source, priority, row.Depth+1)
			if err != nil {
				log.Printf("Could not save discovered URLs: %s\n", err)
				continue
			}
			if len(inserted) == 0 {
				continue
			}
			if err := c.store.PushURLsBatch(ctx, source, inserted, priority); err != nil {
				log.Printf("Could not push discovered URLs to frontier: %s\n", err)
				continue
			}
			for _, u := range inserted {
				logInfo("Discovered URL [%s]: %s (from %s)", source, u, url)
			}
		}
	}

	if err := c.store.MarkURLDone(ctx, url); err != nil {
		log.Printf("Could not mark the URL as done: %s\n", err)
	}

	return len(result.Quotes) > 0, newlySaved, stalled, false
}

// runWorker drives one source's fetch loop independently, forever, on its own goroutine — the
// fix for the crawler previously being single-threaded across all sources. Before this, every
// source shared one loop: a single slow or stalled fetch (Goodreads' ~60s soft-throttle tarpit
// is the known example) blocked the whole process, including a different source's fetch that
// was already off cooldown and ready to go. Cooldown (lastFetch) and, for Wikiquote editions,
// the dead-streak circuit breaker are both plain local variables — each is only ever touched by
// the one goroutine that owns it. onResult, if non-nil, is handed whether the fetch yielded any
// quotes and how many of them were newly saved.
//
// The worker only fetches: keeping its queue filled is runFeeder's job, on a separate goroutine
// (see feeder.go). The two share throttle, so a 429 seen by either one pauses both, and the
// worker holds off while the feeder is in refill mode.
//
// Every wait point uses sleepCtx instead of time.Sleep and checks its result (or ctx.Err()
// directly), so cancelling ctx (graceful shutdown — see Run) stops this loop within roughly one
// in-flight fetch, not after waiting out whatever cooldown or stall penalty happened to be
// pending. fetcher.Fetch itself is also ctx-aware, so even a fetch already in flight when
// shutdown is requested gets cut short instead of running out its full 90s timeout.
//
// minDelay starts as whatever the caller passes in (the proven-safe value for Goodreads, or
// the deliberately narrower wikiquoteExperimentalDelay for a Wikiquote worker) but is a plain
// local variable, not a fixed parameter — abandonExperimentalPace can permanently widen it mid-run
// the instant a real 429 shows up.
func (c *Crawler) runWorker(ctx context.Context, source string, minDelay time.Duration, throttle *sourceThrottle, onResult func(gotQuotes bool, newQuotes int)) {
	// A random startup stagger for every Wikiquote worker (not Goodreads — a single stream has
	// no herd to desynchronize) — found live: all fourteen goroutines start in the same instant,
	// so their very first request collides immediately, before any retry jitter ever gets
	// a chance to matter (confirmed live at startup, twice: three editions 429'd within the same
	// second with a 5s spread window; still five within the same second after widening to that
	// 5s window, given ~12 sources landing across it — bumped to 15s for a meaningfully lower
	// per-second collision rate).
	if source != scoring.SourceGoodreads {
		if sleepCtx(ctx, time.Duration(rand.Float64()*float64(15*time.Second))) {
			return
		}
	}

	var lastFetch time.Time
	for {
		if ctx.Err() != nil {
			return
		}

		if !lastFetch.IsZero() {
			if wait := minDelay - time.Since(lastFetch); wait > 0 {
				if sleepCtx(ctx, wait) {
					return
				}
			}
		}
		if throttle.wait(ctx) {
			return
		}
		// Refill mode (see feeder.go): the feeder is using this source's request budget for
		// discovery, so hold off until it hands it back.
		for throttle.isRefilling() {
			if sleepCtx(ctx, time.Second) {
				return
			}
		}
		if throttle.takeRateLimited() {
			minDelay = c.abandonExperimentalPace(source, minDelay)
		}

		// Move any of this source's delayed retries (see failOrRetry/hardFailureRetryDelay)
		// whose hold has expired back onto the live frontier, so PopURL below can actually pick
		// them up again — done every iteration, not just when the queue looks empty, since a
		// retry becoming due has nothing to do with how much other work is currently pending.
		if promoted, err := c.store.PromoteDueRetries(ctx, source); err != nil {
			log.Printf("[%s] Could not promote due delayed retries: %s\n", source, err)
		} else if promoted > 0 {
			logInfo("[%s] Promoted %d delayed retry URL(s) back onto the frontier", source, promoted)
		}

		url, err := c.store.PopURL(ctx, source)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[%s] Could not get the next URL: %s\n", source, err)
			if sleepCtx(ctx, errorBackoff) {
				return
			}
			continue
		}
		if url == "" {
			// Nothing queued: the feeder is working on it (or the source is exhausted for now).
			if sleepCtx(ctx, idlePollInterval) {
				return
			}
			continue
		}

		lastFetch = time.Now()
		gotQuotes, newQuotes, stalled, rateLimited := c.processURL(ctx, source, url)
		if stalled {
			lastFetch = lastFetch.Add(stallPenaltyFor(source) - minDelay)
			// Goodreads signals throttling with slow responses rather than 429s, so any stall
			// counts as the signal to leave its experimental pace.
			if rateLimited || source == scoring.SourceGoodreads {
				minDelay = c.abandonExperimentalPace(source, minDelay)
			}
			if rateLimited {
				// Hold the feeder off this host for the same penalty.
				throttle.backOff(stallPenaltyFor(source), false)
			}
		}
		if onResult != nil {
			onResult(gotQuotes, newQuotes)
		}
	}
}

// abandonExperimentalPace permanently widens a source's cooldown from an experimental pace
// (wikiquoteExperimentalDelay) up to its normal, proven-safe one (minSourceDelayFor) the first
// time a real 429 is observed at the faster pace — an automatic, one-way safety net for the
// bounded pacing experiment, so a rate-limit signal doesn't require a human watching logs to
// react to it. Only ever widens, never narrows back within the same process run: once real
// evidence of rate-limiting shows up, there's no reason to risk finding it again in the same
// run. A no-op if current is already at or above the safe pace (e.g. Goodreads, which never
// already widened by an earlier stall).
func (c *Crawler) abandonExperimentalPace(source string, current time.Duration) time.Duration {
	safe := minSourceDelayFor(source)
	if current >= safe {
		return current
	}
	log.Printf("[%s] Throttled at the experimental %s pace — reverting to the proven-safe %s for the rest of this run", source, current, safe)
	return safe
}

// wikiquoteTopUpFunc returns the feeder's topUp closure for site. It walks the curated category
// tree (topUpWikiquoteCategories) until wikiquoteEmptyStreakBeforeAllPages calls in a row find
// nothing new, then walks the edition's whole article list (topUpWikiquoteAllPages) to the end of
// a pass. A pass that queued something goes straight back to the category walk. A pass that
// queued nothing switches to random batches (topUpWikiquoteRandomPages) for
// wikiquoteRandomModeSpan, at the feeder's normal pace, then starts the cycle over. Discovery
// never pauses.
//
// All state is local to this closure, one per site, only touched from that site's feeder
// goroutine. Failed calls don't count toward the streak: a rate-limited attempt isn't evidence
// the edition is exhausted. Each call makes at most one request — see topUpResult.
func (c *Crawler) wikiquoteTopUpFunc(site wikiquoteSite) func(ctx context.Context) topUpResult {
	var emptyStreak int
	var inAllPages, passAdded bool
	var randomUntil time.Time
	return func(ctx context.Context) topUpResult {
		if time.Now().Before(randomUntil) {
			return c.topUpWikiquoteRandomPages(ctx, site)
		}

		if inAllPages {
			result, passDone := c.topUpWikiquoteAllPages(ctx, site)
			passAdded = passAdded || result.added
			if passDone {
				inAllPages = false
				if !passAdded {
					randomUntil = time.Now().Add(wikiquoteRandomModeSpan)
					log.Printf("%s: a full allpages pass found nothing new — switching to random batches until %s",
						site.source, randomUntil.Format(time.RFC3339))
				} else {
					log.Printf("%s: allpages pass finished, back to the category walk", site.source)
				}
				passAdded = false
			}
			return result
		}

		result := c.topUpWikiquoteCategories(ctx, site)
		if result.added {
			emptyStreak = 0
			return result
		}
		if result.calledNetwork && !result.networkFailed {
			emptyStreak++
		}
		if emptyStreak >= wikiquoteEmptyStreakBeforeAllPages {
			emptyStreak = 0
			inAllPages = true
			logWarn("%s: %d category-walk calls in a row found nothing new — walking the full article list", site.source, wikiquoteEmptyStreakBeforeAllPages)
		}
		return result
	}
}

func (c *Crawler) Run(ctx context.Context) error {

	quoteCount, err := c.store.CountQuotes(ctx)
	if err != nil {
		return err
	}
	totalQuoteCount.Store(quoteCount)
	log.Printf("Starting up with %s total quotes already in the database", formatCount(quoteCount))
	c.logQuoteSummary(ctx, "at startup")

	err = c.store.WarmSimhashCache(ctx)
	if err != nil {
		return err
	}
	logInfo("Simhash cache warmed up")

	// Loads all language models from disk up front (~3.4s, confirmed live) rather than paying
	// that cost unexplained on whichever quote happens to be first through MatchesClaimedLanguage.
	dedup.WarmLanguageDetector()
	logInfo("Language detector warmed up")

	requeued, err := c.store.RequeueStuckInProgress(ctx)
	if err != nil {
		return err
	}
	if requeued > 0 {
		logWarn("Requeued %d URL(s) stuck in_progress from a previous run", requeued)
	}

	// Goodreads is scored by URL shape (scoring.GoodreadsPriority); rescoring here applies the
	// current scheme to the backlog already queued too, not only to URLs discovered from now on.
	// A no-op write-wise once every pending row already matches.
	rescored, err := c.store.RescorePending(ctx, scoring.SourceGoodreads, func(u string, errorCount int32) float64 {
		return scoring.GoodreadsPriority(u, int(errorCount))
	})
	if err != nil {
		return err
	}
	if rescored > 0 {
		log.Printf("Rescored %s pending Goodreads URL(s) by page type", formatCount(int64(rescored)))
	}

	err = c.store.WarmFrontierCache(ctx)
	if err != nil {
		return err
	}

	err = c.SeedFrontier(ctx)
	if err != nil {
		return err
	}
	logInfo("Seed frontier cache warmed up")

	// Each source gets two goroutines: a worker that only fetches (runWorker), on its own
	// cooldown, so a stall on one source never delays another; and a feeder that keeps the
	// worker's queue filled ahead of it (runFeeder, see feeder.go). They only ever share c.store
	// (pgxpool.Pool and redis.Client are both safe for concurrent use by design) and the source's
	// sourceThrottle.
	var wg sync.WaitGroup
	go2 := func(f func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f()
		}()
	}

	gdThrottle := &sourceThrottle{}
	gd := &goodreadsDiscovery{c: c}
	go2(func() {
		c.runWorker(ctx, scoring.SourceGoodreads, goodreadsExperimentalDelay, gdThrottle,
			func(gotQuotes bool, newQuotes int) { gd.onResult(ctx, gotQuotes, newQuotes) },
		)
	})
	go2(func() {
		c.runFeeder(ctx, scoring.SourceGoodreads, false, 0, 0, gdThrottle,
			c.frontierSizeFunc(scoring.SourceGoodreads), c.goodreadsNeedsSearches, gd.topUp)
	})

	// Every Wikiquote edition starts at wikiquoteExperimentalDelay rather than its proven-safe
	// minSourceDelayFor value — a deliberate, bounded pacing experiment (requested explicitly,
	// with the tradeoff understood), automatically abandoned in favor of the safe pace the
	// instant that edition hits a real 429 (see runWorker/abandonExperimentalPace).
	for _, site := range []wikiquoteSite{
		wikiquoteEN, wikiquoteDE,
		wikiquoteFR, wikiquoteES, wikiquoteIT, wikiquotePT, wikiquotePL,
		wikiquoteSV, wikiquoteRO, wikiquoteCS, wikiquoteHU, wikiquoteDA, wikiquoteNO, wikiquoteFI,
	} {
		site := site
		throttle := &sourceThrottle{}
		go2(func() {
			var deadStreak int
			c.runWorker(ctx, site.source, wikiquoteExperimentalDelay, throttle,
				func(gotQuotes bool, _ int) { deadStreak = c.recordWikiquoteYield(ctx, site, deadStreak, gotQuotes) },
			)
		})
		go2(func() {
			c.runFeeder(ctx, site.source, true, wikiquoteFeederDelay, wikiquoteExperimentalDelay, throttle,
				c.frontierSizeFunc(site.source), nil, c.wikiquoteTopUpFunc(site))
		})
	}

	wg.Wait() // returns once ctx is cancelled and every worker has noticed and exited
	logInfo("All workers stopped, shutting down")
	return nil
}

func (c *Crawler) SeedFrontier(ctx context.Context) error {
	// HasAnyURLs, not "any pending" — the frontier having been seeded once should stay true
	// forever, including after every seed URL finishes. Checking pending-only re-triggers
	// seeding (and re-crawls already-completed pages) the moment a batch finishes, which was
	// observed live: Wikiquote seed pages got re-fetched a 2nd/3rd time across restarts once
	// their pending count hit zero.
	seeded, err := c.store.HasAnyURLs(ctx)
	if err != nil {
		return err
	}
	if seeded {
		logInfo("Frontier already seeded.")
		return nil
	}

	goodreadsSeed := "https://www.goodreads.com/quotes/tag/inspirational"
	seeds := []models.URLFrontier{
		{URL: goodreadsSeed, Source: scoring.SourceGoodreads, Priority: scoring.GoodreadsPriority(goodreadsSeed, 0)},
	}

	// WikiquoteParser has no NextURLs discovery of its own — real discovery comes from
	// topUpWikiquoteCategories (MediaWiki categorymembers, see wikiquote_discovery.go) once
	// this stopgap list runs out. Each URL here is one author page yielding many quotes from a
	// single fetch, so this list lasts a while even before that kicks in. German has no
	// equivalent stopgap list — category-based discovery already works well enough that a
	// hand-picked German author list would just be redundant upfront research; it bootstraps
	// straight from topUpWikiquoteCategories the first time the crawler goes idle.
	wikiquotePriority := scoring.CalculatePriority(scoring.SourceWikiquoteEN, 0, 0)
	wikiquoteAuthors := []string{
		"Mark_Twain", "Oscar_Wilde", "Maya_Angelou", "Winston_Churchill", "Mahatma_Gandhi",
		"Abraham_Lincoln", "Martin_Luther_King_Jr.", "Nelson_Mandela", "William_Shakespeare",
		"Confucius", "Aristotle", "Plato", "Friedrich_Nietzsche", "Ralph_Waldo_Emerson",
		"Henry_David_Thoreau", "Eleanor_Roosevelt", "Theodore_Roosevelt", "Buddha",
		"Leonardo_da_Vinci", "Charles_Darwin",
	}
	for _, author := range wikiquoteAuthors {
		seeds = append(seeds, models.URLFrontier{
			URL:      "https://en.wikiquote.org/wiki/" + author,
			Source:   scoring.SourceWikiquoteEN,
			Priority: wikiquotePriority,
		})
	}

	for _, seed := range seeds {
		_, err := c.store.SaveURL(ctx, seed)
		if err != nil {
			log.Printf("Failed to save URL: %v", err)
			continue
		}
		err = c.store.PushURL(ctx, seed.Source, seed.URL, seed.Priority)
		if err != nil {
			log.Printf("Failed to push URL to Redis: %v", err)
		}
	}

	return nil
}

func truncate(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max]) + "..."
}
