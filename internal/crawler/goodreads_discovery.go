package crawler

import (
	"context"
	"log"
	"net/url"
	"strconv"

	"quotes-crawler/internal/models"
	"quotes-crawler/internal/scoring"
)

// goodreadsDryStreakBeforeSearch: after this many Goodreads pages in a row that had quotes but
// saved none of them (every one a duplicate), seed the next keyword search. Found live at ~2.7M
// quotes: the link graph (tags, authors, pagination) still had plenty of pending URLs, but
// nearly every one was a page of already-saved quotes. The fallbacks in crawler.go never fired
// because they only trigger once the queue is *empty*, and a queue full of duplicates never
// empties fast. So this trigger is based on yield, not queue size.
const goodreadsDryStreakBeforeSearch = 5

// goodreadsSearchCursorKey is the discovery-cursor key for the keyword walk, separate from the
// tag-cycling cursor stored under scoring.SourceGoodreads.
const goodreadsSearchCursorKey = "goodreads-search"

// goodreadsSearchTerms drive Goodreads' quote search (/quotes/search?q=...). Each search
// paginates up to 100 pages of 20 quotes (confirmed live), renders the same quote card markup
// GoodreadsParser already handles, and links each quote's author, so every term opens a fresh
// chain into the long tail plus a batch of authors whose own quotes pages then paginate deep.
// Unlike tags, results match the quote *text*, so they cut across the tag graph instead of
// re-walking it. These are ordinary words that show up in quotes, ordered roughly from broad to
// narrow. The list is easy to extend: a term already seeded is skipped for free.
var goodreadsSearchTerms = []string{
	"life", "love", "time", "heart", "world", "people", "mind", "soul", "truth", "fear",
	"hope", "dream", "death", "night", "god", "war", "child", "mother", "father", "friend",
	"home", "light", "dark", "sea", "sky", "rain", "fire", "earth", "water", "wind",
	"book", "word", "story", "write", "read", "music", "song", "art", "beauty", "silence",
	"memory", "past", "future", "change", "power", "freedom", "peace", "money", "work", "school",
	"city", "road", "journey", "door", "window", "garden", "flower", "tree", "forest", "river",
	"mountain", "winter", "summer", "spring", "autumn", "morning", "evening", "sun", "moon", "star",
	"body", "blood", "skin", "eyes", "hands", "voice", "face", "smile", "tears", "kiss",
	"anger", "joy", "grief", "pain", "loss", "lonely", "alone", "happy", "sad", "strange",
	"wisdom", "knowledge", "science", "history", "nature", "animal", "dog", "cat", "bird", "horse",
	"king", "queen", "hero", "enemy", "stranger", "family", "marriage", "wife", "husband", "sister",
	"brother", "daughter", "son", "baby", "young", "old", "age", "birth", "grave", "ghost",
	"magic", "monster", "dragon", "witch", "angel", "devil", "heaven", "hell", "prayer", "faith",
	"doubt", "question", "answer", "reason", "logic", "madness", "sanity", "secret", "lie", "promise",
	"choice", "chance", "luck", "fate", "courage", "strength", "weakness", "failure", "success", "mistake",
	"forgive", "regret", "shame", "pride", "honor", "justice", "law", "crime", "prison", "revolution",
	"nation", "country", "politics", "leader", "poverty", "wealth", "hunger", "food", "coffee", "wine",
	"bread", "kitchen", "table", "bed", "sleep", "wake", "dance", "laugh", "cry", "walk",
	"run", "fly", "fall", "climb", "swim", "travel", "map", "ocean", "island", "ship",
	"train", "letter", "poem", "poetry", "novel", "library", "teacher", "student", "lesson", "curiosity",
	"imagination", "creativity", "idea", "thought", "feeling", "emotion", "desire", "passion", "jealousy", "envy",
	"greed", "kindness", "cruelty", "mercy", "compassion", "empathy", "respect", "trust", "betrayal", "loyalty",
	"honesty", "patience", "habit", "discipline", "goal", "purpose", "meaning", "existence", "universe", "space",
	"planet", "machine", "technology", "computer", "ancient", "myth", "legend", "childhood", "youth", "growing",
	"becoming", "self", "identity", "mirror", "shadow", "mask",
}

// goodreadsDiscovery owns Goodreads' discovery state for one worker: the existing tag-cycling and
// random-page fallbacks (for an empty queue) plus the yield-triggered keyword search (for a queue
// full of duplicates). Only ever touched from the Goodreads worker goroutine, so no locking.
type goodreadsDiscovery struct {
	c           *Crawler
	emptyStreak int // consecutive tag-cycling attempts that added nothing — see topUp
	dryStreak   int // consecutive pages whose quotes were all duplicates — see onResult
	searchSpent bool
}

// onResult is runWorker's per-fetch callback. A page with no quotes at all (an empty tag, a parse
// miss) doesn't count either way; only "had quotes, all already saved" moves toward a search seed.
// Seeding only touches Postgres/Redis, so doing it here costs no request to goodreads.com.
func (d *goodreadsDiscovery) onResult(ctx context.Context, gotQuotes bool, newQuotes int) {
	if newQuotes > 0 {
		d.dryStreak = 0
		return
	}
	if !gotQuotes {
		return
	}
	d.dryStreak++
	if d.dryStreak < goodreadsDryStreakBeforeSearch {
		return
	}
	d.dryStreak = 0
	d.c.seedGoodreadsSearch(ctx, d)
}

// topUp is the empty-queue path: the next keyword search while any are left (the deepest chain
// available), then the curated tag list, then a random popular-quotes page once tags stop finding
// anything.
//
// Unlike Wikiquote's topUpResult.calledNetwork, topUpGoodreadsIfEmpty never sets calledNetwork (it
// only touches Postgres/Redis), so this checks the pending count directly first: a nonzero count
// means there's nothing to top up right now and must not count toward the streak.
func (d *goodreadsDiscovery) topUp(ctx context.Context) topUpResult {
	pending, err := d.c.store.CountPendingBySource(ctx, scoring.SourceGoodreads)
	if err != nil {
		log.Printf("Could not check Goodreads pending count: %s\n", err)
		return topUpResult{}
	}
	if pending > 0 {
		return topUpResult{}
	}

	if r := d.c.seedGoodreadsSearch(ctx, d); r.added {
		return r
	}

	result := d.c.topUpGoodreadsIfEmpty(ctx)
	if result.added {
		d.emptyStreak = 0
		return result
	}
	d.emptyStreak++
	if d.emptyStreak >= goodreadsEmptyStreakBeforeRandom {
		d.emptyStreak = 0
		logWarn("goodreads: %d consecutive tag-cycling attempts found nothing new — falling back to a random popular-quotes page", goodreadsEmptyStreakBeforeRandom)
		return d.c.topUpGoodreadsRandomPages(ctx)
	}
	return result
}

// seedGoodreadsSearch queues the first page of the next not-yet-seeded search term. Terms already
// in the frontier (seeded on an earlier run) are skipped without any request, so one call walks
// forward until it inserts one or has tried the whole list.
func (c *Crawler) seedGoodreadsSearch(ctx context.Context, d *goodreadsDiscovery) topUpResult {
	if d.searchSpent {
		return topUpResult{}
	}
	idx := 0
	if raw, err := c.store.GetDiscoveryCursor(ctx, goodreadsSearchCursorKey); err == nil && raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			idx = n
		}
	}

	for tries := 0; tries < len(goodreadsSearchTerms); tries++ {
		term := goodreadsSearchTerms[idx%len(goodreadsSearchTerms)]
		idx = (idx + 1) % len(goodreadsSearchTerms)
		if err := c.store.SetDiscoveryCursor(ctx, goodreadsSearchCursorKey, strconv.Itoa(idx)); err != nil {
			log.Printf("Could not save Goodreads search cursor: %s\n", err)
		}

		searchURL := "https://www.goodreads.com/quotes/search?q=" + url.QueryEscape(term)
		priority := scoring.GoodreadsPriority(searchURL, 0)
		inserted, err := c.store.SaveURL(ctx, models.URLFrontier{URL: searchURL, Source: scoring.SourceGoodreads, Priority: priority})
		if err != nil {
			log.Printf("Could not save Goodreads search seed: %s\n", err)
			return topUpResult{}
		}
		if !inserted {
			continue
		}
		if err := c.store.PushURL(ctx, scoring.SourceGoodreads, searchURL, priority); err != nil {
			log.Printf("Could not push Goodreads search seed to frontier: %s\n", err)
			return topUpResult{}
		}
		log.Printf("goodreads: recent pages were all duplicates — seeded keyword search %q (%s)", term, searchURL)
		return topUpResult{added: true}
	}

	d.searchSpent = true
	log.Printf("goodreads: every one of the %d keyword search terms has already been seeded — add more to goodreadsSearchTerms to keep this fallback useful", len(goodreadsSearchTerms))
	return topUpResult{}
}
