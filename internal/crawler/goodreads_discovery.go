package crawler

import (
	"context"
	"log"
	"net/url"
	"strconv"
	"strings"
	"sync"

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

// goodreadsAuthorSearchCursorKey holds the last quotes.id scanned for author names to search.
const goodreadsAuthorSearchCursorKey = "goodreads-author-search"

// goodreadsAuthorScanBatch is how many quotes one author-harvest batch scans, and
// goodreadsAuthorScanMaxBatches how many batches one call may scan looking for a new name.
const (
	goodreadsAuthorScanBatch      = 2000
	goodreadsAuthorScanMaxBatches = 10
)

// goodreadsSkipAuthors are attributions that aren't a person to search for. Compared lowercased.
var goodreadsSkipAuthors = map[string]bool{
	"unknown": true, "anonymous": true, "anon": true, "anon.": true, "anonym": true, "anonyme": true,
	"anónimo": true, "anonimo": true, "anonimowy": true, "anônimo": true, "proverb": true,
	"proverbs": true, "sprichwort": true, "proverbe": true, "proverbio": true, "provérbio": true,
	"przysłowie": true, "ordspråk": true, "unbekannt": true, "inconnu": true, "desconocido": true,
	"sconosciuto": true, "desconhecido": true, "nieznany": true, "okänd": true,
}

// goodreadsDiscovery owns Goodreads' discovery state. The feeder (topUp) and the worker
// (onResult) both reach seedGoodreadsSearch, from different goroutines, so mu guards it all.
type goodreadsDiscovery struct {
	c           *Crawler
	mu          sync.Mutex
	emptyStreak int // consecutive tag-cycling attempts that added nothing — see topUp
	dryStreak   int // consecutive pages whose quotes were all duplicates — see onResult
	searchSpent bool
}

// onResult is runWorker's per-fetch callback. A page with no quotes at all (an empty tag, a parse
// miss) doesn't count either way; only "had quotes, all already saved" moves toward a search seed.
// The feeder keeps a stock of searches queued anyway; this jumps one in right away when the
// pages being fetched have gone stale.
func (d *goodreadsDiscovery) onResult(ctx context.Context, gotQuotes bool, newQuotes int) {
	d.mu.Lock()
	defer d.mu.Unlock()
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
	if !d.c.seedGoodreadsSearch(ctx, d).added {
		d.c.seedGoodreadsAuthorSearches(ctx)
	}
}

// topUp is the feeder's discovery step, called whenever runFeeder decides the queue is
// short. In order: the next keyword search, then searches for authors already collected from
// other sources, then the curated tag list, then a random popular-quotes page once tags stop
// finding anything. Everything here is Postgres/Redis only; no request goes to goodreads.com.
func (d *goodreadsDiscovery) topUp(ctx context.Context) topUpResult {
	d.mu.Lock()
	defer d.mu.Unlock()

	if r := d.c.seedGoodreadsSearch(ctx, d); r.added {
		return r
	}
	if r := d.c.seedGoodreadsAuthorSearches(ctx); r.added {
		return r
	}

	result := d.c.topUpGoodreadsTag(ctx)
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

// seedGoodreadsAuthorSearches queues a Goodreads search for each new author name found in the
// next goodreadsAuthorScanBatch quotes from other sources (Wikiquote, all editions). A search
// for a name lists that author's quotes and links their Goodreads author page, whose pagination
// then runs deep: an entry point into authors the Goodreads link graph may never reach on its
// own. The scan resumes from a cursor and keeps up with newly saved quotes, so it doesn't run
// out the way a fixed word list does. Goodreads' own quotes are skipped: their authors are
// already linked from the pages they came from. Returns added=false only when the scan has
// caught up with the table (or every name in a batch was already queued; the next call moves on).
func (c *Crawler) seedGoodreadsAuthorSearches(ctx context.Context) topUpResult {
	var after int64
	if raw, err := c.store.GetDiscoveryCursor(ctx, goodreadsAuthorSearchCursorKey); err == nil && raw != "" {
		if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
			after = n
		}
	}

	// A batch whose names are all already queued moves the cursor on without adding anything, so
	// keep scanning (bounded, to keep one call short) until a batch adds something.
	for batch := 0; batch < goodreadsAuthorScanMaxBatches; batch++ {
		authors, lastID, err := c.store.QuoteAuthorsAfter(ctx, after, goodreadsAuthorScanBatch, scoring.SourceGoodreads)
		if err != nil {
			log.Printf("Could not scan quote authors for Goodreads searches: %s\n", err)
			return topUpResult{}
		}
		if lastID == after {
			return topUpResult{} // caught up with the quotes table
		}
		after = lastID
		if err := c.store.SetDiscoveryCursor(ctx, goodreadsAuthorSearchCursorKey, strconv.FormatInt(lastID, 10)); err != nil {
			log.Printf("Could not save Goodreads author-search cursor: %s\n", err)
		}

		urls := make([]string, 0, len(authors))
		for _, a := range authors {
			if n := len([]rune(a)); n < 3 || n > 80 || goodreadsSkipAuthors[strings.ToLower(a)] {
				continue
			}
			urls = append(urls, goodreadsSearchURL(a))
		}
		if len(urls) == 0 {
			continue
		}
		priority := scoring.GoodreadsPriority(urls[0], 0)
		inserted, err := c.store.SaveURLsBatch(ctx, urls, scoring.SourceGoodreads, priority, 0)
		if err != nil {
			log.Printf("Could not save Goodreads author searches: %s\n", err)
			return topUpResult{}
		}
		if len(inserted) == 0 {
			continue
		}
		if err := c.store.PushURLsBatch(ctx, scoring.SourceGoodreads, inserted, priority); err != nil {
			log.Printf("Could not push Goodreads author searches to frontier: %s\n", err)
			return topUpResult{}
		}
		log.Printf("goodreads: queued %d author-name searches from quotes up to id %d (e.g. %s)", len(inserted), lastID, inserted[0])
		return topUpResult{added: true, newURLs: len(inserted)}
	}
	return topUpResult{}
}

func goodreadsSearchURL(q string) string {
	return "https://www.goodreads.com/quotes/search?q=" + url.QueryEscape(q)
}

// seedGoodreadsSearch queues the first page of the next not-yet-seeded search term. Terms already
// in the frontier (seeded on an earlier run) are skipped without any request, so one call walks
// forward until it inserts one or has tried the whole list. Caller holds d.mu.
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

		searchURL := goodreadsSearchURL(term)
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
		log.Printf("goodreads: seeded keyword search %q (%s)", term, searchURL)
		return topUpResult{added: true}
	}

	d.searchSpent = true
	log.Printf("goodreads: all %d keyword search terms are seeded — continuing with author-name searches", len(goodreadsSearchTerms))
	return topUpResult{}
}
