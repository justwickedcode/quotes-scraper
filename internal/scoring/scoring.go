package scoring

import (
	"net/url"
	"strconv"
	"strings"
)

const (
	DepthPenalty = 0.5
	ErrorPenalty = 3.0
)

const (
	SourceWikiquoteEN = "wikiquote-en"
	SourceWikiquoteDE = "wikiquote-de"
	SourceWikiquoteFR = "wikiquote-fr"
	SourceWikiquoteES = "wikiquote-es"
	SourceWikiquoteIT = "wikiquote-it"
	SourceWikiquotePT = "wikiquote-pt"
	SourceWikiquotePL = "wikiquote-pl"
	SourceWikiquoteSV = "wikiquote-sv"
	SourceWikiquoteRO = "wikiquote-ro"
	SourceWikiquoteCS = "wikiquote-cs"
	SourceWikiquoteHU = "wikiquote-hu"
	SourceWikiquoteDA = "wikiquote-da"
	SourceWikiquoteNO = "wikiquote-no"
	SourceWikiquoteFI = "wikiquote-fi"
	SourceGoodreads   = "goodreads"
)

// These base scores only affect ordering *within* one source's own queue now — cross-source
// fairness is handled by each source running on its own worker goroutine in crawler.go
// (Crawler.runWorker), not by priority score gaps (an earlier design that broke down once a
// source's backlog grew large — see README).
var sourceBaseScores = map[string]float64{
	SourceWikiquoteEN: 2.0,
	SourceWikiquoteDE: 2.0,
	SourceWikiquoteFR: 2.0,
	SourceWikiquoteES: 2.0,
	SourceWikiquoteIT: 2.0,
	SourceWikiquotePT: 2.0,
	SourceWikiquotePL: 2.0,
	SourceWikiquoteSV: 2.0,
	SourceWikiquoteRO: 2.0,
	SourceWikiquoteCS: 2.0,
	SourceWikiquoteHU: 2.0,
	SourceWikiquoteDA: 2.0,
	SourceWikiquoteNO: 2.0,
	SourceWikiquoteFI: 2.0,
	SourceGoodreads:   10.0,
}

const DefaultSourceBase float64 = 1000

func CalculatePriority(source string, depth int, errorCount int) float64 {
	sourceBase, ok := sourceBaseScores[source]
	if !ok {
		sourceBase = DefaultSourceBase
	}

	return sourceBase + (float64(depth) * DepthPenalty) + (float64(errorCount) * ErrorPenalty)
}

// Goodreads priorities by URL shape (lower = crawled sooner). Goodreads used to share the flat
// base + depth*DepthPenalty formula, which, once the frontier held a large backlog, broke down in
// two ways at once (found live at ~2.7M quotes, nearly every Goodreads page coming back 100%
// duplicates). First, pagination is a chain: page N+1 is only discovered from page N, at depth+1,
// so an author with 99 pages of quotes needed 99 depth levels, each one waiting behind every
// shallower URL in the backlog. That starved exactly the content least likely to be a duplicate:
// the long tail on an author's later pages. Second, every URL at the same depth tied on score, so
// Redis's ZPOPMIN fell back to lexicographic member order and the crawler ground through tag pages
// alphabetically ("the-magician", "the-maid", "the-maiden", ...). A tag's first page only shows
// its ~30 most popular quotes, which carry many tags each and were almost always already saved
// from somewhere else.
//
// So priority now follows the URL itself: continue pagination chains first (authors before
// tags/searches, since an author's quotes page lists *all* their quotes and is the deepest
// reservoir), then keyword-search entry points (only ever seeded when Goodreads has gone dry —
// see crawler's goodreads_discovery.go — so they should run soon after), then fresh author
// pages, and plain tag first pages last.
const (
	goodreadsAuthorNextPagePriority  = 4.0
	goodreadsListNextPagePriority    = 6.0 // tag, search, or popular-quotes page >= 2
	goodreadsAuthorFirstPagePriority = 8.0
	goodreadsSearchFirstPagePriority = 7.0
	goodreadsOtherPriority           = 10.0
	goodreadsTagFirstPagePriority    = 12.0

	// GoodreadsStaleChainPenalty is added to the next-page link of a page >= 2 whose quotes were
	// all already saved: the chain is walking territory that's already known, so it shouldn't
	// keep outranking fresh author pages. Not applied after a first page — every listing's first
	// page is its most popular quotes, which are nearly always already saved from elsewhere, so
	// it says little about the long tail on later pages.
	GoodreadsStaleChainPenalty = 4.0
)

// GoodreadsPage classifies a Goodreads URL into its page kind and page number (1 when absent).
func GoodreadsPage(rawURL string) (kind string, page int) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "other", 1
	}
	page = 1
	if p, err := strconv.Atoi(u.Query().Get("page")); err == nil && p > 1 {
		page = p
	}
	switch {
	case strings.HasPrefix(u.Path, "/author/quotes/"):
		return "author", page
	case strings.HasPrefix(u.Path, "/quotes/tag/"):
		return "tag", page
	case u.Path == "/quotes/search":
		return "search", page
	case u.Path == "/quotes":
		return "popular", page
	}
	return "other", page
}

// GoodreadsPriority scores a Goodreads URL by its shape — see the constants above.
func GoodreadsPriority(rawURL string, errorCount int) float64 {
	kind, page := GoodreadsPage(rawURL)
	var p float64
	switch {
	case kind == "author" && page > 1:
		p = goodreadsAuthorNextPagePriority
	case (kind == "tag" || kind == "search" || kind == "popular") && page > 1:
		p = goodreadsListNextPagePriority
	case kind == "author":
		p = goodreadsAuthorFirstPagePriority
	case kind == "search":
		p = goodreadsSearchFirstPagePriority
	case kind == "tag":
		p = goodreadsTagFirstPagePriority
	default:
		p = goodreadsOtherPriority
	}
	return p + float64(errorCount)*ErrorPenalty
}

// PriorityFor is the single entry point for scoring a frontier URL: Goodreads by URL shape,
// every other source by the generic base + depth formula.
func PriorityFor(source, rawURL string, depth, errorCount int) float64 {
	if source == SourceGoodreads {
		return GoodreadsPriority(rawURL, errorCount)
	}
	return CalculatePriority(source, depth, errorCount)
}
