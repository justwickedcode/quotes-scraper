package scoring

import "testing"

func TestGoodreadsPage(t *testing.T) {
	tests := []struct {
		url      string
		wantKind string
		wantPage int
	}{
		{"https://www.goodreads.com/author/quotes/3389.Stephen_King", "author", 1},
		{"https://www.goodreads.com/author/quotes/3389.Stephen_King?page=7", "author", 7},
		{"https://www.goodreads.com/quotes/tag/the-maid", "tag", 1},
		{"https://www.goodreads.com/quotes/tag/the-manhattanville-incident?page=2", "tag", 2},
		{"https://www.goodreads.com/quotes/search?q=river", "search", 1},
		{"https://www.goodreads.com/quotes/search?page=6&q=river", "search", 6},
		{"https://www.goodreads.com/quotes?page=40", "popular", 40},
		{"https://www.goodreads.com/work/quotes/965315", "other", 1},
		{"https://www.goodreads.com/quotes/tag/x?page=1", "tag", 1},
		{"https://www.goodreads.com/quotes/tag/x?page=abc", "tag", 1},
	}
	for _, tt := range tests {
		kind, page := GoodreadsPage(tt.url)
		if kind != tt.wantKind || page != tt.wantPage {
			t.Errorf("GoodreadsPage(%q) = (%q, %d), want (%q, %d)", tt.url, kind, page, tt.wantKind, tt.wantPage)
		}
	}
}

// The ordering is the point, not the exact numbers: pagination chains first (authors before
// listings), then search entry points, then author first pages, with tag first pages last.
func TestGoodreadsPriorityOrdering(t *testing.T) {
	ordered := []string{
		"https://www.goodreads.com/author/quotes/3389.Stephen_King?page=2",
		"https://www.goodreads.com/quotes/tag/love?page=2",
		"https://www.goodreads.com/quotes/search?q=river",
		"https://www.goodreads.com/author/quotes/3389.Stephen_King",
		"https://www.goodreads.com/work/quotes/965315",
		"https://www.goodreads.com/quotes/tag/love",
	}
	for i := 1; i < len(ordered); i++ {
		prev, cur := GoodreadsPriority(ordered[i-1], 0), GoodreadsPriority(ordered[i], 0)
		if prev >= cur {
			t.Errorf("GoodreadsPriority(%q) = %v, want < GoodreadsPriority(%q) = %v", ordered[i-1], prev, ordered[i], cur)
		}
	}

	if got, want := GoodreadsPriority(ordered[0], 2), GoodreadsPriority(ordered[0], 0)+2*ErrorPenalty; got != want {
		t.Errorf("error penalty not applied: got %v, want %v", got, want)
	}
}

func TestPriorityForNonGoodreadsUnchanged(t *testing.T) {
	if got, want := PriorityFor(SourceWikiquoteEN, "https://en.wikiquote.org/wiki/X", 3, 1), CalculatePriority(SourceWikiquoteEN, 3, 1); got != want {
		t.Errorf("PriorityFor = %v, want %v", got, want)
	}
}
