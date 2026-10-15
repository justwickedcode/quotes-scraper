package crawler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"quotes-crawler/internal/fetcher"
)

// wikiquoteSite is what differs between Wikiquote language editions, so discovery is written once.
type wikiquoteSite struct {
	source   string // e.g. scoring.SourceWikiquoteEN
	language string // models.Quote.Language for this edition
	apiBase  string // e.g. "https://en.wikiquote.org/w/api.php"
	wikiBase string // e.g. "https://en.wikiquote.org/wiki/"
}

func newWikiquoteSite(lang string) wikiquoteSite {
	host := "https://" + lang + ".wikiquote.org"
	return wikiquoteSite{
		source:   "wikiquote-" + lang,
		language: lang,
		apiBase:  host + "/w/api.php",
		wikiBase: host + "/wiki/",
	}
}

// The configured editions: Latin-script languages only, since dedup.IsLatinScript would reject
// Cyrillic content outright. Dutch was checked and skipped: its pages nest quotes under
// "Origineel in het ...:"/"Aanhaling(en):" labels the parsers don't handle, and its corpus is
// tiny (1,317 articles).
var (
	wikiquoteEN = newWikiquoteSite("en")
	wikiquoteDE = newWikiquoteSite("de")
	wikiquoteFR = newWikiquoteSite("fr")
	wikiquoteES = newWikiquoteSite("es")
	wikiquoteIT = newWikiquoteSite("it")
	wikiquotePT = newWikiquoteSite("pt")
	wikiquotePL = newWikiquoteSite("pl")
	wikiquoteSV = newWikiquoteSite("sv")
	wikiquoteRO = newWikiquoteSite("ro")
	wikiquoteCS = newWikiquoteSite("cs")
	wikiquoteHU = newWikiquoteSite("hu")
	wikiquoteDA = newWikiquoteSite("da")
	wikiquoteNO = newWikiquoteSite("no")
	wikiquoteFI = newWikiquoteSite("fi")
)

// wikiquoteRandomBatchSize: titles per list=random call — 500 is the anonymous maximum
// (confirmed live: rnlimit=500 returns 500 titles with no warning).
const wikiquoteRandomBatchSize = 500

type wikiquoteRandomResponse struct {
	Query struct {
		Random []struct {
			Title string `json:"title"`
		} `json:"random"`
	} `json:"query"`
}

// fetchWikiquoteRandomPages asks an edition's MediaWiki API for a batch of random article titles
// (main namespace, redirects excluded). This is Wikiquote's only discovery: it reaches any
// article without depending on links or categories. Draws can repeat, so as coverage grows a
// batch holds fewer unknown titles; SaveURLsBatch skips the known ones. Non-person pages (films,
// topics) come back too; they parse to 0 quotes and cost one fetch each.
func fetchWikiquoteRandomPages(ctx context.Context, site wikiquoteSite) ([]string, error) {
	apiURL := fmt.Sprintf(
		"%s?action=query&list=random&rnnamespace=0&rnfilterredir=nonredirects&rnlimit=%d&format=json",
		site.apiBase, wikiquoteRandomBatchSize,
	)

	body, err := fetcher.Fetch(ctx, apiURL)
	if err != nil {
		return nil, err
	}

	var parsed wikiquoteRandomResponse
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		return nil, err
	}

	titles := make([]string, 0, len(parsed.Query.Random))
	for _, m := range parsed.Query.Random {
		titles = append(titles, m.Title)
	}
	return titles, nil
}

// wikiquoteTitleToURL converts a MediaWiki page title, as returned by the API (spaces, not
// underscores, and not URL-escaped), into that page's real URL on site.
func wikiquoteTitleToURL(site wikiquoteSite, title string) string {
	return site.wikiBase + url.PathEscape(strings.ReplaceAll(title, " ", "_"))
}
