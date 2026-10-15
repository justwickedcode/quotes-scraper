package crawler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"quotes-crawler/internal/fetcher"
)

// wikiquoteCategoryBatchSize is the max category members the MediaWiki API allows per
// request for anonymous (unauthenticated) access.
const wikiquoteCategoryBatchSize = 500

// wikiquoteSite bundles what differs between Wikiquote language editions (host, category
// namespace prefix, which curated categories to walk) so discovery logic is written once and
// reused per language rather than copy-pasted — added when a second language (German) turned
// "the English implementation" into "the Wikiquote-family implementation."
type wikiquoteSite struct {
	source            string // scoring.SourceWikiquote / scoring.SourceWikiquoteDE
	language          string // models.Quote.Language for this edition
	apiBase           string // e.g. "https://en.wikiquote.org/w/api.php"
	wikiBase          string // e.g. "https://en.wikiquote.org/wiki/"
	categoryPrefix    string // "Category:" (en) / "Kategorie:" (de) — MediaWiki namespace name is localized
	curatedCategories []string
}

// wikiquoteEN / wikiquoteDE are the two configured editions. Category sizes were confirmed
// live via categoryinfo before being added (English: see wikiquote_discovery.go history;
// German: Schriftsteller 767, Politiker 507, Philosoph 263, Schauspieler 140, Musiker 66,
// Wissenschaftler 45 direct + 38 subcats).
var wikiquoteEN = wikiquoteSite{
	source:         "wikiquote-en",
	language:       "en",
	apiBase:        "https://en.wikiquote.org/w/api.php",
	wikiBase:       "https://en.wikiquote.org/wiki/",
	categoryPrefix: "Category:",
	curatedCategories: []string{
		"Writers", "Actors", "Actresses", "Scientists", "Musicians", "Philosophers",
		"Politicians", "Activists", "Directors", "Comedians", "Academics", "Journalists",
		"Businesspeople", "Poets", "Novelists", "Religious leaders", "Composers", "Economists",
		"Educators", "Historians", "Artists", "Inventors", "Judges",
	},
}

var wikiquoteDE = wikiquoteSite{
	source:         "wikiquote-de",
	language:       "de",
	apiBase:        "https://de.wikiquote.org/w/api.php",
	wikiBase:       "https://de.wikiquote.org/wiki/",
	categoryPrefix: "Kategorie:",
	curatedCategories: []string{
		"Schriftsteller", "Schauspieler", "Politiker", "Philosoph", "Musiker",
		"Wissenschaftler",
	},
}

// wikiquoteFR / wikiquoteES / wikiquoteIT / wikiquotePT / wikiquotePL round out the five most
// widely spoken Latin-script languages Wikiquote runs full editions in (Italian's is the
// largest of the five by article count, bigger than German's — confirmed live via each
// edition's own siteinfo statistics before adding it, same discipline as EN/DE). Each edition's
// category namespace prefix and at least one curated category's real membership were confirmed
// live via the MediaWiki API before being added here, same as EN/DE were. Russian/other
// non-Latin-script editions are deliberately excluded — dedup.IsLatinScript would reject their
// content outright, a bigger design question than adding another Latin-script edition.
//
// These editions use LocalizedWikiquoteParser (internal/parser/wikiquote_i18n.go), not a
// per-language parser file — see that type's doc comment for why (page layout is "German-shaped":
// no fixed quotes heading, exclusion-list based) and why they deliberately skip citation-link
// NextURLs (self-sustaining via this same category discovery instead).
// Each edition's curatedCategories list was expanded well beyond its original 2-3 entries —
// requested explicitly after live evidence that English (23 curated categories, deep
// subcategory trees) was completing far more real page fetches per unit time than any of these
// smaller editions at the exact same independent pace: with only 2-3 categories each, a smaller
// edition's own category walk exhausted and started cycling back through already-known
// categories much faster, so a much larger share of its cycles went to low-yield discovery
// churn (empty-streak counting, random-page fallback) instead of productive page fetches, not
// because of any unfair scheduling — each source already runs fully independently. More
// categories per edition means more always-available real work, the same advantage English
// already had. Most of these additional category names were confirmed live to have real
// members via the MediaWiki API the same way the original ones were; a few of the less common
// occupation categories on the smaller editions (Hungarian/Danish/Norwegian/Finnish specifically)
// couldn't all be individually re-confirmed in one sitting once live verification itself started
// getting rate-limited — a wrong or empty category name here is harmless (the discovery walk
// just finds 0 members and moves on, via the same `ON CONFLICT DO NOTHING` safety net already in
// place), so not worth blocking on perfect verification for every single entry.
var wikiquoteFR = wikiquoteSite{
	source:         "wikiquote-fr",
	language:       "fr",
	apiBase:        "https://fr.wikiquote.org/w/api.php",
	wikiBase:       "https://fr.wikiquote.org/wiki/",
	categoryPrefix: "Catégorie:",
	curatedCategories: []string{
		"Écrivain", "Philosophe", "Scientifique", "Acteur", "Actrice", "Homme politique",
		"Musicien", "Poète", "Historien", "Artiste", "Journaliste",
	},
}

var wikiquoteES = wikiquoteSite{
	source:         "wikiquote-es",
	language:       "es",
	apiBase:        "https://es.wikiquote.org/w/api.php",
	wikiBase:       "https://es.wikiquote.org/wiki/",
	categoryPrefix: "Categoría:",
	curatedCategories: []string{
		"Escritores", "Filósofos", "Científicos", "Actores", "Actrices", "Políticos",
		"Músicos", "Poetas", "Historiadores", "Artistas", "Periodistas",
	},
}

var wikiquoteIT = wikiquoteSite{
	source:         "wikiquote-it",
	language:       "it",
	apiBase:        "https://it.wikiquote.org/w/api.php",
	wikiBase:       "https://it.wikiquote.org/wiki/",
	categoryPrefix: "Categoria:",
	curatedCategories: []string{
		"Scrittori", "Filosofi", "Scienziati", "Attori", "Politici", "Musicisti", "Poeti",
		"Storici", "Artisti", "Giornalisti",
	},
}

var wikiquotePT = wikiquoteSite{
	source:         "wikiquote-pt",
	language:       "pt",
	apiBase:        "https://pt.wikiquote.org/w/api.php",
	wikiBase:       "https://pt.wikiquote.org/wiki/",
	categoryPrefix: "Categoria:",
	curatedCategories: []string{
		"Escritores", "Filósofos", "Atores", "Atrizes", "Políticos", "Músicos", "Poetas",
		"Historiadores", "Artistas", "Jornalistas",
	},
}

var wikiquotePL = wikiquoteSite{
	source:         "wikiquote-pl",
	language:       "pl",
	apiBase:        "https://pl.wikiquote.org/w/api.php",
	wikiBase:       "https://pl.wikiquote.org/wiki/",
	categoryPrefix: "Kategoria:",
	curatedCategories: []string{
		"Pisarze", "Filozofowie", "Aktorzy", "Politycy", "Muzycy", "Poeci", "Historycy",
		"Artyści", "Dziennikarze",
	},
}

// wikiquoteSV / wikiquoteRO / wikiquoteCS / wikiquoteHU — four more major European languages,
// requested explicitly ("big European languages, not niche"). Dutch was checked and deliberately
// skipped: its Wikiquote pages use a genuinely different, incompatible layout (each quote nested
// under "Origineel in het ...:"/"Aanhaling(en):" labels rather than a flat quote list), and its
// corpus is tiny (1,317 articles) — not worth a dedicated structure for. Russian/Ukrainian were
// also considered but skipped: both are Cyrillic-script, which dedup.IsLatinScript would reject
// outright — supporting them is a real architecture change (a second, non-Latin detector model
// set, doubling memory per the tradeoff already documented in language.go), not a drop-in
// addition like these four.
var wikiquoteSV = wikiquoteSite{
	source:         "wikiquote-sv",
	language:       "sv",
	apiBase:        "https://sv.wikiquote.org/w/api.php",
	wikiBase:       "https://sv.wikiquote.org/wiki/",
	categoryPrefix: "Kategori:",
	curatedCategories: []string{
		"Filosofer", "Författare", "Skådespelare", "Politiker", "Poeter", "Historiker",
		"Konstnärer", "Journalister",
	},
}

var wikiquoteRO = wikiquoteSite{
	source:         "wikiquote-ro",
	language:       "ro",
	apiBase:        "https://ro.wikiquote.org/w/api.php",
	wikiBase:       "https://ro.wikiquote.org/wiki/",
	categoryPrefix: "Categorie:",
	curatedCategories: []string{
		"Scriitori", "Filozofi", "Actori", "Politicieni", "Muzicieni", "Poeți", "Istorici",
		"Artiști",
	},
}

var wikiquoteCS = wikiquoteSite{
	source:         "wikiquote-cs",
	language:       "cs",
	apiBase:        "https://cs.wikiquote.org/w/api.php",
	wikiBase:       "https://cs.wikiquote.org/wiki/",
	categoryPrefix: "Kategorie:",
	curatedCategories: []string{
		"Spisovatelé", "Filozofové", "Herci", "Politici", "Hudebníci", "Básníci",
	},
}

var wikiquoteHU = wikiquoteSite{
	source:         "wikiquote-hu",
	language:       "hu",
	apiBase:        "https://hu.wikiquote.org/w/api.php",
	wikiBase:       "https://hu.wikiquote.org/wiki/",
	categoryPrefix: "Kategória:",
	curatedCategories: []string{
		"Írók", "Filozófusok", "Színészek", "Politikusok", "Zenészek", "Költők",
		"Történészek",
	},
}

// wikiquoteDA / wikiquoteNO / wikiquoteFI — Danish, Norwegian (Bokmål, the edition at
// no.wikiquote.org), and Finnish. Dutch was checked and skipped: confirmed live (two separate
// pages) that Dutch Wikiquote gives quotes in their *original* language (German/English/etc.)
// with Dutch-only editorial labels around them ("Origineel in het Duits:", "Bron:",
// "Aanhaling(en):") rather than a Dutch translation — a genuinely different extraction problem
// (identify and extract only the Dutch-original entries, skip the rest), not a drop-in fit for
// this generalized parser, and not worth building separately for a 1,317-article edition.
var wikiquoteDA = wikiquoteSite{
	source:         "wikiquote-da",
	language:       "da",
	apiBase:        "https://da.wikiquote.org/w/api.php",
	wikiBase:       "https://da.wikiquote.org/wiki/",
	categoryPrefix: "Kategori:",
	curatedCategories: []string{
		"Filosoffer", "Forfattere", "Skuespillere", "Politikere", "Musikere",
	},
}

var wikiquoteNO = wikiquoteSite{
	source:         "wikiquote-no",
	language:       "no",
	apiBase:        "https://no.wikiquote.org/w/api.php",
	wikiBase:       "https://no.wikiquote.org/wiki/",
	categoryPrefix: "Kategori:",
	curatedCategories: []string{
		"Filosofer", "Forfattere", "Skuespillere", "Politikere", "Musikere",
	},
}

var wikiquoteFI = wikiquoteSite{
	source:         "wikiquote-fi",
	language:       "fi",
	apiBase:        "https://fi.wikiquote.org/w/api.php",
	wikiBase:       "https://fi.wikiquote.org/wiki/",
	categoryPrefix: "Luokka:",
	curatedCategories: []string{
		"Kirjailijat", "Filosofit", "Näyttelijät", "Poliitikot", "Muusikot",
	},
}

type wikiquoteCategoryMembersResponse struct {
	Continue struct {
		CMContinue string `json:"cmcontinue"`
	} `json:"continue"`
	Query struct {
		CategoryMembers []struct {
			Title string `json:"title"`
			Type  string `json:"type"` // "page" or "subcat" — see cmprop=type below
		} `json:"categorymembers"`
	} `json:"query"`
}

// fetchWikiquoteCategoryMembers calls a Wikiquote edition's MediaWiki API
// (action=query&list=categorymembers) to list both real article titles *and* subcategories
// directly in category, in a single request (cmtype=page|subcat, cmprop=title|type so each
// member says which it is) — confirmed live this is supported and meaningfully cheaper than
// two separate calls (one cmtype=page, one cmtype=subcat), which is what an earlier version of
// this function did. Halving the round-trips this way is a real throughput win that costs
// nothing in politeness — the per-source cooldown between *calls* is unchanged, this only
// reduces how many calls a single category needs. Pages are what's targeted for quotes;
// subcategories are what let discovery go deeper than one level (see wikiquoteDiscoveryCursor).
// Subcategory titles come back with the namespace prefix included (e.g. "Category:Gay
// writers"); site.categoryPrefix is stripped so they're ready to pass straight back into this
// same function as a plain category name, the same shape as curatedCategories' own entries.
//
// cmContinue is the previous call's continuation token for this category (empty to start it
// from the beginning); the returned token is empty once the category is fully walked.
func fetchWikiquoteCategoryMembers(ctx context.Context, site wikiquoteSite, category string, cmContinue string) (pages []string, subcats []string, nextCMContinue string, err error) {
	apiURL := fmt.Sprintf(
		"%s?action=query&list=categorymembers&cmtitle=%s&cmtype=page%%7Csubcat&cmprop=title%%7Ctype&cmlimit=%d&format=json",
		site.apiBase, url.QueryEscape(site.categoryPrefix+category), wikiquoteCategoryBatchSize,
	)
	if cmContinue != "" {
		apiURL += "&cmcontinue=" + url.QueryEscape(cmContinue)
	}

	body, err := fetcher.Fetch(ctx, apiURL)
	if err != nil {
		return nil, nil, "", err
	}

	var parsed wikiquoteCategoryMembersResponse
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		return nil, nil, "", err
	}

	for _, m := range parsed.Query.CategoryMembers {
		if m.Type == "subcat" {
			subcats = append(subcats, strings.TrimPrefix(m.Title, site.categoryPrefix))
		} else {
			pages = append(pages, m.Title)
		}
	}
	return pages, subcats, parsed.Continue.CMContinue, nil
}

type wikiquoteAllPagesResponse struct {
	Continue struct {
		APContinue string `json:"apcontinue"`
	} `json:"continue"`
	Query struct {
		AllPages []struct {
			Title string `json:"title"`
		} `json:"allpages"`
	} `json:"query"`
}

// fetchWikiquoteAllPages lists the next wikiquoteCategoryBatchSize article titles of an edition
// in title order (action=query&list=allpages, main namespace, redirects excluded), starting at
// apContinue ("" for the beginning). The returned token is "" once the last article is reached.
// This is the exhaustive fallback behind the curated category walk: it sees every article,
// including ones no curated category reaches. It also sees non-person pages (films, topics);
// those parse to 0 quotes and cost one fetch each, which beats a worker sitting idle.
func fetchWikiquoteAllPages(ctx context.Context, site wikiquoteSite, apContinue string) (titles []string, next string, err error) {
	apiURL := fmt.Sprintf(
		"%s?action=query&list=allpages&apnamespace=0&apfilterredir=nonredirects&aplimit=%d&format=json",
		site.apiBase, wikiquoteCategoryBatchSize,
	)
	if apContinue != "" {
		apiURL += "&apcontinue=" + url.QueryEscape(apContinue)
	}

	body, err := fetcher.Fetch(ctx, apiURL)
	if err != nil {
		return nil, "", err
	}

	var parsed wikiquoteAllPagesResponse
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		return nil, "", err
	}

	titles = make([]string, 0, len(parsed.Query.AllPages))
	for _, p := range parsed.Query.AllPages {
		titles = append(titles, p.Title)
	}
	return titles, parsed.Continue.APContinue, nil
}

// wikiquoteTitleToURL converts a MediaWiki page title, as returned by the API (spaces, not
// underscores, and not URL-escaped), into that page's real URL on site.
func wikiquoteTitleToURL(site wikiquoteSite, title string) string {
	return site.wikiBase + url.PathEscape(strings.ReplaceAll(title, " ", "_"))
}

// wikiquoteDiscoveryCursor packs all discovery state into the single string
// db.GetDiscoveryCursor/SetDiscoveryCursor already persist (keyed by site.source, so English
// and German each get their own independent cursor), avoiding a schema change.
//
// CategoryIndex picks the top-level curated category once Current is empty (a fresh branch).
// Current, when set, is whatever category is actively being walked right now — either a
// top-level curated category or a subcategory discovered under it, since discovery goes deeper
// than one level (see below). CMContinue paginates whichever category is currently being
// walked; fetchWikiquoteCategoryMembers returns both that category's own pages *and* its direct
// subcategories in the same call, so there's no separate "now check subcategories" phase to
// track — once CMContinue comes back empty, the category is simply done, both dimensions.
//
// Any newly found subcategories are appended to Queue (a FIFO — breadth-first within this
// branch, so one path doesn't spiral arbitrarily deep before sibling subcategories get a turn)
// and recorded in Visited (so a diamond-shaped or cyclic category graph doesn't get walked
// twice, or loop forever). Confirmed live that meaningful subcategories exist even under a
// well-mined top-level category (Category:Writers → "Writers by language," "Gay writers,"
// "Legal writers," ...), so stopping at one level left real content undiscovered. Queue/Visited
// are scoped to the *current* top-level branch, not kept forever — they reset to nil the moment
// CategoryIndex advances to the next top-level category, since a subcategory name from one
// branch has no relevance once that branch is done.
//
// Every topUp call makes exactly one HTTP request and is exactly one topUpResult — this matters
// for correctness, not just tidiness: runFeeder only applies its delay once per
// topUp call, so a call that quietly made two HTTP requests back-to-back internally would
// reintroduce the same unthrottled-discovery-burst bug that caused a live 429 earlier.
type wikiquoteDiscoveryCursor struct {
	CategoryIndex int      `json:"categoryIndex"`
	Current       string   `json:"current,omitempty"`
	CMContinue    string   `json:"cmContinue,omitempty"`
	Queue         []string `json:"queue,omitempty"`
	Visited       []string `json:"visited,omitempty"`
}

func decodeWikiquoteCursor(raw string) wikiquoteDiscoveryCursor {
	var c wikiquoteDiscoveryCursor
	if raw == "" {
		return c
	}
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		return wikiquoteDiscoveryCursor{}
	}
	return c
}

func (c wikiquoteDiscoveryCursor) encode() string {
	b, _ := json.Marshal(c)
	return string(b)
}
