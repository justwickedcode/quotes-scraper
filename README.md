# quotes-crawler

A polite, long-running web crawler in Go that builds a multilingual corpus of quotes from
Goodreads and 14 Wikiquote language editions. It powers the passages in
[Vroomy](https://vroomy.justwickedcode.dev), a multiplayer typing game.

**Go · PostgreSQL · Redis · goquery · lingua-go · goose · Docker**

## Highlights

- **Concurrent per-source pipelines.** Each source gets a fetch worker and a discovery feeder
  goroutine. A slow or throttled source never blocks the others.
- **Self-sustaining discovery.** Feeders keep each queue stocked ahead of the worker. Wikiquote
  editions pull random article batches from the MediaWiki API; Goodreads walks pagination, tags,
  keyword searches, and author names harvested from the rest of the corpus.
- **Adaptive politeness.** Per-source pacing that speeds up while responses stay fast and backs
  off on slow responses or 429s, with a shared pause so discovery and fetching never pile onto a
  throttled host.
- **Two-layer deduplication.** Exact matches via SHA-256 on normalized text, near-duplicates via
  64-bit SimHash with LSH banding in Redis, so an insert checks a handful of candidates rather
  than millions of rows.
- **Content quality filters.** Script and language verification, length limits, and heuristics
  that drop dictionary entries, wiki markup and talk-page noise. Quotes unfit for a typing game
  (too short, poem formatting, untypeable characters, excluded authors) are flagged, not
  deleted.
- **Crash-safe state.** Postgres is the source of truth. Redis queues and caches are rebuilt on
  startup, and URLs orphaned by a crash are requeued automatically.

## How it works

```
per source (×15)
┌──────────────────────────────────────────────────────┐
│  feeder ──► frontier (Redis ZSET) ──► worker         │
│  discovery     priority queue         paced fetches  │
└──────────────────────────────────────┬───────────────┘
                                       ▼
              parse ─► filter ─► dedup (SHA-256 + SimHash/LSH) ─► Postgres
```

1. **Frontier.** Every URL is recorded once in `url_frontier` (Postgres) and queued in a
   per-source Redis sorted set. Goodreads URLs are scored by page type, so deep pagination
   chains, where unseen quotes live, run before popular first pages. Chains that keep returning
   duplicates are demoted.
2. **Feeding.** Each feeder keeps its queue between 10 and 30 URLs. When a queue drops to 10,
   the source enters refill mode: the worker pauses and discovery uses its request budget until
   50 new URLs are queued.
3. **Fetching and parsing.** Each source has a parser behind a common interface. Wikiquote's
   page layout differs by language, so English, German and a shared localized parser cover all
   14 editions.
4. **Saving.** Quotes pass the quality filters, then exact and near-duplicate checks, and are
   batch-inserted. Failed fetches retry with backoff, up to 3 attempts.

## Sources

| Source | Coverage | Discovery |
| --- | --- | --- |
| [Goodreads](https://www.goodreads.com/quotes) | Primary volume source | Pagination, tags, keyword search, author-name search |
| [Wikiquote](https://www.wikiquote.org) | en, de, fr, es, it, pt, pl, sv, ro, cs, hu, da, no, fi | MediaWiki `list=random`, 500 titles per call |

Only Latin-script editions are crawled, and every quote is checked against its edition's
language with [lingua-go](https://github.com/pemistahl/lingua-go). The crawler identifies
itself per the [Wikimedia User-Agent policy](https://foundation.wikimedia.org/wiki/Policy:User-Agent_policy)
and doesn't render JavaScript or bypass any access controls.

## Getting started

Requirements: Go 1.25+, Docker (or Podman).

```bash
cp .env.example .env                              # local defaults work as-is
docker compose -f docker-compose.dev.yml up -d    # Postgres + Redis
go run ./cmd/crawler                              # migrates the schema, seeds, starts crawling
```

The schema migrates automatically on startup, and each migration is logged before it runs.

| Variable | Purpose |
| --- | --- |
| `DATABASE_URL` | Postgres connection string. Add `pool_max_conns` to override the default pool of 20. |
| `REDIS_ADDR`, `REDIS_PASSWORD`, `REDIS_DB` | Redis connection |
| `LOG_LEVEL` | `info` (default), `warn` (recommended in production), or `error` |

## Testing

```bash
go test ./...                                              # unit + DB tests (testcontainers)
go test -tags=integration ./internal/crawler/... -count=1  # live fetches against real sites
```

Parsers are tested against saved HTML fixtures. Store tests run against real Postgres and Redis
in throwaway containers via [testcontainers-go](https://golang.testcontainers.org). The
integration suite needs `docker-compose.dev.yml` running.

## Project layout

```
cmd/
  crawler/      entry point
  backfill/     one-off job to recompute game-suitability flags
internal/
  crawler/      workers, feeders, pacing, per-source discovery
  fetcher/      shared HTTP client
  parser/       Goodreads and Wikiquote parsers
  dedup/        normalization, SHA-256, SimHash/LSH, quality and language filters
  scoring/      frontier priorities
  db/           Postgres/Redis store, embedded goose migrations
```

## Deployment

A multi-stage `Dockerfile` builds a static binary with migrations embedded. `docker-compose.yml`
runs the crawler with a private Redis against an existing Postgres. See
[PRODUCTION.md](PRODUCTION.md) for secrets, backups, memory sizing, and the schema contract with
the game's API.

## Excluding authors from the game

Quotes by authors on the `excluded_authors` list (leaders of genocidal regimes, terrorists) are
flagged and never served by the game. To change the list:

```sql
INSERT INTO excluded_authors VALUES ('some name', 'Some Name', 'reason');
SELECT refresh_excluded_authors();   -- re-applies the list to saved quotes
```
