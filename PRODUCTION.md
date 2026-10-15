# Production deployment

`docker-compose.yml` deploys the crawler on its own. It deliberately has **no**
`postgres`/`redis` service blocks — it deploys against Postgres and Redis that already exist
elsewhere (e.g. Dokploy Database resources you manage and back up separately), via
`DATABASE_URL`/`REDIS_ADDR`. `docker-compose.dev.yml` is the local-dev Postgres/Redis only.

This project was split out of the Vroomy monorepo (`backend/scraper`), git history included.

## Contract with consumers

The crawler's only interface to anything else is the **Postgres database itself**. It owns the
schema (`quotes`, `url_frontier`), self-migrates on startup (goose, default
`goose_db_version` table), and is the only writer. Consumers just point their own
`DATABASE_URL` at the same database and read.

Vroomy (`api` and `ws`) currently depends on these `quotes` columns — treat them as a public
interface and don't rename/drop them without updating Vroomy first:

| Column            | Used for                                                          |
| ----------------- | ----------------------------------------------------------------- |
| `id`              | random-pick by `id >= random` scan                                |
| `text`            | the passage                                                       |
| `author`          | shown with the passage                                            |
| `source`          | shown with the passage                                            |
| `language`        | filter (`en`, `de`, …)                                            |
| `word_count`      | filter (`BETWEEN minWords AND maxWords`)                          |
| `game_unsuitable` | filter (`= false`) — see `internal/scoring` / `cmd/backfill`      |

Redis is private to the crawler — nothing else reads it, and it's fully rebuildable from
Postgres on every start (`WarmSimhashCache`, `WarmFrontierCache`).

The crawler must have started at least once against a fresh database before any consumer
relies on the `quotes` table existing.

## 1. Required environment variables

```
DATABASE_URL  — the shared quotes Postgres connection string
REDIS_ADDR    — the crawler's own Redis (redis://... form is fine directly — see below)
```

`.env.prod.example` documents both — copy it to `.env.prod` (gitignored) if deploying with plain
`docker compose`. `REDIS_DB`, `GOMEMLIMIT` and `LOG_LEVEL` are already set in the compose file.

## 2. Bring it up (plain `docker compose`, no Dokploy)

```bash
docker compose --env-file .env.prod up -d --build
docker compose --env-file .env.prod logs -f
```

`restart: unless-stopped`, no ports, no healthcheck — it's a background worker with no HTTP
server; its logs are the signal to watch. Stops cleanly on `SIGTERM`.

The compose file pins `networks.default` to the external `dokploy-network` (see finding #3). Off
Dokploy, either create that network (`docker network create dokploy-network`) or drop the
`networks:` block and make sure your Postgres/Redis are reachable from the default network.

## 3. Backups

If Postgres is managed by an orchestrator with its own backup scheduler (e.g. Dokploy's
per-database scheduled backups to S3-compatible storage), use that. Otherwise:

```bash
POSTGRES_USER=quotes POSTGRES_DB=quotes POSTGRES_CONTAINER=quotes-postgres ./scripts/backup.sh
```

Dumps to `./backups/quotes_<timestamp>.sql.gz` and prunes anything older than 14 days
(`RETENTION_DAYS`, `BACKUP_DIR` are overridable). Schedule it with cron:

```cron
0 3 * * * cd /path/to/quotes-crawler && POSTGRES_USER=quotes POSTGRES_DB=quotes POSTGRES_CONTAINER=quotes-postgres ./scripts/backup.sh >> /var/log/quotes-backup.log 2>&1
```

Local disk only — point `BACKUP_DIR` somewhere synced offsite for real disaster recovery.

## 4. Deploying via Dokploy

- Deploy as a **Compose** project with compose path `docker-compose.yml` (Dokploy's default). No Domain — there
  is nothing to route to.
- Set `DATABASE_URL`/`REDIS_ADDR` under **Environment Settings** with **"Create Environment
  File"** enabled — with it off, every `${VAR}` silently resolves to an empty string. Saving env
  vars alone doesn't touch running containers; trigger a real **redeploy**.
- **`REDIS_ADDR` accepts Dokploy's internal Redis URL directly** (`redis://...`) — `ConnectRedis`
  switches to URL parsing for anything containing `://`, so `REDIS_PASSWORD`/`REDIS_DB` are
  unused in that case.
- **Memory**: 2 GB hard limit, `GOMEMLIMIT=1536MiB`, both in the compose file (see finding #2).
  A memory-limit kill is a blip, not an incident: Redis is rehydrated from Postgres on start and
  `db.RequeueStuckInProgress` recovers URLs a killed process left mid-fetch.
- **On a small host (4–5 GB), add a swapfile** so a transient spike pages out instead of OOM-killing:
  ```bash
  sudo fallocate -l 2G /swapfile && sudo chmod 600 /swapfile
  sudo mkswap /swapfile && sudo swapon /swapfile
  echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
  ```
- **Day/night scheduling = stop/start the container**, not an in-app pause. An earlier in-app
  active-hours pause was reverted: it cut CPU/network but never freed the ~1.25 GB the language
  models hold. Now that the crawler is its own Compose project, Dokploy can stop/start it without
  touching anything else.
- **`LOG_LEVEL`**: `info` (unset) logs everything; `warn` drops per-page/per-quote narration but
  keeps skips/retries/backoffs; `error` keeps genuine failures only. The compose file sets
  `error`.
- **Log size** is capped at 10 MB × 3 via the compose `x-logging` anchor.

## Findings from live testing

1. **`REDIS_ADDR=redis:6379` silently connected to the wrong host.** `redis.ParseURL("redis:6379")`
   parses `"redis"` as a scheme and yields `Addr="localhost:6379"`. Invisible in dev because
   `REDIS_ADDR` there really is `localhost:6379`. Fixed in `internal/db/redis.go`.
2. **Real memory need is ~1.25 GB**, not ~250 MB. The language detector's models (~330 MB even
   restricted to Latin-script languages) plus crawl overhead plateau there. Limits were
   **measured** (limit removed, usage observed over a sustained run). Setting `GOMEMLIMIT` below
   real need just made the GC burn 400–600% CPU before OOMing anyway.
3. **A plain Compose deploy lands on its own isolated network**, with no route to Dokploy's
   Database resources on `dokploy-network` — hostnames fail DNS resolution (easy to misread as a
   credentials problem). Fixed by pinning `networks.default` to the external `dokploy-network`.

If you change what the crawler does in a way that could shift its memory profile, re-measure
with `docker stats` against an unconstrained container rather than trusting these numbers.
