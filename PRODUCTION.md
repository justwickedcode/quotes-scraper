# Production

`docker-compose.yml` runs the crawler and a private Redis. Postgres is not part of it: the
crawler connects to an existing database through `DATABASE_URL`. (`docker-compose.dev.yml` is
for local development only.)

## Schema contract

The database is the crawler's only interface. The crawler owns the schema, migrates it on
startup, and is the only writer. Consumers point their own `DATABASE_URL` at the same database
and read from it. Start the crawler once against a fresh database before anything else depends
on the tables.

The Vroomy API reads these `quotes` columns, so don't rename or drop them without updating it
first:

| Column | Used for |
| --- | --- |
| `id` | random selection (`id >= random`) |
| `text`, `author`, `source` | the passage and its attribution |
| `language` | filter (`en`, `de`, …) |
| `word_count` | length filter |
| `game_unsuitable` | filter (`= false`); also set for authors on the `excluded_authors` list |

Redis holds only rebuildable state (frontier queues, the SimHash index), so it runs with no
persistence and no password, on a network nothing else joins.

## Configuration

| Variable | Notes |
| --- | --- |
| `DATABASE_URL` | Required. See `.env.prod.example`. |
| `LOG_LEVEL` | `error` by default in the compose file; `warn` or `info` for more detail. |

`REDIS_ADDR`, `REDIS_DB` and `GOMEMLIMIT` are set in the compose file.

## Running

```bash
docker compose --env-file .env.prod up -d --build
docker compose --env-file .env.prod logs -f
```

It's a background worker: no ports, no HTTP server, and it shuts down cleanly on `SIGTERM`. To
run it only at certain times of day, stop and start the container.

The crawler joins the external `dokploy-network` to reach Postgres. Outside Dokploy, either
create that network (`docker network create dokploy-network`) or remove it from the compose file
and make Postgres reachable another way.

### Dokploy

- Deploy as a **Compose** project using `docker-compose.yml`. No domain is needed.
- Set `DATABASE_URL` under **Environment Settings** with **Create Environment File** enabled.
  Without it, `${VAR}` references resolve to empty strings. Changing variables requires a
  redeploy.

## Resources

- **Memory:** the crawler needs about 1.25 GB, most of it the language-detection models. The
  compose file sets a 2 GB limit and `GOMEMLIMIT=1536MiB`; setting `GOMEMLIMIT` lower only makes
  the GC thrash. If the container is killed anyway, nothing is lost: Redis is rebuilt from
  Postgres and in-flight URLs are requeued on the next start.
- **Small hosts (4–5 GB):** add a swapfile so a short spike pages out instead of getting killed.
- **Logs:** capped at 10 MB × 3 files.
- **Database connections:** at least 20 by default. Set `pool_max_conns` in `DATABASE_URL` to
  change it.

## Backups

Use your Postgres host's scheduled backups if it has them. Otherwise, `scripts/backup.sh` writes
a gzipped `pg_dump` to `./backups` and deletes dumps older than 14 days:

```bash
POSTGRES_USER=quotes POSTGRES_DB=quotes POSTGRES_CONTAINER=quotes-postgres ./scripts/backup.sh
```

Run it from cron, and point `BACKUP_DIR` at storage that's synced offsite.

## Migrations

Migrations run automatically on startup, and each pending one is logged before it starts.
While a migration runs, it can lock `quotes`. The largest so far (excluded authors) took about
5 seconds on 2.7M rows.
