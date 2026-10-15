package db

import (
	"embed"
	"log"
	"path"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrate applies any pending migrations. Each one runs once: goose records applied versions in
// goose_db_version and skips them on every later start. The pending ones are logged up front,
// since some (like the excluded-authors backfill) hold a lock on quotes while they run.
func Migrate(pool *pgxpool.Pool) error {
	sqlDB := stdlib.OpenDBFromPool(pool)

	goose.SetBaseFS(migrations)

	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}

	current, err := goose.EnsureDBVersion(sqlDB)
	if err != nil {
		return err
	}
	pending, err := goose.CollectMigrations("migrations", current, goose.MaxVersion)
	if err != nil {
		return err
	}
	if len(pending) == 0 {
		log.Printf("Database schema is up to date (version %d)", current)
		return nil
	}
	for _, m := range pending {
		log.Printf("Migration pending: %s", path.Base(m.Source))
	}
	log.Printf("Starting %d database migration(s) — quotes may be locked until they finish", len(pending))

	start := time.Now()
	if err := goose.Up(sqlDB, "migrations"); err != nil {
		log.Printf("Database migration failed after %s: %s", time.Since(start).Round(time.Millisecond), err)
		return err
	}
	log.Printf("Database migration(s) finished in %s", time.Since(start).Round(time.Millisecond))
	return nil
}
