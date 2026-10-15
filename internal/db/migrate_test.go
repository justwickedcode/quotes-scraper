package db

import (
	"context"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// A restart against an already-migrated database must succeed: goose reports "nothing pending"
// as ErrNoMigrationFiles, which once crash-looped production on every start.
func TestMigrateIsIdempotent(t *testing.T) {
	ctx := context.Background()
	pgContainer, err := postgres.Run(ctx,
		"postgres:16",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2)),
	)
	if err != nil {
		t.Fatalf("could not start postgres container: %v", err)
	}
	defer func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Fatalf("could not terminate postgres container: %v", err)
		}
	}()

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := ConnectPostgres(connStr)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	if err := Migrate(pool); err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	if err := Migrate(pool); err != nil {
		t.Fatalf("second migrate (nothing pending): %v", err)
	}
}
