package postgreskv

import (
	"context"
	"testing"

	"github.com/a-h/kv/tests"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgres(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, "postgres://postgres:secret@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		t.Fatalf("Unable to connect to database: %v\n", err)
	}
	defer pool.Close()

	store := NewStore(pool)
	scheduler := NewScheduler(pool)
	tests.Run(t, store, scheduler)
}
