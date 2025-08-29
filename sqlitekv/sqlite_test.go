package sqlitekv

import (
	"testing"

	"github.com/a-h/kv/tests"
	"zombiezen.com/go/sqlite/sqlitex"
)

func TestSqlite(t *testing.T) {
	pool, err := sqlitex.NewPool("file::memory:?mode=memory&cache=shared", sqlitex.PoolOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	store := NewStore(pool)
	scheduler := NewScheduler(pool)
	tests.Run(t, store, scheduler)
}
