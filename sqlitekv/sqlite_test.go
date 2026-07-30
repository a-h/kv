package sqlitekv

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-h/kv/tests"
	"golang.org/x/sync/errgroup"
	"zombiezen.com/go/sqlite/sqlitex"
)

func newDiskPool(b *testing.B) (*sqlitex.Pool, func()) {
	b.Helper()
	tmpDir, err := os.MkdirTemp("", "sqlite_bench")
	if err != nil {
		b.Fatal(err)
	}
	pool, err := NewPool(filepath.Join(tmpDir, "bench.db"), sqlitex.PoolOptions{})
	if err != nil {
		os.RemoveAll(tmpDir)
		b.Fatal(err)
	}
	return pool, func() {
		pool.Close()
		os.RemoveAll(tmpDir)
	}
}

func BenchmarkPut(b *testing.B) {
	pool, cleanup := newDiskPool(b)
	defer cleanup()

	store := NewStore(pool)
	ctx := context.Background()
	if err := store.Init(ctx); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := range b.N {
		key := fmt.Sprintf("bench-key-%d", i)
		value := map[string]any{"index": i, "data": "some benchmark data"}
		if err := store.Put(ctx, key, -1, value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutConcurrent(b *testing.B) {
	pool, cleanup := newDiskPool(b)
	defer cleanup()

	store := NewStore(pool)
	ctx := context.Background()
	if err := store.Init(ctx); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i int
		for pb.Next() {
			keyBytes := make([]byte, 16)
			if _, err := rand.Read(keyBytes); err != nil {
				b.Fatal(err)
			}
			key := fmt.Sprintf("bench-key-%x-%d", keyBytes, i)
			value := map[string]any{"index": i, "data": "some benchmark data"}
			if err := store.Put(ctx, key, -1, value); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

func BenchmarkMixed(b *testing.B) {
	pool, cleanup := newDiskPool(b)
	defer cleanup()

	store := NewStore(pool)
	ctx := context.Background()
	if err := store.Init(ctx); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := range b.N {
		key := fmt.Sprintf("bench-key-%d", i)
		value := map[string]any{"index": i, "data": "some benchmark data"}
		if err := store.Put(ctx, key, -1, value); err != nil {
			b.Fatal(err)
		}
		var retrieved map[string]any
		if _, _, err := store.Get(ctx, key, &retrieved); err != nil {
			b.Fatal(err)
		}
	}
}

func TestSqlite(t *testing.T) {
	pool, err := NewPool("file::memory:?mode=memory&cache=shared", sqlitex.PoolOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	store := NewStore(pool)
	scheduler := NewScheduler(pool)
	tests.Run(t, store, scheduler)
}

func TestSqliteConcurrentDiskOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	tmpDir, err := os.MkdirTemp("", "sqlite_concurrent_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	pool, err := NewPool(filepath.Join(tmpDir, "test.db"), sqlitex.PoolOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	store := NewStore(pool)

	ctx := context.Background()
	err = store.Init(ctx)
	if err != nil {
		t.Fatal(err)
	}

	var wg errgroup.Group

	for workerID := range 15 {
		wg.Go(func() error {
			for op := range 1000 {
				keyBytes := make([]byte, 32)
				_, err = rand.Read(keyBytes)
				if err != nil {
					return fmt.Errorf("worker %d operation %d failed to read random: %w", workerID, op, err)
				}
				key := fmt.Sprintf("worker/%d/op/%d/random/%x", workerID, op, keyBytes)

				// Put.
				if op%2 == 0 {
					value := map[string]any{
						"worker_id": workerID,
						"operation": op,
						"timestamp": time.Now().Unix(),
						"data":      fmt.Sprintf("some data for worker %d operation %d", workerID, op),
					}
					if err := store.Put(ctx, key, -1, value); err != nil {
						return fmt.Errorf("worker %d put operation %d failed: %w", workerID, op, err)
					}
					continue
				}

				// Get.
				var retrieved map[string]any
				_, _, err := store.Get(ctx, key, &retrieved)
				if err != nil {
					return fmt.Errorf("worker %d get operation %d failed: %w", workerID, op, err)
				}
			}
			return nil
		})
	}

	if err := wg.Wait(); err != nil {
		t.Fatal(err)
	}
}
