package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type BenchmarkPatchCommand struct {
	N int `arg:"-n,--number" help:"Number of times to patch per worker" default:"250"`
	W int `arg:"-w,--workers" help:"Number of workers to use" default:"4"`
}

func (c *BenchmarkPatchCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}
	if err = store.Init(ctx); err != nil {
		return fmt.Errorf("failed to initialize store: %w", err)
	}

	initial := map[string]any{
		"count": 0,
	}
	if err := store.Put(ctx, "kv-benchmark-patch-key", -1, initial); err != nil {
		return fmt.Errorf("failed to put record: %w", err)
	}

	fmt.Printf("Patching record with %d workers...\n", c.W)

	var wg sync.WaitGroup

	workerCounts := make([]int, c.W)

	start := time.Now()
	for i := 0; i < c.W; i++ {
		wg.Add(1)
		go func(workerIndex int) {
			defer wg.Done()

			p := map[string]any{}
			for i := 0; i < c.N; i++ {
				workerCounts[workerIndex]++
				p[fmt.Sprintf("worker_%d", workerIndex)] = workerCounts[workerIndex]
				err := store.Patch(ctx, "kv-benchmark-patch-key", -1, p)
				if err != nil {
					fmt.Printf("error: %v\n", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	end := time.Now()

	timeTaken := end.Sub(start)
	opsPerSecond := float64(c.N) / timeTaken.Seconds()
	fmt.Printf("Complete, in %v, %v ops per second\n", end.Sub(start), opsPerSecond)

	// Validate that the worker counts add up.
	var data map[string]any
	_, _, err = store.Get(ctx, "kv-benchmark-patch-key", &data)
	if err != nil {
		return fmt.Errorf("failed to get updated value: %w", err)
	}
	for i := 0; i < c.W; i++ {
		var countForWorker int
		if fc, ok := data[fmt.Sprintf("worker_%d", i)].(float64); ok {
			countForWorker = int(fc)
		}
		if countForWorker != c.N {
			return fmt.Errorf("worker %d did not patch the correct number of times, expected %d, got %d", i, c.N, countForWorker)
		}
	}

	return nil
}
