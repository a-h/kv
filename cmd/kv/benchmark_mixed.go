package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"time"
)

type BenchmarkMixedCommand struct {
	N int `arg:"-n,--number" help:"Number of operations to perform" default:"10000"`
	W int `arg:"-w,--workers" help:"Number of workers to use" default:"15"`
}

func (c *BenchmarkMixedCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}
	if err = store.Init(ctx); err != nil {
		return fmt.Errorf("failed to initialize store: %w", err)
	}

	fmt.Printf("Running %d mixed operations with %d workers...\n", c.N, c.W)

	var wg sync.WaitGroup

	operations := make(chan int, c.W)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range c.N {
			operations <- i
		}
		close(operations)
	}()

	start := time.Now()
	for workerID := range c.W {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for op := range operations {
				keyBytes := make([]byte, 32)
				_, err := rand.Read(keyBytes)
				if err != nil {
					fmt.Printf("worker %d operation %d failed to read random: %v\n", id, op, err)
					return
				}
				key := fmt.Sprintf("worker/%d/op/%d/random/%x", id, op, keyBytes)

				// Put.
				value := map[string]any{
					"worker_id": id,
					"operation": op,
					"timestamp": time.Now().Unix(),
					"data":      fmt.Sprintf("some data for worker %d operation %d", id, op),
				}
				if err := store.Put(ctx, key, -1, value); err != nil {
					fmt.Printf("worker %d put operation %d failed: %v\n", id, op, err)
					return
				}

				// Get operation.
				var retrieved map[string]any
				_, _, err = store.Get(ctx, key, &retrieved)
				if err != nil {
					fmt.Printf("worker %d get operation %d failed: %v\n", id, op, err)
					return
				}
			}
		}(workerID)
	}
	wg.Wait()
	end := time.Now()

	timeTaken := end.Sub(start)
	opsPerSecond := float64(c.N) / timeTaken.Seconds()
	fmt.Printf("Complete, in %v, %.2f ops per second\n", timeTaken, opsPerSecond)

	return nil
}
