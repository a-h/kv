package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type BenchmarkPutCommand struct {
	N int `arg:"-n,--number" help:"Number of items to put" default:"30000"`
	W int `arg:"-w,--workers" help:"Number of workers to use" default:"100"`
}

func (c *BenchmarkPutCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}
	if err = store.Init(ctx); err != nil {
		return fmt.Errorf("failed to initialize store: %w", err)
	}

	fmt.Printf("Putting %d records with %d workers...\n", c.N, c.W)

	var wg sync.WaitGroup

	puts := make(chan map[string]any, c.W)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < c.N; i++ {
			puts <- map[string]any{
				"key":      fmt.Sprintf("kv-benchmark-put-key-%d", i),
				"name":     fmt.Sprintf("Alice-%d", c.N),
				"age":      42,
				"address1": "1 The Street",
				"address2": "The Town",
				"address3": "The County",
				"address4": "The Country",
				"postcode": "AB1 2CD",
			}
		}
		close(puts)
	}()

	start := time.Now()
	for i := 0; i < c.W; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range puts {
				err := store.Put(ctx, p["key"].(string), -1, p)
				if err != nil {
					fmt.Printf("error: %v\n", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	end := time.Now()

	timeTaken := end.Sub(start)
	opsPerSecond := float64(c.N) / timeTaken.Seconds()
	fmt.Printf("Complete, in %v, %v ops per second\n", end.Sub(start), opsPerSecond)

	return nil
}
