package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

type BenchmarkGetCommand struct {
	X int `arg:"-x,--number" help:"Number of items to put" default:"100"`
	N int `arg:"-n,--number" help:"Number of items to get from the set" default:"10000"`
	W int `arg:"-w,--workers" help:"Number of workers to use" default:"100"`
}

func (c *BenchmarkGetCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}
	if err = store.Init(ctx); err != nil {
		return fmt.Errorf("failed to initialize store: %w", err)
	}

	fmt.Printf("Putting %d initial records...\n", c.X)

	for i := range c.X {
		p := map[string]any{
			"key":      fmt.Sprintf("kv-benchmark-get-key-%d", i),
			"name":     fmt.Sprintf("Alice-%d", c.N),
			"age":      42,
			"address1": "1 The Street",
			"address2": "The Town",
			"address3": "The County",
			"address4": "The Country",
			"postcode": "AB1 2CD",
		}
		err := store.Put(ctx, p["key"].(string), -1, p)
		if err != nil {
			return fmt.Errorf("failed to put record: %w", err)
		}
	}

	fmt.Printf("Getting %d records with %d workers...\n", c.N, c.W)

	var wg sync.WaitGroup

	gets := make(chan string, c.W)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range c.N {
			gets <- fmt.Sprintf("kv-benchmark-get-key-%d", rand.IntN(c.X))
		}
		close(gets)
	}()

	start := time.Now()
	for range c.W {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var p map[string]any
			for key := range gets {
				_, _, err := store.Get(ctx, key, &p)
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
