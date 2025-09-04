package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
)

type GetBatchCommand struct {
	Keys []string `arg:"" help:"Keys to get. If none provided, keys will be read from stdin (one per line)."`
}

func (g *GetBatchCommand) Run(ctx context.Context, globals GlobalFlags) error {
	store, err := globals.Store()
	if err != nil {
		return err
	}

	if err := store.Init(ctx); err != nil {
		return err
	}

	keys := g.Keys
	if len(keys) == 0 {
		// Read keys from stdin.
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			key := strings.TrimSpace(scanner.Text())
			if key != "" {
				keys = append(keys, key)
			}
		}
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("error reading keys from stdin: %w", err)
		}
	}

	if len(keys) == 0 {
		return fmt.Errorf("no keys provided")
	}

	items, err := store.GetBatch(ctx, keys...)
	if err != nil {
		return err
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")

	keysFound := slices.Collect(maps.Keys(items))
	slices.Sort(keysFound)
	for _, key := range keysFound {
		if err := enc.Encode(items[key]); err != nil {
			return fmt.Errorf("error encoding item %q: %w", key, err)
		}
	}
	return nil
}
