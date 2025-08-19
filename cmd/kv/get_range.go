package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/a-h/kv"
)

type GetRangeCommand struct {
	From   string `arg:"" help:"Start of the range." required:""`
	To     string `arg:"" help:"End of the range (exclusive)." required:""`
	Offset int    `arg:"-o,--offset" help:"Range offset." default:"0"`
	Limit  int    `arg:"-l,--limit" help:"The maximum number of records to return, or -1 for no limit." default:"1000"`
}

func (c *GetRangeCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	data, err := store.GetRange(ctx, c.From, c.To, c.Offset, c.Limit)
	if err != nil {
		return fmt.Errorf("failed to get data: %w", err)
	}

	records, err := kv.RecordsOf[map[string]any](data)
	if err != nil {
		return fmt.Errorf("failed to convert records: %w", err)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(records)
}
