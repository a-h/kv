package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/a-h/kv"
)

type StreamGetCommand struct {
	Offset int `arg:"-o,--offset" help:"Range offset." default:"0"`
	Limit  int `arg:"-l,--limit" help:"The maximum number of records to return, or -1 for no limit." default:"1000"`
}

func (c *StreamGetCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	data, err := store.Stream(ctx, c.Offset, c.Limit)
	if err != nil {
		return fmt.Errorf("failed to list data: %w", err)
	}

	type streamRecordDisplay struct {
		Seq    int                         `json:"seq"`
		Action kv.Action                   `json:"action"`
		Record kv.RecordOf[map[string]any] `json:"record"`
	}
	var displayRecords []streamRecordDisplay
	for _, record := range data {
		typedRecords, err := kv.RecordsOf[map[string]any]([]kv.Record{record.Record})
		if err != nil {
			return fmt.Errorf("failed to convert record: %w", err)
		}
		srd := streamRecordDisplay{
			Seq:    record.Seq,
			Action: record.Action,
			Record: typedRecords[0],
		}
		displayRecords = append(displayRecords, srd)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err = enc.Encode(displayRecords); err != nil {
		return err
	}

	// Write the updated offset.
	_, err = fmt.Printf("kv --type %q --connection %q records %d %d\n", g.Type, g.Connection, c.Offset+len(data), c.Limit)
	return err
}
