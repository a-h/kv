package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/a-h/kv"
)

type StreamGetCommand struct {
	Seq   int `arg:"" help:"Sequence number to start from." default:"0"`
	Limit int `arg:"" help:"The maximum number of records to return, or -1 for no limit." default:"1000"`
}

func (c *StreamGetCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	data, err := store.Stream(ctx, kv.TypeAll, c.Seq, c.Limit)
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
	lastSeq := c.Seq
	if len(data) > 0 {
		lastSeq = data[len(data)-1].Seq
	}
	_, err = fmt.Printf("kv --type %q --connection %q stream get %d %d\n", g.Type, g.Connection, lastSeq+1, c.Limit)
	return err
}
