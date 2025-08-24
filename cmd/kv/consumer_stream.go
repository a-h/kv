package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/a-h/kv"
)

type ConsumerStreamCommand struct {
	StreamName   string `arg:"" help:"The stream name." required:"true"`
	ConsumerName string `arg:"" help:"The consumer name." required:"true"`
	OfType       string `help:"The type of records to fetch." default:"all" enum:"all,put,delete"`
	CommitMode   string `help:"Commit mode: 'none' (no commits), 'batch' (commit after each batch), 'all' (commit after each record)." enum:"none,batch,all" default:"none"`
	Limit        int    `help:"The maximum number of records to fetch per batch." default:"10"`
}

func (c *ConsumerStreamCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	consumer := kv.NewStreamConsumer(ctx, store, c.StreamName, c.ConsumerName, kv.Type(c.OfType))
	consumer.Limit = c.Limit
	consumer.CommitMode = kv.CommitMode(c.CommitMode)

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")

	type recordDisplay struct {
		Key     string `json:"key"`
		Version int    `json:"version"`
		Value   any    `json:"value"`
		Created any    `json:"created"`
	}
	type consumerRecordDisplay struct {
		Seq    int           `json:"seq"`
		Action string        `json:"action"`
		Record recordDisplay `json:"record"`
	}

	for record, err := range consumer.Read(ctx) {
		if err != nil {
			return err
		}

		typedRecords, err := kv.RecordsOf[map[string]any]([]kv.Record{record.Record})
		if err != nil {
			return fmt.Errorf("failed to convert record: %w", err)
		}

		displayRecord := consumerRecordDisplay{
			Seq:    record.Seq,
			Action: string(record.Action),
			Record: recordDisplay{
				Key:     typedRecords[0].Key,
				Version: typedRecords[0].Version,
				Value:   typedRecords[0].Value,
				Created: typedRecords[0].Created,
			},
		}
		if err := enc.Encode(displayRecord); err != nil {
			return err
		}
	}

	return nil
}
