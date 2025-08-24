package main

import (
	"context"
	"encoding/json"
	"os"

	"github.com/a-h/kv"
)

type ConsumerGetCommand struct {
	Name  string `arg:"" help:"The consumer name." required:"true"`
	Type  string `help:"The type of records to fetch." default:"all"`
	Limit int    `help:"The maximum number of records to fetch." default:"10"`
}

func (c *ConsumerGetCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return err
	}
	records, err := kv.GetStreamConsumerRecords(ctx, store, c.Name, kv.Type(c.Type), c.Limit)
	if err != nil {
		return err
	}
	// Decode record values to JSON, matching stream get CLI behavior.
	type recordDisplay struct {
		Key     string      `json:"key"`
		Version int         `json:"version"`
		Value   interface{} `json:"value"`
		Created interface{} `json:"created"`
	}
	type consumerRecordDisplay struct {
		Seq    int           `json:"seq"`
		Action string        `json:"action"`
		Record recordDisplay `json:"record"`
	}
	var displayRecords []consumerRecordDisplay
	for _, r := range records {
		var value interface{}
		_ = json.Unmarshal(r.Record.Value, &value)
		displayRecords = append(displayRecords, consumerRecordDisplay{
			Seq:    r.Seq,
			Action: string(r.Action),
			Record: recordDisplay{
				Key:     r.Record.Key,
				Version: r.Record.Version,
				Value:   value,
				Created: r.Record.Created,
			},
		})
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(displayRecords)
}
