package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
)

type TaskGetCommand struct {
	ID string `arg:"" help:"The task ID." required:"true"`
}

func (c *TaskGetCommand) Run(ctx context.Context, g GlobalFlags) error {
	scheduler, err := g.Scheduler()
	if err != nil {
		return err
	}

	task, ok, err := scheduler.Get(ctx, c.ID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if !ok {
		return fmt.Errorf("task not found: %s", c.ID)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(task); err != nil {
		return fmt.Errorf("failed to encode task: %w", err)
	}

	return nil
}
