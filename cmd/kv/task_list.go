package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/a-h/kv"
)

type TaskListCommand struct {
	Status string `help:"Filter by status (pending, running, completed, failed, cancelled)." default:""`
	Name   string `help:"Filter by task name." default:""`
	Offset int    `help:"Offset for pagination." default:"0"`
	Limit  int    `help:"Limit for pagination." default:"10"`
}

func (c *TaskListCommand) Run(ctx context.Context, g GlobalFlags) error {
	scheduler, err := g.Scheduler()
	if err != nil {
		return err
	}

	status := kv.TaskStatus(c.Status)
	tasks, err := scheduler.List(ctx, status, c.Name, c.Offset, c.Limit)
	if err != nil {
		return fmt.Errorf("failed to list tasks: %w", err)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")

	for _, task := range tasks {
		if err := enc.Encode(task); err != nil {
			return fmt.Errorf("failed to encode task: %w", err)
		}
	}

	return nil
}
