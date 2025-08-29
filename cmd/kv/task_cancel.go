package main

import (
	"context"
	"fmt"
)

type TaskCancelCommand struct {
	ID string `arg:"" help:"The task ID." required:"true"`
}

func (c *TaskCancelCommand) Run(ctx context.Context, g GlobalFlags) error {
	scheduler, err := g.Scheduler()
	if err != nil {
		return err
	}
	if err := scheduler.Cancel(ctx, c.ID); err != nil {
		return fmt.Errorf("failed to cancel task: %w", err)
	}
	fmt.Printf("Task %s cancelled successfully\n", c.ID)
	return nil
}
