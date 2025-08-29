package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/a-h/kv"
)

type TaskNewCommand struct {
	Name           string `arg:"" help:"The task name." required:"true"`
	MaxRetries     int    `help:"Maximum number of retries." default:"3"`
	TimeoutSeconds int    `help:"Task timeout in seconds." default:"300"`
	ScheduledFor   string `help:"When to run the task (RFC3339 format). Defaults to now." optional:""`
}

func (c *TaskNewCommand) Run(ctx context.Context, g GlobalFlags) error {
	scheduler, err := g.Scheduler()
	if err != nil {
		return err
	}

	payloadBytes, err := io.ReadAll(os.Stdin)
	if err != nil {
		return fmt.Errorf("failed to read payload from stdin: %w", err)
	}

	scheduledFor := time.Now().UTC()
	if c.ScheduledFor != "" {
		scheduledFor, err = time.Parse(time.RFC3339, c.ScheduledFor)
		if err != nil {
			return fmt.Errorf("invalid scheduled_for time format (use RFC3339): %w", err)
		}
	}
	task := kv.NewScheduledTask(c.Name, payloadBytes, scheduledFor)

	// Override defaults with user-specified values.
	if c.MaxRetries > 0 {
		task.MaxRetries = c.MaxRetries
	}
	if c.TimeoutSeconds > 0 {
		task.TimeoutSeconds = c.TimeoutSeconds
	}

	if err := scheduler.New(ctx, task); err != nil {
		return fmt.Errorf("failed to create task: %w", err)
	}

	fmt.Printf("Created task with ID: %s\n", task.ID)
	return nil
}
