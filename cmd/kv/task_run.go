package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/a-h/kv"
	"github.com/google/uuid"
)

type TaskRunCommand struct {
	RunnerID     string        `help:"Runner ID (defaults to generated UUID)." optional:""`
	PollInterval time.Duration `help:"How often to check for tasks." default:"10s"`
	LockDuration time.Duration `help:"How long to hold task locks." default:"5m"`
	TaskTypes    []string      `help:"Task types this runner can handle. If empty, handles all types." optional:""`
}

func (c *TaskRunCommand) Run(ctx context.Context, g GlobalFlags) error {
	scheduler, err := g.Scheduler()
	if err != nil {
		return err
	}

	// Generate a runner ID if not provided.
	runnerID := c.RunnerID
	if runnerID == "" {
		runnerID = uuid.New().String()
	}

	fmt.Printf("Starting task runner with ID: %s\n", runnerID)

	// Set up signal handling for graceful shutdown.
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Create task runner.
	runner := kv.NewTaskRunner(scheduler, runnerID)
	runner.PollInterval = c.PollInterval
	runner.LockDuration = c.LockDuration

	// Register handlers - if task types are specified, only register those handlers.
	// Otherwise, register all available example handlers.
	availableHandlers := map[string]func(ctx context.Context, task kv.Task) error{
		"echo": func(_ context.Context, task kv.Task) error {
			fmt.Printf("Echo task executed: %s\n", string(task.Payload))
			return nil
		},
		"log": func(_ context.Context, task kv.Task) error {
			var data map[string]any
			if err := json.Unmarshal(task.Payload, &data); err != nil {
				return err
			}
			fmt.Printf("Log task executed: %+v\n", data)
			return nil
		},
		"process": func(_ context.Context, task kv.Task) error {
			fmt.Printf("Process task executed with payload: %s\n", string(task.Payload))
			// Simulate some work.
			time.Sleep(100 * time.Millisecond)
			return nil
		},
	}

	// Determine which handlers to register.
	handlersToRegister := availableHandlers
	if len(c.TaskTypes) > 0 {
		// Register only the specified task types.
		handlersToRegister = make(map[string]func(ctx context.Context, task kv.Task) error)
		for _, taskType := range c.TaskTypes {
			if handler, exists := availableHandlers[taskType]; exists {
				handlersToRegister[taskType] = handler
			} else {
				fmt.Printf("Warning: No handler available for task type '%s', skipping\n", taskType)
			}
		}
	}

	// Register the handlers.
	for taskType, handler := range handlersToRegister {
		runner.RegisterHandler(taskType, handler)
	}

	// Log what task types we're handling.
	taskTypes := runner.TaskTypes()
	fmt.Printf("Task runner configured to handle task types: %v\n", taskTypes)

	// Run the task runner.
	fmt.Printf("Task runner started. Press Ctrl+C to stop.\n")
	return runner.Run(ctx)
}
