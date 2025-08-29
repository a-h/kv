package kv

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"
)

// TaskHandler is a function that processes a task.
// It receives the task and should return an error if the task failed.
type TaskHandler func(ctx context.Context, task Task) error

// TaskRunner manages the execution of scheduled tasks.
type TaskRunner struct {
	Scheduler       Scheduler
	RunnerID        string
	Handlers        map[string]TaskHandler
	PollInterval    time.Duration
	LockDuration    time.Duration
	ShutdownTimeout time.Duration
}

// NewTaskRunner creates a new task runner with the given configuration.
func NewTaskRunner(scheduler Scheduler, runnerID string) *TaskRunner {
	return &TaskRunner{
		Scheduler:       scheduler,
		RunnerID:        runnerID,
		Handlers:        make(map[string]TaskHandler),
		PollInterval:    10 * time.Second,
		LockDuration:    5 * time.Minute,
		ShutdownTimeout: 30 * time.Second,
	}
}

// RegisterHandler registers a task handler for a specific task name.
func (tr *TaskRunner) RegisterHandler(taskName string, handler TaskHandler) {
	tr.Handlers[taskName] = handler
}

// TaskTypes returns the task types this runner can handle based on registered handlers.
// If no handlers are registered, it returns an empty slice.
func (tr *TaskRunner) TaskTypes() []string {
	return slices.Collect(maps.Keys(tr.Handlers))
}

// Run starts the task runner and processes tasks until the context is cancelled.
func (tr *TaskRunner) Run(ctx context.Context) error {
	ticker := time.NewTicker(tr.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := tr.processTasks(ctx); err != nil {
				// Log error but continue running.
				// In a real implementation, you might want to use a logger here.
				continue
			}
		}
	}
}

// processTasks attempts to get and process the next pending task.
func (tr *TaskRunner) processTasks(ctx context.Context) error {
	taskTypes := tr.TaskTypes()
	if len(taskTypes) == 0 {
		return fmt.Errorf("task runner: no task handlers registered")
	}

	task, locked, err := tr.Scheduler.Lock(ctx, tr.RunnerID, tr.LockDuration, taskTypes...)
	if err != nil {
		return fmt.Errorf("task runner: failed to lock next pending task: %w", err)
	}
	if !locked {
		// No tasks available or couldn't acquire lock.
		return nil
	}

	// Execute the task with timeout.
	taskCtx, cancel := context.WithTimeout(ctx, time.Duration(task.TimeoutSeconds)*time.Second)
	defer cancel()

	if err := tr.executeTask(taskCtx, task); err != nil {
		return fmt.Errorf("task runner: failed to execute task %s: %w", task.ID, err)
	}

	return nil
}

// executeTask runs a single task and updates its status.
func (tr *TaskRunner) executeTask(ctx context.Context, task Task) error {
	// Get the handler for this task.
	handler, exists := tr.Handlers[task.Name]
	if !exists {
		// Release with failed status and error message.
		if releaseErr := tr.Scheduler.Release(ctx, task.ID, tr.RunnerID, TaskStatusFailed, fmt.Sprintf("no handler registered for task type: %s", task.Name)); releaseErr != nil {
			return fmt.Errorf("failed to release task after missing handler: %w", releaseErr)
		}
		return fmt.Errorf("no handler registered for task type: %s", task.Name)
	}

	// Execute the handler.
	if err := handler(ctx, task); err != nil {
		// Release with failed status and error message.
		if releaseErr := tr.Scheduler.Release(ctx, task.ID, tr.RunnerID, TaskStatusFailed, err.Error()); releaseErr != nil {
			return fmt.Errorf("failed to release task after handler error: %w", releaseErr)
		}
		return err
	}

	// Mark task as completed.
	if err := tr.Scheduler.Release(ctx, task.ID, tr.RunnerID, TaskStatusCompleted, ""); err != nil {
		return fmt.Errorf("failed to release task after completion: %w", err)
	}

	return nil
}
