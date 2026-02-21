package kv

import (
	"context"
	"log/slog"
	"time"
)

// CleanupResult contains metrics from a single cleanup run.
type CleanupResult struct {
	FilesDeleted int
	Duration     time.Duration
}

// CleanupFunc is the signature for caller-supplied cleanup implementations.
// It receives the store for reading counter stats and deleting cached items.
type CleanupFunc func(ctx context.Context, store Store) (CleanupResult, error)

// maxCleanupDuration is the elapsed time after which a warning is logged.
const maxCleanupDuration = 12 * time.Hour

// NewCleanupTaskHandler returns a TaskHandler that runs fn and logs metrics on completion.
// A warning is logged if the cleanup takes longer than 12 hours.
func NewCleanupTaskHandler(fn CleanupFunc, store Store) TaskHandler {
	return func(ctx context.Context, task Task) error {
		start := time.Now()

		warningTimer := time.NewTimer(maxCleanupDuration)
		warningDone := make(chan struct{})
		go func() {
			select {
			case <-warningTimer.C:
				slog.WarnContext(ctx, "cleanup is taking longer than expected",
					slog.String("task", task.Name),
					slog.String("elapsed", time.Since(start).String()),
					slog.String("threshold", maxCleanupDuration.String()),
				)
			case <-warningDone:
			}
		}()
		defer func() {
			close(warningDone)
			warningTimer.Stop()
		}()

		result, err := fn(ctx, store)
		result.Duration = time.Since(start)

		slog.InfoContext(ctx, "cleanup completed",
			slog.String("task", task.Name),
			slog.Int("filesDeleted", result.FilesDeleted),
			slog.Duration("duration", result.Duration),
		)

		return err
	}
}

// ScheduleDailyCleanup creates a task scheduled to run at the next daily occurrence of timeOfDay.
// timeOfDay is measured from midnight; for example, 2*time.Hour schedules at 02:00 local time.
func ScheduleDailyCleanup(ctx context.Context, scheduler Scheduler, taskName string, timeOfDay time.Duration) error {
	now := time.Now()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	nextRun := today.Add(timeOfDay)
	if !nextRun.After(now) {
		nextRun = nextRun.Add(24 * time.Hour)
	}
	task := NewScheduledTask(taskName, nil, nextRun)
	return scheduler.New(ctx, task)
}
