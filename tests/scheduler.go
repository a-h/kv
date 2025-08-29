package tests

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/a-h/kv"
	"github.com/google/uuid"
)

// uniqueTaskName generates a unique task name to avoid conflicts between tests
func uniqueTaskName(baseName string) string {
	return fmt.Sprintf("%s-%s", baseName, uuid.New().String())
}

func newSchedulerTest(ctx context.Context, scheduler kv.Scheduler) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("New", newSchedulerNewTest(ctx, scheduler))
		t.Run("Get", newSchedulerGetTest(ctx, scheduler))
		t.Run("List", newSchedulerListTest(ctx, scheduler))
		t.Run("Lock", newSchedulerLockTest(ctx, scheduler))
		t.Run("Release", newSchedulerReleaseTest(ctx, scheduler))
		t.Run("Cancel", newSchedulerCancelTest(ctx, scheduler))
		t.Run("TaskFlow", newSchedulerTaskFlowTest(ctx, scheduler))
	}
}

func newSchedulerNewTest(ctx context.Context, scheduler kv.Scheduler) func(t *testing.T) {
	return func(t *testing.T) {
		now := time.Now().UTC()
		task := kv.NewTask("test-task", []byte(`{"message": "hello"}`))
		task.ScheduledFor = now.Add(5 * time.Minute)
		task.MaxRetries = 5
		task.TimeoutSeconds = 600

		err := scheduler.New(ctx, task)
		if err != nil {
			t.Fatalf("unexpected error creating task: %v", err)
		}

		// Verify the task was created.
		retrievedTask, ok, err := scheduler.Get(ctx, task.ID)
		if err != nil {
			t.Fatalf("unexpected error getting task: %v", err)
		}
		if !ok {
			t.Fatalf("task was not found after creation")
		}

		if retrievedTask.ID != task.ID {
			t.Errorf("expected task ID %s, got %s", task.ID, retrievedTask.ID)
		}
		if retrievedTask.Name != task.Name {
			t.Errorf("expected task name %s, got %s", task.Name, retrievedTask.Name)
		}
		expectedPayload := task.Payload
		actualPayload := retrievedTask.Payload
		if !bytes.Equal(actualPayload, expectedPayload) {
			t.Errorf("expected payload %s, got %s", string(expectedPayload), string(actualPayload))
		}
		if retrievedTask.Status != kv.TaskStatusPending {
			t.Errorf("expected status pending, got %s", retrievedTask.Status)
		}
		if retrievedTask.MaxRetries != 5 {
			t.Errorf("expected max retries 5, got %d", retrievedTask.MaxRetries)
		}
		if retrievedTask.TimeoutSeconds != 600 {
			t.Errorf("expected timeout 600, got %d", retrievedTask.TimeoutSeconds)
		}
	}
}

func newSchedulerGetTest(ctx context.Context, scheduler kv.Scheduler) func(t *testing.T) {
	return func(t *testing.T) {
		// Test getting a non-existent task.
		_, ok, err := scheduler.Get(ctx, "non-existent-id")
		if err != nil {
			t.Fatalf("unexpected error getting non-existent task: %v", err)
		}
		if ok {
			t.Fatalf("expected task not found, but task was returned")
		}

		// Create a task.
		task := kv.NewTask("get-test", []byte(`{"data": "test"}`))
		err = scheduler.New(ctx, task)
		if err != nil {
			t.Fatalf("unexpected error creating task: %v", err)
		}

		// Get the task.
		retrievedTask, ok, err := scheduler.Get(ctx, task.ID)
		if err != nil {
			t.Fatalf("unexpected error getting task: %v", err)
		}
		if !ok {
			t.Fatalf("task was not found")
		}

		if retrievedTask.ID != task.ID {
			t.Errorf("expected task ID %s, got %s", task.ID, retrievedTask.ID)
		}
		if retrievedTask.Name != task.Name {
			t.Errorf("expected task name %s, got %s", task.Name, retrievedTask.Name)
		}
	}
}

func newSchedulerListTest(ctx context.Context, scheduler kv.Scheduler) func(t *testing.T) {
	return func(t *testing.T) {
		// Create multiple tasks with different statuses and names.
		tasks := []kv.Task{
			kv.NewTask("email-task", []byte(`{"to": "user1@example.com"}`)),
			kv.NewTask("email-task", []byte(`{"to": "user2@example.com"}`)),
			kv.NewTask("cleanup-task", []byte(`{"dir": "/tmp"}`)),
		}

		for _, task := range tasks {
			err := scheduler.New(ctx, task)
			if err != nil {
				t.Fatalf("unexpected error creating task: %v", err)
			}
		}

		// Test listing all tasks.
		allTasks, err := scheduler.List(ctx, "", "", 0, 10)
		if err != nil {
			t.Fatalf("unexpected error listing all tasks: %v", err)
		}
		if len(allTasks) < 3 {
			t.Errorf("expected at least 3 tasks, got %d", len(allTasks))
		}

		// Test filtering by status.
		pendingTasks, err := scheduler.List(ctx, kv.TaskStatusPending, "", 0, 10)
		if err != nil {
			t.Fatalf("unexpected error listing pending tasks: %v", err)
		}
		if len(pendingTasks) < 3 {
			t.Errorf("expected at least 3 pending tasks, got %d", len(pendingTasks))
		}

		// Test filtering by name.
		emailTasks, err := scheduler.List(ctx, "", "email-task", 0, 10)
		if err != nil {
			t.Fatalf("unexpected error listing email tasks: %v", err)
		}
		if len(emailTasks) < 2 {
			t.Errorf("expected at least 2 email tasks, got %d", len(emailTasks))
		}

		// Test filtering by both status and name.
		pendingEmailTasks, err := scheduler.List(ctx, kv.TaskStatusPending, "email-task", 0, 10)
		if err != nil {
			t.Fatalf("unexpected error listing pending email tasks: %v", err)
		}
		if len(pendingEmailTasks) < 2 {
			t.Errorf("expected at least 2 pending email tasks, got %d", len(pendingEmailTasks))
		}

		// Test pagination.
		firstPage, err := scheduler.List(ctx, "", "", 0, 1)
		if err != nil {
			t.Fatalf("unexpected error getting first page: %v", err)
		}
		if len(firstPage) != 1 {
			t.Errorf("expected 1 task on first page, got %d", len(firstPage))
		}

		secondPage, err := scheduler.List(ctx, "", "", 1, 1)
		if err != nil {
			t.Fatalf("unexpected error getting second page: %v", err)
		}
		if len(secondPage) != 1 {
			t.Errorf("expected 1 task on second page, got %d", len(secondPage))
		}

		// Verify different tasks on different pages.
		if firstPage[0].ID == secondPage[0].ID {
			t.Errorf("expected different tasks on different pages")
		}
	}
}

func newSchedulerLockTest(ctx context.Context, scheduler kv.Scheduler) func(t *testing.T) {
	return func(t *testing.T) {
		// Create a single task scheduled for now to test lock contention.
		now := time.Now().UTC()
		task := kv.NewTask("lock-test-unique", []byte(`{"test": true}`))
		task.ScheduledFor = now.Add(-1 * time.Minute) // Schedule in the past so it's ready.

		err := scheduler.New(ctx, task)
		if err != nil {
			t.Fatalf("unexpected error creating task: %v", err)
		}

		// Test locking the task.
		runnerID := "test-runner-1"
		lockDuration := 5 * time.Minute
		lockedTask, locked, err := scheduler.Lock(ctx, runnerID, lockDuration, "lock-test-unique")
		if err != nil {
			t.Fatalf("unexpected error locking task: %v", err)
		}
		if !locked {
			t.Fatalf("expected task to be locked, but it wasn't")
		}

		if lockedTask.ID != task.ID {
			t.Errorf("expected locked task ID %s, got %s", task.ID, lockedTask.ID)
		}
		if lockedTask.Status != kv.TaskStatusRunning {
			t.Errorf("expected task status running, got %s", lockedTask.Status)
		}
		if lockedTask.LockedBy != runnerID {
			t.Errorf("expected task locked by %s, got %s", runnerID, lockedTask.LockedBy)
		}

		// Verify times are set correctly.
		if lockedTask.LockedAt == nil {
			t.Errorf("expected LockedAt to be set")
		} else if lockedTask.LockedAt.Before(now) {
			t.Errorf("expected LockedAt to be after task creation")
		}

		if lockedTask.LockExpiresAt == nil {
			t.Errorf("expected LockExpiresAt to be set")
		} else if lockedTask.LockExpiresAt.Before(*lockedTask.LockedAt) {
			t.Errorf("expected LockExpiresAt to be after LockedAt")
		}

		// Try to lock the same task type again with a different runner - should not succeed since there's only one task of this type.
		_, locked2, err := scheduler.Lock(ctx, "test-runner-2", lockDuration, "lock-test-unique")
		if err != nil {
			t.Fatalf("unexpected error on second lock attempt: %v", err)
		}
		if locked2 {
			t.Errorf("expected second lock attempt to fail since the only task of this type is already locked")
		}

		// Test locking with task type filtering.
		emailTask := kv.NewTask("email", []byte(`{"to": "test@example.com"}`))
		emailTask.ScheduledFor = now.Add(-1 * time.Minute)
		err = scheduler.New(ctx, emailTask)
		if err != nil {
			t.Fatalf("unexpected error creating email task: %v", err)
		}

		// Lock with specific task type.
		lockedEmail, locked, err := scheduler.Lock(ctx, "email-runner", lockDuration, "email")
		if err != nil {
			t.Fatalf("unexpected error locking email task: %v", err)
		}
		if !locked {
			t.Fatalf("expected email task to be locked")
		}
		if lockedEmail.Name != "email" {
			t.Errorf("expected locked task name 'email', got %s", lockedEmail.Name)
		}

		// Try to lock non-existent task type.
		_, locked, err = scheduler.Lock(ctx, "specific-runner", lockDuration, "non-existent-type")
		if err != nil {
			t.Fatalf("unexpected error locking non-existent task type: %v", err)
		}
		if locked {
			t.Errorf("expected no task to be locked for non-existent type")
		}
	}
}

func newSchedulerReleaseTest(ctx context.Context, scheduler kv.Scheduler) func(t *testing.T) {
	return func(t *testing.T) {
		now := time.Now().UTC()
		runnerID := "test-runner"
		lockDuration := 5 * time.Minute

		// Test successful task completion.
		successTaskName := uniqueTaskName("release-test-success")
		task1 := kv.NewTask(successTaskName, []byte(`{"test": true}`))
		task1.ScheduledFor = now.Add(-1 * time.Minute)
		err := scheduler.New(ctx, task1)
		if err != nil {
			t.Fatalf("unexpected error creating task: %v", err)
		}

		lockedTask, locked, err := scheduler.Lock(ctx, runnerID, lockDuration, successTaskName)
		if err != nil {
			t.Fatalf("unexpected error locking task: %v", err)
		}
		if !locked {
			t.Fatalf("expected task to be locked")
		}

		// Release with success.
		err = scheduler.Release(ctx, lockedTask.ID, runnerID, kv.TaskStatusCompleted, "")
		if err != nil {
			t.Fatalf("unexpected error releasing task: %v", err)
		}

		// Verify task status.
		completedTask, ok, err := scheduler.Get(ctx, lockedTask.ID)
		if err != nil {
			t.Fatalf("unexpected error getting completed task: %v", err)
		}
		if !ok {
			t.Fatalf("completed task not found")
		}
		if completedTask.Status != kv.TaskStatusCompleted {
			t.Errorf("expected task status completed, got %s", completedTask.Status)
		}
		if completedTask.CompletedAt == nil {
			t.Errorf("expected CompletedAt to be set")
		}

		// Test failed task - verify retry logic updates retry count and status.
		failedTaskName := uniqueTaskName("release-test-failed")
		task2 := kv.NewTask(failedTaskName, []byte(`{"test": true}`))
		task2.ScheduledFor = now.Add(-1 * time.Minute)
		task2.MaxRetries = 3 // Allow several retries
		err = scheduler.New(ctx, task2)
		if err != nil {
			t.Fatalf("unexpected error creating task: %v", err)
		}

		// Lock and fail the task.
		lockedTask2, locked, err := scheduler.Lock(ctx, runnerID, lockDuration, failedTaskName)
		if err != nil {
			t.Fatalf("unexpected error locking task: %v", err)
		}
		if !locked {
			t.Fatalf("expected task to be locked")
		}

		errorMessage := "task failed due to network error"
		err = scheduler.Release(ctx, lockedTask2.ID, runnerID, kv.TaskStatusFailed, errorMessage)
		if err != nil {
			t.Fatalf("unexpected error releasing failed task: %v", err)
		}

		// Verify task is back to pending for retry (it may be scheduled for the future due to backoff).
		retriedTask, ok, err := scheduler.Get(ctx, lockedTask2.ID)
		if err != nil {
			t.Fatalf("unexpected error getting retried task: %v", err)
		}
		if !ok {
			t.Fatalf("retried task not found")
		}
		if retriedTask.Status != kv.TaskStatusPending {
			t.Errorf("expected task status pending (for retry), got %s", retriedTask.Status)
		}
		if retriedTask.RetryCount != 1 {
			t.Errorf("expected retry count 1, got %d", retriedTask.RetryCount)
		}
		if retriedTask.LastError != errorMessage {
			t.Errorf("expected last error %s, got %s", errorMessage, retriedTask.LastError)
		}

		// Test task that exceeds max retries - create a new task with very low max retries.
		maxRetriesTaskName := uniqueTaskName("release-test-max-retries")
		task3 := kv.NewTask(maxRetriesTaskName, []byte(`{"test": true}`))
		task3.ScheduledFor = now.Add(-1 * time.Minute)
		task3.MaxRetries = 0 // No retries allowed
		err = scheduler.New(ctx, task3)
		if err != nil {
			t.Fatalf("unexpected error creating task for max retries test: %v", err)
		}

		// Lock and fail the task.
		lockedTask3, locked, err := scheduler.Lock(ctx, runnerID, lockDuration, maxRetriesTaskName)
		if err != nil {
			t.Fatalf("unexpected error locking task for max retries test: %v", err)
		}
		if !locked {
			t.Fatalf("expected task to be locked for max retries test")
		}

		err = scheduler.Release(ctx, lockedTask3.ID, runnerID, kv.TaskStatusFailed, "failed permanently")
		if err != nil {
			t.Fatalf("unexpected error releasing failed task for max retries test: %v", err)
		}

		// This task should be permanently failed since max retries = 0.
		finalTask, ok, err := scheduler.Get(ctx, lockedTask3.ID)
		if err != nil {
			t.Fatalf("unexpected error getting final failed task: %v", err)
		}
		if !ok {
			t.Fatalf("final failed task not found")
		}
		if finalTask.Status != kv.TaskStatusFailed {
			t.Errorf("expected task status failed (permanently), got %s", finalTask.Status)
		}
		if finalTask.RetryCount != 1 {
			t.Errorf("expected retry count 1 (indicating one failed attempt), got %d", finalTask.RetryCount)
		}
	}
}

func newSchedulerCancelTest(ctx context.Context, scheduler kv.Scheduler) func(t *testing.T) {
	return func(t *testing.T) {
		// Create a pending task.
		task := kv.NewTask("cancel-test", []byte(`{"test": true}`))
		err := scheduler.New(ctx, task)
		if err != nil {
			t.Fatalf("unexpected error creating task: %v", err)
		}

		// Cancel the task.
		err = scheduler.Cancel(ctx, task.ID)
		if err != nil {
			t.Fatalf("unexpected error cancelling task: %v", err)
		}

		// Verify task is cancelled.
		cancelledTask, ok, err := scheduler.Get(ctx, task.ID)
		if err != nil {
			t.Fatalf("unexpected error getting cancelled task: %v", err)
		}
		if !ok {
			t.Fatalf("cancelled task not found")
		}
		if cancelledTask.Status != kv.TaskStatusCancelled {
			t.Errorf("expected task status cancelled, got %s", cancelledTask.Status)
		}

		// Test cancelling a non-existent task.
		err = scheduler.Cancel(ctx, "non-existent-id")
		if err != nil {
			t.Fatalf("unexpected error cancelling non-existent task: %v", err)
		}

		// Create and lock a running task.
		runningTask := kv.NewTask("running-cancel-test", []byte(`{"test": true}`))
		runningTask.ScheduledFor = time.Now().UTC().Add(-1 * time.Minute)
		err = scheduler.New(ctx, runningTask)
		if err != nil {
			t.Fatalf("unexpected error creating running task: %v", err)
		}

		// Lock the task.
		lockedTask, locked, err := scheduler.Lock(ctx, "test-runner", 5*time.Minute, "running-cancel-test")
		if err != nil {
			t.Fatalf("unexpected error locking task: %v", err)
		}
		if !locked {
			t.Fatalf("expected task to be locked")
		}

		// Cancel the running task.
		err = scheduler.Cancel(ctx, lockedTask.ID)
		if err != nil {
			t.Fatalf("unexpected error cancelling running task: %v", err)
		}

		// Verify the running task is cancelled.
		cancelledRunningTask, ok, err := scheduler.Get(ctx, lockedTask.ID)
		if err != nil {
			t.Fatalf("unexpected error getting cancelled running task: %v", err)
		}
		if !ok {
			t.Fatalf("cancelled running task not found")
		}
		if cancelledRunningTask.Status != kv.TaskStatusCancelled {
			t.Errorf("expected running task status cancelled, got %s", cancelledRunningTask.Status)
		}
	}
}

func newSchedulerTaskFlowTest(ctx context.Context, scheduler kv.Scheduler) func(t *testing.T) {
	return func(t *testing.T) {
		// Test complete task lifecycle: create -> lock -> complete.
		now := time.Now().UTC()
		task := kv.NewTask("lifecycle-test", []byte(`{"message": "end-to-end test"}`))
		task.ScheduledFor = now.Add(-1 * time.Minute) // Ready to run.
		task.MaxRetries = 1
		task.TimeoutSeconds = 120

		// Create the task.
		err := scheduler.New(ctx, task)
		if err != nil {
			t.Fatalf("unexpected error creating task: %v", err)
		}

		// Verify it appears in pending list.
		pendingTasks, err := scheduler.List(ctx, kv.TaskStatusPending, "", 0, 10)
		if err != nil {
			t.Fatalf("unexpected error listing pending tasks: %v", err)
		}

		var foundTask bool
		for _, pt := range pendingTasks {
			if pt.ID == task.ID {
				foundTask = true
				break
			}
		}
		if !foundTask {
			t.Errorf("created task not found in pending list")
		}

		// Lock the task.
		runnerID := "lifecycle-runner"
		lockedTask, locked, err := scheduler.Lock(ctx, runnerID, 5*time.Minute, "lifecycle-test")
		if err != nil {
			t.Fatalf("unexpected error locking task: %v", err)
		}
		if !locked {
			t.Fatalf("expected task to be locked")
		}
		if lockedTask.ID != task.ID {
			t.Errorf("locked wrong task, expected %s, got %s", task.ID, lockedTask.ID)
		}

		// Verify it appears in running list.
		runningTasks, err := scheduler.List(ctx, kv.TaskStatusRunning, "", 0, 10)
		if err != nil {
			t.Fatalf("unexpected error listing running tasks: %v", err)
		}

		foundTask = false
		for _, rt := range runningTasks {
			if rt.ID == task.ID {
				foundTask = true
				break
			}
		}
		if !foundTask {
			t.Errorf("locked task not found in running list")
		}

		// Complete the task.
		err = scheduler.Release(ctx, lockedTask.ID, runnerID, kv.TaskStatusCompleted, "")
		if err != nil {
			t.Fatalf("unexpected error completing task: %v", err)
		}

		// Verify it appears in completed list.
		completedTasks, err := scheduler.List(ctx, kv.TaskStatusCompleted, "", 0, 10)
		if err != nil {
			t.Fatalf("unexpected error listing completed tasks: %v", err)
		}

		foundTask = false
		for _, ct := range completedTasks {
			if ct.ID == task.ID {
				foundTask = true
				if ct.Status != kv.TaskStatusCompleted {
					t.Errorf("expected completed task status, got %s", ct.Status)
				}
				break
			}
		}
		if !foundTask {
			t.Errorf("completed task not found in completed list")
		}
	}
}
