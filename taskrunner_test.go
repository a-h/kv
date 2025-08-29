package kv_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/a-h/kv"
	"github.com/a-h/kv/sqlitekv"
	"zombiezen.com/go/sqlite/sqlitex"
)

type MockScheduler struct {
	mu           sync.RWMutex
	newFunc      func(ctx context.Context, task kv.Task) error
	getFunc      func(ctx context.Context, id string) (kv.Task, bool, error)
	listFunc     func(ctx context.Context, status kv.TaskStatus, name string, offset, limit int) ([]kv.Task, error)
	lockFunc     func(ctx context.Context, runnerID string, lockDuration time.Duration, taskTypes ...string) (kv.Task, bool, error)
	releaseFunc  func(ctx context.Context, id string, runnerID string, status kv.TaskStatus, errorMessage string) error
	cancelFunc   func(ctx context.Context, id string) error
	lockCalls    []lockCall
	releaseCalls []releaseCall
}

type lockCall struct {
	runnerID     string
	lockDuration time.Duration
	taskTypes    []string
}

type releaseCall struct {
	id           string
	runnerID     string
	status       kv.TaskStatus
	errorMessage string
}

func (m *MockScheduler) New(ctx context.Context, task kv.Task) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.newFunc != nil {
		return m.newFunc(ctx, task)
	}
	return nil
}

func (m *MockScheduler) Get(ctx context.Context, id string) (kv.Task, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.getFunc != nil {
		return m.getFunc(ctx, id)
	}
	return kv.Task{}, false, nil
}

func (m *MockScheduler) List(ctx context.Context, status kv.TaskStatus, name string, offset, limit int) ([]kv.Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.listFunc != nil {
		return m.listFunc(ctx, status, name, offset, limit)
	}
	return nil, nil
}

func (m *MockScheduler) Lock(ctx context.Context, runnerID string, lockDuration time.Duration, taskTypes ...string) (kv.Task, bool, error) {
	m.mu.Lock()
	m.lockCalls = append(m.lockCalls, lockCall{
		runnerID:     runnerID,
		lockDuration: lockDuration,
		taskTypes:    append([]string(nil), taskTypes...),
	})
	m.mu.Unlock()

	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.lockFunc != nil {
		return m.lockFunc(ctx, runnerID, lockDuration, taskTypes...)
	}
	return kv.Task{}, false, nil
}

func (m *MockScheduler) Release(ctx context.Context, id string, runnerID string, status kv.TaskStatus, errorMessage string) error {
	m.mu.Lock()
	m.releaseCalls = append(m.releaseCalls, releaseCall{
		id:           id,
		runnerID:     runnerID,
		status:       status,
		errorMessage: errorMessage,
	})
	m.mu.Unlock()

	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.releaseFunc != nil {
		return m.releaseFunc(ctx, id, runnerID, status, errorMessage)
	}
	return nil
}

func (m *MockScheduler) Cancel(ctx context.Context, id string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.cancelFunc != nil {
		return m.cancelFunc(ctx, id)
	}
	return nil
}

func (m *MockScheduler) getLockCalls() []lockCall {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]lockCall(nil), m.lockCalls...)
}

func (m *MockScheduler) getReleaseCalls() []releaseCall {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]releaseCall(nil), m.releaseCalls...)
}

func TestTaskRunner(t *testing.T) {
	t.Run("creates a task runner with sensible defaults", func(t *testing.T) {
		scheduler := &MockScheduler{}
		runnerID := "test-runner"

		tr := kv.NewTaskRunner(scheduler, runnerID)

		if tr.Scheduler != scheduler {
			t.Errorf("expected scheduler to be set")
		}
		if tr.RunnerID != runnerID {
			t.Errorf("expected runnerID to be %s, got %s", runnerID, tr.RunnerID)
		}
		if tr.Handlers == nil {
			t.Errorf("expected handlers map to be initialized")
		}
		if tr.PollInterval != 10*time.Second {
			t.Errorf("expected default poll interval of 10s, got %v", tr.PollInterval)
		}
		if tr.LockDuration != 5*time.Minute {
			t.Errorf("expected default lock duration of 5m, got %v", tr.LockDuration)
		}
		if tr.ShutdownTimeout != 30*time.Second {
			t.Errorf("expected default shutdown timeout of 30s, got %v", tr.ShutdownTimeout)
		}
	})
	t.Run("registering a handler makes it available for task execution", func(t *testing.T) {
		scheduler := &MockScheduler{}
		tr := kv.NewTaskRunner(scheduler, "test-runner")

		handlerCalled := false
		handler := func(ctx context.Context, task kv.Task) error {
			handlerCalled = true
			return nil
		}

		tr.RegisterHandler("test-task", handler)

		if len(tr.Handlers) != 1 {
			t.Errorf("expected 1 handler, got %d", len(tr.Handlers))
		}

		if _, exists := tr.Handlers["test-task"]; !exists {
			t.Errorf("expected handler for 'test-task' to be registered")
		}

		err := tr.Handlers["test-task"](context.Background(), kv.Task{})
		if err != nil {
			t.Errorf("unexpected error from handler: %v", err)
		}
		if !handlerCalled {
			t.Errorf("expected handler to be called")
		}
	})
	t.Run("returns an empty slice when no handlers are registered", func(t *testing.T) {
		scheduler := &MockScheduler{}
		tr := kv.NewTaskRunner(scheduler, "test-runner")

		types := tr.TaskTypes()
		if len(types) != 0 {
			t.Errorf("expected no task types, got %v", types)
		}
	})
	t.Run("returns all registered handler names", func(t *testing.T) {
		scheduler := &MockScheduler{}
		tr := kv.NewTaskRunner(scheduler, "test-runner")

		tr.RegisterHandler("task1", func(ctx context.Context, task kv.Task) error { return nil })
		tr.RegisterHandler("task2", func(ctx context.Context, task kv.Task) error { return nil })

		types := tr.TaskTypes()
		if len(types) != 2 {
			t.Errorf("expected 2 task types, got %d", len(types))
		}

		typeMap := make(map[string]bool)
		for _, taskType := range types {
			typeMap[taskType] = true
		}
		if !typeMap["task1"] || !typeMap["task2"] {
			t.Errorf("expected task types to include 'task1' and 'task2', got %v", types)
		}
	})
	t.Run("Run", func(t *testing.T) {
		t.Run("returns context error when context is cancelled", func(t *testing.T) {
			scheduler := &MockScheduler{}
			tr := kv.NewTaskRunner(scheduler, "test-runner")
			tr.PollInterval = 10 * time.Millisecond

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			err := tr.Run(ctx)
			if !errors.Is(err, context.Canceled) {
				t.Errorf("expected context.Canceled error, got %v", err)
			}
		})
		t.Run("does not poll when no handlers are registered", func(t *testing.T) {
			scheduler := &MockScheduler{}
			tr := kv.NewTaskRunner(scheduler, "test-runner")
			tr.PollInterval = 10 * time.Millisecond

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			err := tr.Run(ctx)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("expected context.DeadlineExceeded error, got %v", err)
			}

			lockCalls := scheduler.getLockCalls()
			if len(lockCalls) != 0 {
				t.Errorf("expected no lock calls when no handlers registered, got %d", len(lockCalls))
			}
		})
		t.Run("continues polling when no tasks are available", func(t *testing.T) {
			scheduler := &MockScheduler{}
			scheduler.lockFunc = func(ctx context.Context, runnerID string, lockDuration time.Duration, taskTypes ...string) (kv.Task, bool, error) {
				return kv.Task{}, false, nil
			}

			tr := kv.NewTaskRunner(scheduler, "test-runner")
			tr.RegisterHandler("test-task", func(ctx context.Context, task kv.Task) error { return nil })
			tr.PollInterval = 10 * time.Millisecond

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			err := tr.Run(ctx)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("expected context.DeadlineExceeded error, got %v", err)
			}

			lockCalls := scheduler.getLockCalls()
			if len(lockCalls) == 0 {
				t.Errorf("expected at least one lock call")
			}
		})
		t.Run("continues polling when lock operation fails", func(t *testing.T) {
			scheduler := &MockScheduler{}
			scheduler.lockFunc = func(ctx context.Context, runnerID string, lockDuration time.Duration, taskTypes ...string) (kv.Task, bool, error) {
				return kv.Task{}, false, errors.New("lock failed")
			}

			tr := kv.NewTaskRunner(scheduler, "test-runner")
			tr.RegisterHandler("test-task", func(ctx context.Context, task kv.Task) error { return nil })
			tr.PollInterval = 10 * time.Millisecond

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			err := tr.Run(ctx)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("expected context.DeadlineExceeded error, got %v", err)
			}

			lockCalls := scheduler.getLockCalls()
			if len(lockCalls) == 0 {
				t.Errorf("expected at least one lock call")
			}
		})
		t.Run("executes task handler and releases with completed status on success", func(t *testing.T) {
			var handlerCalled bool
			var handlerTask kv.Task

			task := kv.NewTask("test-task", []byte(`{"message": "hello"}`))
			task.TimeoutSeconds = 1

			scheduler := &MockScheduler{}
			scheduler.lockFunc = func(ctx context.Context, runnerID string, lockDuration time.Duration, taskTypes ...string) (kv.Task, bool, error) {
				return task, true, nil
			}

			tr := kv.NewTaskRunner(scheduler, "test-runner")
			tr.RegisterHandler("test-task", func(ctx context.Context, t kv.Task) error {
				handlerCalled = true
				handlerTask = t
				return nil
			})
			tr.PollInterval = 10 * time.Millisecond

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			err := tr.Run(ctx)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("expected context.DeadlineExceeded error, got %v", err)
			}

			if !handlerCalled {
				t.Errorf("expected handler to be called")
			}

			if handlerTask.ID != task.ID {
				t.Errorf("expected handler to receive task with ID %s, got %s", task.ID, handlerTask.ID)
			}

			releaseCalls := scheduler.getReleaseCalls()
			if len(releaseCalls) == 0 {
				t.Errorf("expected at least one release call")
			}

			releaseCall := releaseCalls[0]
			if releaseCall.id != task.ID {
				t.Errorf("expected release call for task %s, got %s", task.ID, releaseCall.id)
			}
			if releaseCall.status != kv.TaskStatusCompleted {
				t.Errorf("expected release call with status %s, got %s", kv.TaskStatusCompleted, releaseCall.status)
			}
			if releaseCall.errorMessage != "" {
				t.Errorf("expected empty error message, got %s", releaseCall.errorMessage)
			}
		})
		t.Run("releases task with failed status when handler returns an error", func(t *testing.T) {
			var handlerCalled bool
			handlerError := errors.New("handler failed")

			task := kv.NewTask("test-task", []byte(`{"message": "hello"}`))
			task.TimeoutSeconds = 1

			scheduler := &MockScheduler{}
			scheduler.lockFunc = func(ctx context.Context, runnerID string, lockDuration time.Duration, taskTypes ...string) (kv.Task, bool, error) {
				return task, true, nil
			}

			tr := kv.NewTaskRunner(scheduler, "test-runner")
			tr.RegisterHandler("test-task", func(ctx context.Context, t kv.Task) error {
				handlerCalled = true
				return handlerError
			})
			tr.PollInterval = 10 * time.Millisecond

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			err := tr.Run(ctx)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("expected context.DeadlineExceeded error, got %v", err)
			}

			if !handlerCalled {
				t.Errorf("expected handler to be called")
			}

			releaseCalls := scheduler.getReleaseCalls()
			if len(releaseCalls) == 0 {
				t.Errorf("expected at least one release call")
			}

			releaseCall := releaseCalls[0]
			if releaseCall.id != task.ID {
				t.Errorf("expected release call for task %s, got %s", task.ID, releaseCall.id)
			}
			if releaseCall.status != kv.TaskStatusFailed {
				t.Errorf("expected release call with status %s, got %s", kv.TaskStatusFailed, releaseCall.status)
			}
			if releaseCall.errorMessage != handlerError.Error() {
				t.Errorf("expected error message %s, got %s", handlerError.Error(), releaseCall.errorMessage)
			}
		})

		t.Run("releases task with failed status when no handler exists for the task type", func(t *testing.T) {
			task := kv.NewTask("unknown-task", []byte(`{"message": "hello"}`))
			task.TimeoutSeconds = 1

			scheduler := &MockScheduler{}
			scheduler.lockFunc = func(ctx context.Context, runnerID string, lockDuration time.Duration, taskTypes ...string) (kv.Task, bool, error) {
				return task, true, nil
			}

			tr := kv.NewTaskRunner(scheduler, "test-runner")
			tr.RegisterHandler("test-task", func(ctx context.Context, t kv.Task) error { return nil })
			tr.PollInterval = 10 * time.Millisecond

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			err := tr.Run(ctx)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("expected context.DeadlineExceeded error, got %v", err)
			}

			releaseCalls := scheduler.getReleaseCalls()
			if len(releaseCalls) == 0 {
				t.Errorf("expected at least one release call")
				return
			}

			releaseCall := releaseCalls[0]
			if releaseCall.id != task.ID {
				t.Errorf("expected release call for task %s, got %s", task.ID, releaseCall.id)
			}
			if releaseCall.status != kv.TaskStatusFailed {
				t.Errorf("expected release call with status %s, got %s", kv.TaskStatusFailed, releaseCall.status)
			}
			if releaseCall.errorMessage == "" {
				t.Errorf("expected error message about missing handler")
			}
		})

		t.Run("continues when the release operation fails", func(t *testing.T) {
			task := kv.NewTask("test-task", []byte(`{"message": "hello"}`))
			task.TimeoutSeconds = 1

			scheduler := &MockScheduler{}
			scheduler.lockFunc = func(ctx context.Context, runnerID string, lockDuration time.Duration, taskTypes ...string) (kv.Task, bool, error) {
				return task, true, nil
			}
			scheduler.releaseFunc = func(ctx context.Context, id string, runnerID string, status kv.TaskStatus, errorMessage string) error {
				return errors.New("release failed")
			}

			tr := kv.NewTaskRunner(scheduler, "test-runner")
			tr.RegisterHandler("test-task", func(ctx context.Context, t kv.Task) error { return nil })
			tr.PollInterval = 10 * time.Millisecond

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			err := tr.Run(ctx)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("expected context.DeadlineExceeded error, got %v", err)
			}

			releaseCalls := scheduler.getReleaseCalls()
			if len(releaseCalls) == 0 {
				t.Errorf("expected at least one release call")
			}
		})

		t.Run("cancels handler execution and releases with failed status when the task times out", func(t *testing.T) {
			task := kv.NewTask("test-task", []byte(`{"message": "hello"}`))
			task.TimeoutSeconds = 1

			scheduler := &MockScheduler{}
			callCount := 0
			scheduler.lockFunc = func(ctx context.Context, runnerID string, lockDuration time.Duration, taskTypes ...string) (kv.Task, bool, error) {
				callCount++
				if callCount == 1 {
					return task, true, nil
				}
				return kv.Task{}, false, nil
			}

			tr := kv.NewTaskRunner(scheduler, "test-runner")
			tr.RegisterHandler("test-task", func(ctx context.Context, t kv.Task) error {
				select {
				case <-time.After(2 * time.Second):
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			tr.PollInterval = 10 * time.Millisecond

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			err := tr.Run(ctx)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("expected context.DeadlineExceeded error, got %v", err)
			}

			releaseCalls := scheduler.getReleaseCalls()
			if len(releaseCalls) == 0 {
				t.Errorf("expected at least one release call")
			}

			releaseCall := releaseCalls[0]
			if releaseCall.status != kv.TaskStatusFailed {
				t.Errorf("expected release call with status %s, got %s", kv.TaskStatusFailed, releaseCall.status)
			}
			if releaseCall.errorMessage == "" {
				t.Errorf("expected error message about timeout")
			}
		})
		t.Run("passes all registered task types to the scheduler when locking tasks", func(t *testing.T) {
			task1 := kv.NewTask("task-type-1", []byte(`{"id": 1}`))
			task2 := kv.NewTask("task-type-2", []byte(`{"id": 2}`))

			scheduler := &MockScheduler{}
			callCount := 0
			scheduler.lockFunc = func(ctx context.Context, runnerID string, lockDuration time.Duration, taskTypes ...string) (kv.Task, bool, error) {
				callCount++
				switch callCount {
				case 1:
					return task1, true, nil
				case 2:
					return task2, true, nil
				default:
					return kv.Task{}, false, nil
				}
			}

			tr := kv.NewTaskRunner(scheduler, "test-runner")
			tr.PollInterval = 10 * time.Millisecond

			var handler1Called, handler2Called bool
			tr.RegisterHandler("task-type-1", func(ctx context.Context, t kv.Task) error {
				handler1Called = true
				return nil
			})
			tr.RegisterHandler("task-type-2", func(ctx context.Context, t kv.Task) error {
				handler2Called = true
				return nil
			})

			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()

			err := tr.Run(ctx)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("expected context.DeadlineExceeded error, got %v", err)
			}

			if !handler1Called {
				t.Errorf("expected handler1 to be called")
			}
			if !handler2Called {
				t.Errorf("expected handler2 to be called")
			}

			lockCalls := scheduler.getLockCalls()
			if len(lockCalls) == 0 {
				t.Errorf("expected at least one lock call")
			}

			taskTypesMap := make(map[string]bool)
			for _, call := range lockCalls {
				for _, taskType := range call.taskTypes {
					taskTypesMap[taskType] = true
				}
			}

			if !taskTypesMap["task-type-1"] || !taskTypesMap["task-type-2"] {
				t.Errorf("expected lock calls to include both task types")
			}
		})
	})

	t.Run("integrates correctly with the SQLite scheduler", func(t *testing.T) {
		ctx := context.Background()

		pool, err := sqlitex.NewPool("file::memory:?mode=memory&cache=shared", sqlitex.PoolOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()

		scheduler := sqlitekv.NewScheduler(pool)
		if err := scheduler.Init(ctx); err != nil {
			t.Fatalf("failed to init scheduler: %v", err)
		}

		task := kv.NewTask("integration-test", []byte(`{"message": "hello world"}`))
		if err := scheduler.New(ctx, task); err != nil {
			t.Fatalf("failed to create task: %v", err)
		}

		tr := kv.NewTaskRunner(scheduler, "integration-runner")
		tr.PollInterval = 50 * time.Millisecond

		var handlerCalled bool
		var receivedTask kv.Task
		tr.RegisterHandler("integration-test", func(ctx context.Context, t kv.Task) error {
			handlerCalled = true
			receivedTask = t
			return nil
		})

		runCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()

		err = tr.Run(runCtx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected context.DeadlineExceeded error, got %v", err)
		}

		if !handlerCalled {
			t.Errorf("expected handler to be called")
		}

		if receivedTask.ID != task.ID {
			t.Errorf("expected handler to receive task with ID %s, got %s", task.ID, receivedTask.ID)
		}

		completedTask, ok, err := scheduler.Get(ctx, task.ID)
		if err != nil {
			t.Fatalf("failed to get task: %v", err)
		}
		if !ok {
			t.Fatalf("task not found")
		}
		if completedTask.Status != kv.TaskStatusCompleted {
			t.Errorf("expected task status to be %s, got %s", kv.TaskStatusCompleted, completedTask.Status)
		}
	})
}
