package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/a-h/kv"
	"github.com/a-h/kv/sqlitekv"
	"zombiezen.com/go/sqlite/sqlitex"
)

// This example demonstrates how to create specialized task runners that only handle
// specific task types, allowing for worker pools with different capabilities.

func main() {
	// Set up in-memory SQLite store for demonstration.
	pool, err := sqlitex.NewPool("file::memory:?mode=memory&cache=shared", sqlitex.PoolOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	scheduler := sqlitekv.NewScheduler(pool)

	// Apply migrations.
	if err := scheduler.Init(context.Background()); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Create different types of tasks to simulate a real workload.
	createExampleTasks(ctx, scheduler)

	// Start different worker pools concurrently.
	go startGPUWorkerPool(ctx, scheduler)
	go startCPUWorkerPool(ctx, scheduler)
	go startGeneralWorkerPool(ctx, scheduler)

	fmt.Println("Started specialized worker pools:")
	fmt.Println("- GPU workers handle: gpu-compute, gpu-ml-training")
	fmt.Println("- CPU workers handle: cpu-intensive, data-processing")
	fmt.Println("- General workers handle: cleanup, email, log (any type)")
	fmt.Println("\nPress Ctrl+C to stop...")

	<-ctx.Done()
	fmt.Println("\nShutting down workers...")
}

func createExampleTasks(ctx context.Context, scheduler kv.Scheduler) {
	tasks := []struct {
		name    string
		payload map[string]any
	}{
		{"gpu-compute", map[string]any{"operation": "matrix_multiply", "size": 1000}},
		{"gpu-ml-training", map[string]any{"model": "neural_network", "epochs": 100}},
		{"cpu-intensive", map[string]any{"algorithm": "prime_calculation", "limit": 10000}},
		{"data-processing", map[string]any{"file": "/data/large_dataset.csv", "operation": "aggregate"}},
		{"cleanup", map[string]any{"directory": "/tmp", "older_than": "7d"}},
		{"email", map[string]any{"to": "user@example.com", "subject": "Task completed"}},
		{"log", map[string]any{"level": "info", "message": "System health check"}},
	}

	for _, taskInfo := range tasks {
		payload, _ := json.Marshal(taskInfo.payload)
		task := kv.NewTask(taskInfo.name, payload)
		if err := scheduler.New(ctx, task); err != nil {
			log.Printf("Failed to create task %s: %v", taskInfo.name, err)
		} else {
			fmt.Printf("Created task: %s\n", taskInfo.name)
		}
	}
}

func startGPUWorkerPool(ctx context.Context, scheduler kv.Scheduler) {
	for i := 0; i < 2; i++ {
		go func(workerID int) {
			runnerID := fmt.Sprintf("gpu-worker-%d", workerID)
			runner := kv.NewTaskRunner(scheduler, runnerID)

			runner.PollInterval = 2 * time.Second

			// Register GPU-specific handlers - by registering only these handlers,
			// this worker will only handle "gpu-compute" and "gpu-ml-training" tasks.
			runner.RegisterHandler("gpu-compute", func(ctx context.Context, task kv.Task) error {
				var payload map[string]any
				if err := json.Unmarshal(task.Payload, &payload); err != nil {
					return fmt.Errorf("failed to unmarshal task payload: %w", err)
				}
				fmt.Printf("[%s] Performing GPU compute: %+v\n", runnerID, payload)
				// Simulate work.
				time.Sleep(1 * time.Second)
				return nil
			})

			runner.RegisterHandler("gpu-ml-training", func(ctx context.Context, task kv.Task) error {
				var payload map[string]any
				if err := json.Unmarshal(task.Payload, &payload); err != nil {
					return fmt.Errorf("failed to unmarshal task payload: %w", err)
				}
				fmt.Printf("[%s] Training ML model on GPU: %+v\n", runnerID, payload)
				// Simulate work.
				time.Sleep(2 * time.Second)
				return nil
			})

			if err := runner.Run(ctx); err != nil {
				log.Printf("GPU worker %d stopped: %v", workerID, err)
			}
		}(i + 1)
	}
}

func startCPUWorkerPool(ctx context.Context, scheduler kv.Scheduler) {
	for i := 0; i < 3; i++ {
		go func(workerID int) {
			runnerID := fmt.Sprintf("cpu-worker-%d", workerID)
			runner := kv.NewTaskRunner(scheduler, runnerID)

			runner.PollInterval = 2 * time.Second

			// Register CPU-specific handlers - by registering only these handlers,
			// this worker will only handle "cpu-intensive" and "data-processing" tasks.
			runner.RegisterHandler("cpu-intensive", func(ctx context.Context, task kv.Task) error {
				var payload map[string]any
				if err := json.Unmarshal(task.Payload, &payload); err != nil {
					return fmt.Errorf("failed to unmarshal task payload: %w", err)
				}
				fmt.Printf("[%s] Performing CPU-intensive task: %+v\n", runnerID, payload)
				// Simulate work.
				time.Sleep(1500 * time.Millisecond)
				return nil
			})

			runner.RegisterHandler("data-processing", func(ctx context.Context, task kv.Task) error {
				var payload map[string]any
				if err := json.Unmarshal(task.Payload, &payload); err != nil {
					return fmt.Errorf("failed to unmarshal task payload: %w", err)
				}
				fmt.Printf("[%s] Processing data: %+v\n", runnerID, payload)
				// Simulate work.
				time.Sleep(1 * time.Second)
				return nil
			})

			if err := runner.Run(ctx); err != nil {
				log.Printf("CPU worker %d stopped: %v", workerID, err)
			}
		}(i + 1)
	}
}

func startGeneralWorkerPool(ctx context.Context, scheduler kv.Scheduler) {
	for i := 0; i < 2; i++ {
		go func(workerID int) {
			runnerID := fmt.Sprintf("general-worker-%d", workerID)
			runner := kv.NewTaskRunner(scheduler, runnerID)

			runner.PollInterval = 3 * time.Second

			// Register general handlers - by registering only these handlers,
			// this worker will only handle "cleanup", "email", and "log" tasks.
			runner.RegisterHandler("cleanup", func(ctx context.Context, task kv.Task) error {
				var payload map[string]any
				if err := json.Unmarshal(task.Payload, &payload); err != nil {
					return fmt.Errorf("failed to unmarshal task payload: %w", err)
				}
				fmt.Printf("[%s] Performing cleanup: %+v\n", runnerID, payload)
				// Simulate work.
				time.Sleep(500 * time.Millisecond)
				return nil
			})

			runner.RegisterHandler("email", func(ctx context.Context, task kv.Task) error {
				var payload map[string]any
				if err := json.Unmarshal(task.Payload, &payload); err != nil {
					return fmt.Errorf("failed to unmarshal task payload: %w", err)
				}
				fmt.Printf("[%s] Sending email: %+v\n", runnerID, payload)
				// Simulate work.
				time.Sleep(300 * time.Millisecond)
				return nil
			})

			runner.RegisterHandler("log", func(ctx context.Context, task kv.Task) error {
				var payload map[string]any
				if err := json.Unmarshal(task.Payload, &payload); err != nil {
					return fmt.Errorf("failed to unmarshal task payload: %w", err)
				}
				fmt.Printf("[%s] Logging: %+v\n", runnerID, payload)
				// Simulate work.
				time.Sleep(100 * time.Millisecond)
				return nil
			})

			if err := runner.Run(ctx); err != nil {
				log.Printf("General worker %d stopped: %v", workerID, err)
			}
		}(i + 1)
	}
}
