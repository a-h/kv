package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/a-h/kv"
	"github.com/a-h/kv/sqlitekv"
	"zombiezen.com/go/sqlite/sqlitex"
)

func main() {
	ctx := context.Background()

	// Create an in-memory SQLite database for this example.
	pool, err := sqlitex.NewPool("file::memory:?mode=memory&cache=shared", sqlitex.PoolOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	scheduler := sqlitekv.NewScheduler(pool)

	// Initialize the database schema.
	if err := scheduler.Init(ctx); err != nil {
		log.Fatal(err)
	}

	// Create different types of tasks.
	fmt.Println("Creating tasks of different types...")

	// GPU-intensive tasks.
	gpuPayload, _ := json.Marshal(map[string]any{
		"model":    "llama3",
		"input":    "Generate a creative story",
		"gpu_type": "A100",
	})

	for i := 0; i < 3; i++ {
		task := kv.NewTask("gpu-inference", gpuPayload)
		if err := scheduler.New(ctx, task); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Created GPU task: %s\n", task.ID)
	}

	// CPU-intensive tasks.
	cpuPayload, _ := json.Marshal(map[string]any{
		"data":      []int{1, 2, 3, 4, 5},
		"algorithm": "quicksort",
	})

	for i := 0; i < 2; i++ {
		task := kv.NewTask("cpu-compute", cpuPayload)
		if err := scheduler.New(ctx, task); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Created CPU task: %s\n", task.ID)
	}

	// I/O tasks.
	ioPayload, _ := json.Marshal(map[string]any{
		"source": "/data/input.csv",
		"target": "/data/processed.csv",
	})

	task := kv.NewTask("file-processing", ioPayload)
	if err := scheduler.New(ctx, task); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Created I/O task: %s\n", task.ID)

	fmt.Println("\n--- Worker Scenarios ---")

	// Scenario 1: GPU worker that only handles GPU tasks.
	fmt.Println("1. GPU Worker (handles only 'gpu-inference' tasks):")
	gpuRunner := kv.NewTaskRunner(scheduler, "gpu-worker-1")

	// Register GPU-specific handler - by registering only this handler,
	// this worker will only handle "gpu-inference" tasks.
	gpuRunner.RegisterHandler("gpu-inference", func(ctx context.Context, task kv.Task) error {
		var payload map[string]any
		if err := json.Unmarshal(task.Payload, &payload); err != nil {
			return fmt.Errorf("failed to unmarshal task payload: %w", err)
		}
		fmt.Printf("  🔥 GPU Worker processing: %s (model: %s)\n", task.ID, payload["model"])
		return nil
	})

	// Fetch and process GPU tasks.
	for i := 0; i < 3; i++ {
		task, locked, err := scheduler.Lock(ctx, gpuRunner.RunnerID, 5*time.Minute, gpuRunner.TaskTypes()...)
		if err != nil {
			log.Fatal(err)
		}
		if !locked {
			fmt.Println("  No more GPU tasks available")
			break
		}

		// Process the task.
		if err := gpuRunner.Handlers[task.Name](ctx, task); err != nil {
			log.Fatal(err)
		}

		// Mark as completed using Release.
		if err := scheduler.Release(ctx, task.ID, gpuRunner.RunnerID, kv.TaskStatusCompleted, ""); err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("\n2. CPU Worker (handles 'cpu-compute' and 'file-processing' tasks):")
	cpuRunner := kv.NewTaskRunner(scheduler, "cpu-worker-1")

	// Register CPU-specific handlers - by registering only these handlers,
	// this worker will only handle "cpu-compute" and "file-processing" tasks.
	cpuRunner.RegisterHandler("cpu-compute", func(ctx context.Context, task kv.Task) error {
		var payload map[string]any
		if err := json.Unmarshal(task.Payload, &payload); err != nil {
			return fmt.Errorf("failed to unmarshal task payload: %w", err)
		}
		fmt.Printf("  ⚡ CPU Worker processing compute: %s (algorithm: %s)\n", task.ID, payload["algorithm"])
		return nil
	})

	cpuRunner.RegisterHandler("file-processing", func(ctx context.Context, task kv.Task) error {
		var payload map[string]any
		if err := json.Unmarshal(task.Payload, &payload); err != nil {
			return fmt.Errorf("failed to unmarshal task payload: %w", err)
		}
		fmt.Printf("  📁 CPU Worker processing file: %s (source: %s)\n", task.ID, payload["source"])
		return nil
	})

	// Fetch and process CPU/I/O tasks.
	for i := 0; i < 5; i++ {
		task, locked, err := scheduler.Lock(ctx, cpuRunner.RunnerID, 5*time.Minute, cpuRunner.TaskTypes()...)
		if err != nil {
			log.Fatal(err)
		}
		if !locked {
			fmt.Println("  No more CPU/I/O tasks available")
			break
		}

		// Process the task.
		if err := cpuRunner.Handlers[task.Name](ctx, task); err != nil {
			log.Fatal(err)
		}

		// Mark as completed using Release.
		if err := scheduler.Release(ctx, task.ID, cpuRunner.RunnerID, kv.TaskStatusCompleted, ""); err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("\n3. General Worker (handles all task types):")
	generalRunner := kv.NewTaskRunner(scheduler, "general-worker-1")
	// Don't set TaskTypes - this means it handles all types.

	generalRunner.RegisterHandler("gpu-inference", func(ctx context.Context, task kv.Task) error {
		fmt.Printf("  🌟 General Worker handling GPU task: %s\n", task.ID)
		return nil
	})

	generalRunner.RegisterHandler("cpu-compute", func(ctx context.Context, task kv.Task) error {
		fmt.Printf("  🌟 General Worker handling CPU task: %s\n", task.ID)
		return nil
	})

	generalRunner.RegisterHandler("file-processing", func(ctx context.Context, task kv.Task) error {
		fmt.Printf("  🌟 General Worker handling I/O task: %s\n", task.ID)
		return nil
	})

	// Try to fetch any remaining tasks.
	task, locked, err := scheduler.Lock(ctx, generalRunner.RunnerID, 5*time.Minute)
	if err != nil {
		log.Fatal(err)
	}
	if locked {
		fmt.Printf("  General Worker found remaining task: %s (type: %s)\n", task.ID, task.Name)

		if err := generalRunner.Handlers[task.Name](ctx, task); err != nil {
			log.Fatal(err)
		}

		if err := scheduler.Release(ctx, task.ID, generalRunner.RunnerID, kv.TaskStatusCompleted, ""); err != nil {
			log.Fatal(err)
		}
	} else {
		fmt.Println("  No remaining tasks - all processed!")
	}

	fmt.Println("\n--- Summary ---")

	// Show task completion status.
	allTasks, err := scheduler.List(ctx, "", "", 0, 100)
	if err != nil {
		log.Fatal(err)
	}

	completed := 0
	for _, task := range allTasks {
		if task.Status == kv.TaskStatusCompleted {
			completed++
		}
	}

	fmt.Printf("Total tasks: %d\n", len(allTasks))
	fmt.Printf("Completed: %d\n", completed)
	fmt.Printf("Task filtering allows workers to focus on their specialties! 🎯\n")
}
