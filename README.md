# kv

A KV store on top of sqlite / rqlite / postgres.

It can be used as a CLI tool, or as a Go library.

## CLI

The CLI tool can be used to interact with a sqlite, rqlite, or postgres database.

To connect to an rqlite database, use `--type rqlite --connection 'http://localhost:4001?user=admin&password=secret'`.

To connect to a sqlite database, use `--type sqlite --connection 'file:data.db?mode=rwc'`.

To connect to a postgres database, use `--type postgres --connection 'postgres://postgres:secret@localhost:5432/testdb?sslmode=disable'`.

Start the development services with `xc docker-run-services` to get RQLite, PostgreSQL, and NATS running locally.

```bash
# Create a new data.db file (use the --connection flag to specify a different file).
kv init

# Put a key into the store.
echo '{"hello": "world"}' | kv put <key>

# Get the key back.
kv get key <key>

# Get all keys with a prefix.
kv get prefix <prefix> [<offset> [<limit>]]

# Get a range of keys.
kv get range <from> <to> [<offset> [<limit>]]

# List all keys in the store.
kv list [<offset> [<limit>]]

# Delete a key.
kv delete <key>

# Count operations:
kv count all
kv count prefix <prefix>
kv count range <from> <to>

# Stream operations:
kv stream get [<seq> [<limit>]] --of-type=<type>
kv stream seq
kv stream trim <seq>

# Consumer operations:
kv consumer get <name> --of-type=<type> --limit=<limit>
kv consumer stream <stream-name> <consumer-name> --of-type=<type> --commit-mode=<mode> --limit=<limit>
kv consumer status <name>
kv consumer commit <name> <last-seq>
kv consumer delete <name>

# Lock operations:
kv lock acquire <name> <locked-by> --duration=<duration>
kv lock release <name> <locked-by>
kv lock status <name>

# Task operations:
echo '{"message": "Hello World"}' | kv task new log
kv task list [--status=pending|running|completed|failed|cancelled] [--name=<name>]
kv task get <task-id>
kv task cancel <task-id>
kv task run [--runner-id=<id>] [--poll-interval=10s] [--lock-duration=5m]

# Patch a key:
echo '{"field": "value"}' | kv patch <key>

# Benchmark operations:
kv benchmark get [<x> [<n> [<w>]]]
kv benchmark put [<n> [<w>]]
kv benchmark patch [<n> [<w>]]
```

### CLI Usage

```bash
Usage: kv <command> [flags]

Global Flags:
  --type        The type of KV store to use. One of: sqlite, rqlite, postgres. Default: sqlite
  --connection  The connection string to use. Default: file:data.db?mode=rwc

Commands:
  init                                      Initialize the store.
  get key <key>                            Get a key.
  get prefix <prefix> [<offset> [<limit>]] Get all keys with a given prefix.
  get range <from> <to> [<offset> [<limit>]] Get a range of keys.
  delete <key>                             Delete a key.
  count all                                Count the number of keys.
  count prefix <prefix>                    Count the number of keys with a given prefix.
  count range <from> <to>                  Count the number of keys in a range.
  list [<offset> [<limit>]]                List all keys.
  put <key>                                Put a key (reads from stdin).
  patch <key>                              Patch a key (reads from stdin).
  stream get [<seq> [<limit>]]             Get a batch of records from the stream.
  stream seq                               Show the current stream sequence number.
  stream trim <seq>                        Trim the stream to a given sequence number.
  consumer get <name>                      Get a batch of records for a consumer.
  consumer stream <stream-name> <consumer-name> Continuously consume records from a stream.
  consumer status <name>                   Show the status of a consumer.
  consumer commit <name> <last-seq>        Commit the consumer position to a sequence number.
  consumer delete <name>                   Delete a consumer.
  lock acquire <name> <locked-by>          Acquire a lock.
  lock release <name> <locked-by>          Release a lock.
  lock status <name>                       Show the status of a lock.
  task new <name>                          Create a new task (reads payload from stdin).
  task list                                List tasks.
  task get <id>                            Get a task by ID.
  task cancel <id>                         Cancel a task by ID.
  task run                                 Run task runner to process scheduled tasks.
  benchmark get [<x> [<n> [<w>]]]          Benchmark getting records.
  benchmark put [<n> [<w>]]                Benchmark putting records.
  benchmark patch [<n> [<w>]]              Benchmark patching records.

Run "kv <command> --help" for more information on a command.
```

## Library Usage

The library provides two main interfaces:

* `Store` - Key-value operations, streaming, and locking
* `Scheduler` - Task scheduling and coordination

Both interfaces have 3 implementations:

* `sqlitekv` - SQLite implementation
* `rqlitekv` - RQLite implementation  
* `postgreskv` - PostgreSQL implementation

### Simple Store Example

```go
package main

import (
  "context"
  "fmt"
  "log"

  "github.com/a-h/kv/sqlitekv"
  "zombiezen.com/go/sqlite/sqlitex"
)

type Person struct {
  Name string   `json:"name"`
  Age  int      `json:"age"`
  City string   `json:"city"`
}

func main() {
  ctx := context.Background()
  
  // Create a new SQLite database.
  pool, err := sqlitex.NewPool("file:example.db?mode=rwc", sqlitex.PoolOptions{})
  if err != nil {
    log.Fatal(err)
  }
  defer pool.Close()

  // Create a store.
  store := sqlitekv.NewStore(pool)

  // Initialize the store (creates tables).
  if err := store.Init(ctx); err != nil {
    log.Fatal(err)
  }

  // Put a record.
  person := Person{Name: "Alice", Age: 30, City: "New York"}
  if err := store.Put(ctx, "person:alice", -1, person); err != nil {
    log.Fatal(err)
  }

  // Get the record back.
  var retrieved Person
  record, found, err := store.Get(ctx, "person:alice", &retrieved)
  if err != nil {
    log.Fatal(err)
  }
  if !found {
    log.Fatal("person not found")
  }

  fmt.Printf("Found person: %+v (version: %d)\n", retrieved, record.Version)
  // Output: Found person: {Name:Alice Age:30 City:New York} (version: 1)
}
```

### Database-Specific Examples

#### SQLite

```go
// SQLite Store.
pool, err := sqlitex.NewPool("file:data.db?mode=rwc", sqlitex.PoolOptions{})
if err != nil {
  return err
}
defer pool.Close()
store := sqlitekv.NewStore(pool)

// SQLite Scheduler.
scheduler := sqlitekv.NewScheduler(pool)
```

#### RQLite

```go
// RQLite Store.
client := rqlitehttp.NewClient("http://localhost:4001", nil)
client.SetBasicAuth("admin", "secret") // If auth is required.
store := rqlitekv.NewStore(client)

// RQLite Scheduler.
scheduler := rqlitekv.NewScheduler(client)
```

#### PostgreSQL

```go
// PostgreSQL Store.
pool, err := pgxpool.New(ctx, "postgres://user:password@localhost:5432/dbname?sslmode=disable")
if err != nil {
  return err
}
defer pool.Close()
store := postgreskv.NewStore(pool)

// PostgreSQL Scheduler.
scheduler := postgreskv.NewScheduler(pool)
```

## Store Interface

The `Store` interface provides key-value operations, streaming, and locking:

```go
type Store interface {
  Init(ctx context.Context) error
  Get(ctx context.Context, key string, v any) (r Record, ok bool, err error)
  GetPrefix(ctx context.Context, prefix string, offset, limit int) (rows []Record, err error)
  GetRange(ctx context.Context, from, to string, offset, limit int) (rows []Record, err error)
  GetType(ctx context.Context, t Type, offset, limit int) (rows []Record, err error)
  List(ctx context.Context, start, limit int) (rows []Record, err error)
  Put(ctx context.Context, key string, version int, value any) (err error)
  Delete(ctx context.Context, keys ...string) (rowsAffected int, err error)
  DeletePrefix(ctx context.Context, prefix string, offset, limit int) (rowsAffected int, err error)
  DeleteRange(ctx context.Context, from, to string, offset, limit int) (rowsAffected int, err error)
  Count(ctx context.Context) (n int, err error)
  CountPrefix(ctx context.Context, prefix string) (count int, err error)
  CountRange(ctx context.Context, from, to string) (count int, err error)
  Patch(ctx context.Context, key string, version int, patch any) (err error)
  MutateAll(ctx context.Context, mutations ...Mutation) (rowsAffected []int, err error)
  Stream(ctx context.Context, t Type, seq int, limit int) (rows []StreamRecord, err error)
  StreamSeq(ctx context.Context) (seq int, err error)
  StreamTrim(ctx context.Context, seq int) (err error)
  LockAcquire(ctx context.Context, name string, lockedBy string, duration time.Duration) (acquired bool, err error)
  LockRelease(ctx context.Context, name string, lockedBy string) (err error)
  LockStatus(ctx context.Context, name string) (status LockStatus, ok bool, err error)
  SetNow(now func() time.Time)
}
```

## Scheduler Interface

The `Scheduler` interface provides task scheduling and coordination:

```go
type Scheduler interface {
  New(ctx context.Context, task Task) (err error)
  Get(ctx context.Context, id string) (task Task, ok bool, err error)
  List(ctx context.Context, status TaskStatus, name string, offset, limit int) (tasks []Task, err error)
  Cancel(ctx context.Context, id string) (err error)
  Lock(ctx context.Context, runnerID string, lockDuration time.Duration, taskTypes ...string) (task Task, locked bool, err error)
  Release(ctx context.Context, id string, runnerID string, status TaskStatus, errorMessage string) (err error)
}
```

See `store.go` for mutation helpers and record helpers.

## Scheduled Tasks

The KV store supports scheduled and one-off tasks that are executed with distributed coordination to ensure only one compute node processes each task.

### Task Types

Tasks are one-off executions scheduled for a specific time.

For recurring/cron-style scheduling, clients can implement their own schedulers using the existing lock primitives to ensure only one scheduler instance runs, then create new one-off tasks as needed.

### Task States

* `pending` - Task is waiting to be executed
* `running` - Task is currently being processed by a runner
* `completed` - Task finished successfully
* `failed` - Task failed after exhausting retries
* `cancelled` - Task was manually cancelled

### Creating Tasks

```go
// Create a scheduler.
scheduler := sqlitekv.NewScheduler(pool)

// Create a one-off task.
task := kv.Task{
    ID:             uuid.New().String(),
    Name:           "send-email",
    Payload:        []byte(`{"to": "user@example.com", "subject": "Hello"}`),
    Status:         kv.TaskStatusPending,
    Created:        time.Now().UTC(),
    ScheduledFor:   time.Now().UTC().Add(5 * time.Minute), // Run in 5 minutes
    MaxRetries:     3,
    TimeoutSeconds: 300,
}

err := scheduler.New(ctx, task)
```

### Task Runner

```go
// Create and configure a task runner.
scheduler := sqlitekv.NewScheduler(pool)
runner := kv.NewTaskRunner(scheduler, "worker-1")
runner.PollInterval = 10 * time.Second
runner.LockDuration = 5 * time.Minute

// Register task handlers.
runner.RegisterHandler("send-email", func(ctx context.Context, task kv.Task) error {
    var payload struct {
        To      string `json:"to"`
        Subject string `json:"subject"`
    }
    if err := json.Unmarshal(task.Payload, &payload); err != nil {
        return fmt.Errorf("invalid payload: %w", err)
    }

    // Send email logic here.
    fmt.Printf("Sending email to %s: %s\n", payload.To, payload.Subject)
    return nil
})

runner.RegisterHandler("cleanup", func(ctx context.Context, task kv.Task) error {
    // Cleanup logic here.
    return nil
})

// Run the task runner (blocks until context is cancelled)
err := runner.Run(ctx)
```

### CLI Examples

```bash
# Create a log task.
echo '{"message": "Hello World"}' | kv task new log

# List all tasks.
kv task list

# List only pending tasks.
kv task list --status=pending

# Get a specific task.
kv task get <task-id>

# Cancel a task.
kv task cancel <task-id>

# Run task processor.
kv task run --poll-interval=5s --lock-duration=2m
```

### Built-in Task Handlers

The CLI task runner includes these example handlers:

* `log` - Logs the task payload to stdout
* `echo` - Echoes the task payload

### Implementing Cron Scheduling

For recurring tasks, implement a scheduler client using the existing lock primitives:

```go
func runCronScheduler(ctx context.Context, store kv.Store, scheduler kv.Scheduler) error {
    ticker := time.NewTicker(time.Minute)
    defer ticker.Stop()
    
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-ticker.C:
            // Try to acquire or extend the scheduler lock.
            acquired, err := store.LockAcquire(ctx, "cron-scheduler", "instance-1", 5*time.Minute)
            if err != nil {
              //TODO: Log error.
              continue
            }
            if !acquired {
              continue
            }
  
            //TODO Schedule any due cron jobs using scheduler.New()...
  
            // Release lock
            store.LockRelease(ctx, "cron-scheduler", "instance-1")
        }
    }
}
```

### Distributed Coordination

Tasks use the same locking mechanism as stream consumers to ensure:

* Only one runner processes each task
* Failed runners don't block tasks indefinitely
* Automatic retry with exponential backoff
* Configurable timeouts and retry limits

## Tasks

### build

```bash
go build -o kv ./cmd/kv/
```

### test

```bash
go test ./...
```

### test-cover

```bash
go test -coverprofile=coverage.out ./...
```

### lint

```bash
golangci-lint run
```

### develop

```bash
nix develop
```

### update-version

```bash
version set
```

### push-tag

Push a semantic version number.

```sh
version push
```

### nix-build

```bash
nix build
```

### docker-build-aarch64

```bash
nix build .#packages.aarch64-linux.docker-image
```

### docker-build-x86_64

```bash
nix build .#packages.x86_64-linux.docker-image
```

### crane-push

env: CONTAINER_REGISTRY=ghcr.io/kv

```bash
nix build .#packages.x86_64-linux.docker-image
cp ./result /tmp/kv.tar.gz
gunzip -f /tmp/kv.tar.gz
crane push /tmp/kv.tar ${CONTAINER_REGISTRY}/kv:v0.0.1
```

### docker-services-run

Interactive: true

Start development services (RQLite, PostgreSQL, NATS with JetStream).

```bash
docker compose up
```

### docker-services-stop

Stop development services.

```bash
docker compose down
```

### docker-services-logs

View logs from development services.

```bash
docker compose logs -f
```

### rqlite-db-shell

interactive: true

```bash
rqlite --user='admin:secret'
```

### postgres-db-shell

Env: PGPASSWORD=secret
interactive: true

```bash
pgcli -h localhost -u postgres -d postgres
```

### nats-cli

Connect to NATS server for testing and monitoring.

```bash
nats --server=nats://localhost:4222 server info
```

### drop-tables

```bash
sqlite3 data.db "DROP TABLE IF EXISTS kv; DROP TABLE IF EXISTS locks; DROP TABLE IF EXISTS stream; DROP TABLE IF EXISTS tasks; DROP TABLE IF EXISTS migration_version;"
echo "DROP TABLE IF EXISTS kv, locks, stream, tasks, migration_version CASCADE;" | PGPASSWORD=secret pgcli -h localhost -u postgres -d postgres
curl -XPOST 'http://admin:secret@localhost:4001/db/execute?pretty&timings' -H "Content-Type: application/json" -d '[
    "DROP TABLE IF EXISTS kv",
    "DROP TABLE IF EXISTS locks", 
    "DROP TABLE IF EXISTS stream",
    "DROP TABLE IF EXISTS tasks",
    "DROP TABLE IF EXISTS migration_version"
]'
```
