# kv

A KV store on top of sqlite / rqlite / postgres with graph database functionality, task scheduling, and built-in pagination support.

It can be used as a CLI tool, or as a Go library.
**Prerequisites for visualization:**

- Install Graphviz: `brew install graphviz` (macOS) or `apt-get install graphviz` (Ubuntu)
- The `dot` command converts DOT files to various image formats (PNG, SVG, PDF)

**Alternative visualization options:**

- Use online tools like [Graphviz Online](https://dreampuf.github.io/GraphvizOnline/)
- Import Mermaid format into GitHub/GitLab markdown or [Mermaid Live Editor](https://mermaid.live/)
- Use VS Code extensions for DOT/Mermaid preview

## Library Usage

The library provides three main interfaces:

- `Store` - Key-value operations, streaming, and locking
- `Scheduler` - Task scheduling and coordination
- `Graph` - Graph database functionality for relationships between entities

The Store and Scheduler interfaces have 3 implementations:

- `sqlitekv` - SQLite implementation
- `rqlitekv` - RQLite implementation  
- `postgreskv` - PostgreSQL implementation

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

# Graph operations:
kv graph add-edge <from-type> <from-id> <to-type> <to-id> <edge-type> [<properties>]
kv graph get-edge <from-type> <from-id> <to-type> <to-id> <edge-type>
kv graph get-outgoing <entity-type> <entity-id> [<edge-type>]
kv graph get-incoming <entity-type> <entity-id> [<edge-type>]
kv graph remove-edge <from-type> <from-id> <to-type> <to-id> <edge-type>
kv graph find-path <from-type> <from-id> <to-type> <to-id> [<edge-type>]
kv graph view

# Patch a key:
echo '{"field": "value"}' | kv patch <key>

# Benchmark operations:
kv benchmark get [<x> [<n> [<w>]]]
kv benchmark put [<n> [<w>]]
kv benchmark patch [<n> [<w>]]
```

### Graph Examples

```bash
# Store some entities first.
echo '{"name": "Alice", "age": 30}' | kv put user/alice
echo '{"name": "Bob", "age": 25}' | kv put user/bob
echo '{"title": "Hello World", "author": "alice"}' | kv put post/1

# Create relationships between entities.
echo '{"since": "2024-01-01"}' | kv graph add-edge User alice User bob follows
echo '{"timestamp": "2024-01-01T10:00:00Z"}' | kv graph add-edge User alice Post 1 authored

# Query relationships.
kv graph get-edge User alice User bob follows
kv graph get-outgoing User alice follows
kv graph get-incoming User bob follows

# Find paths between entities.
kv graph find-path User alice Post 1 authored

# Remove relationships.
kv graph remove-edge User alice User bob follows

# Visualize the graph.
kv graph view
```

### Complete Graph Example with Visualization

Here's a complete example of building a social network graph and generating a PNG visualization:

```bash
# Initialize the store.
kv init

# Create user entities.
echo '{"name": "Alice", "bio": "Software Engineer"}' | kv put user/alice
echo '{"name": "Bob", "bio": "Data Scientist"}' | kv put user/bob  
echo '{"name": "Charlie", "bio": "Product Manager"}' | kv put user/charlie
echo '{"name": "Diana", "bio": "Designer"}' | kv put user/diana

# Create post entities.
echo '{"title": "Hello World", "content": "My first post"}' | kv put post/1
echo '{"title": "Graph Databases", "content": "Exploring relationships"}' | kv put post/2
echo '{"title": "Design Principles", "content": "UX best practices"}' | kv put post/3

# Create social relationships.
echo '{"since": "2024-01-15", "status": "active"}' | kv graph add-edge User alice User bob follows
echo '{"since": "2024-02-01", "status": "active"}' | kv graph add-edge User alice User charlie follows
echo '{"since": "2024-01-20", "status": "active"}' | kv graph add-edge User bob User diana follows
echo '{"since": "2024-02-10", "status": "active"}' | kv graph add-edge User charlie User alice follows

# Create authorship relationships.
echo '{"timestamp": "2024-01-01T10:00:00Z", "draft_count": 3}' | kv graph add-edge User alice Post 1 authored
echo '{"timestamp": "2024-01-15T14:30:00Z", "draft_count": 5}' | kv graph add-edge User bob Post 2 authored
echo '{"timestamp": "2024-02-01T09:15:00Z", "draft_count": 2}' | kv graph add-edge User diana Post 3 authored

# Create engagement relationships.
echo '{"timestamp": "2024-01-02T11:00:00Z", "reaction": "love"}' | kv graph add-edge User bob Post 1 liked
echo '{"timestamp": "2024-01-16T15:00:00Z", "reaction": "thumbs_up"}' | kv graph add-edge User alice Post 2 liked
echo '{"timestamp": "2024-02-02T10:30:00Z", "reaction": "love"}' | kv graph add-edge User charlie Post 3 liked

# Generate DOT visualization and convert to PNG.
kv graph view "*" "*" > social_network.dot
dot -Tpng social_network.dot -o social_network.png

# View specific subgraphs.
# All relationships from Alice:
kv graph view User alice > alice_network.dot
dot -Tpng alice_network.dot -o alice_network.png

# Only "follows" relationships:
kv graph view "*" "*" --edge-type=follows > follows_only.dot  
dot -Tpng follows_only.dot -o follows_only.png

# Generate Mermaid format (for web/markdown):
kv graph view "*" "*" --format=mermaid > social_network.mmd
```

The generated DOT file will look like this:

```dot
digraph G {
  rankdir=LR;
  node [shape=box, style=rounded];

  "Post:1";
  "Post:2"; 
  "Post:3";
  "User:alice";
  "User:bob";
  "User:charlie";
  "User:diana";

  "User:alice" -> "Post:1" [label="authored\n{\"timestamp\": \"2024-01-01T10:00:00Z\", \"draft_count\": 3}"];
  "User:alice" -> "User:bob" [label="follows\n{\"since\": \"2024-01-15\", \"status\": \"active\"}"];
  "User:alice" -> "User:charlie" [label="follows\n{\"since\": \"2024-02-01\", \"status\": \"active\"}"];
  "User:bob" -> "Post:2" [label="authored\n{\"timestamp\": \"2024-01-15T14:30:00Z\", \"draft_count\": 5}"];
  "User:bob" -> "User:diana" [label="follows\n{\"since\": \"2024-01-20\", \"status\": \"active\"}"];
  "User:charlie" -> "User:alice" [label="follows\n{\"since\": \"2024-02-10\", \"status\": \"active\"}"];
}
```

**Prerequisites for visualization:**

- Install Graphviz: `brew install graphviz` (macOS) or `apt-get install graphviz` (Ubuntu)
- The `dot` command converts DOT files to various image formats (PNG, SVG, PDF)

**Alternative visualization options:**

- Use online tools like [Graphviz Online](https://dreampuf.github.io/GraphvizOnline/)
- Import Mermaid format into GitHub/GitLab markdown or [Mermaid Live Editor](https://mermaid.live/)
- Use VS Code extensions for DOT/Mermaid preview

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
  graph add-edge <from-type> <from-id> <to-type> <to-id> <edge-type> Add an edge between two entities.
  graph get-edge <from-type> <from-id> <to-type> <to-id> <edge-type> Get a specific edge.
  graph get-outgoing <entity-type> <entity-id> [<edge-type>] Get outgoing edges from an entity.
  graph get-incoming <entity-type> <entity-id> [<edge-type>] Get incoming edges to an entity.
  graph remove-edge <from-type> <from-id> <to-type> <to-id> <edge-type> Remove an edge.
  graph find-path <from-type> <from-id> <to-type> <to-id> [<edge-type>] Find shortest path.
  graph view                               Generate graph visualization output.
  benchmark get [<x> [<n> [<w>]]]          Benchmark getting records.
  benchmark put [<n> [<w>]]                Benchmark putting records.
  benchmark patch [<n> [<w>]]              Benchmark patching records.

Run "kv <command> --help" for more information on a command.
```

### Pagination

Most query operations support pagination using `offset` and `limit` parameters to handle large datasets efficiently:

```go
// Get keys with pagination.
rows, err := store.GetPrefix(ctx, "user:", 0, 100)  // First 100 records
rows, err = store.GetPrefix(ctx, "user:", 100, 100) // Next 100 records

// List keys with pagination.
rows, err := store.List(ctx, 0, 50)    // First 50 keys
rows, err = store.List(ctx, 50, 50)    // Next 50 keys

// Range queries with pagination.
rows, err := store.GetRange(ctx, "a", "m", 0, 1000)  // First 1000 keys in range

// Task listing with pagination.
tasks, err := scheduler.List(ctx, kv.TaskStatusPending, "", 0, 20) // First 20 pending tasks
```

Operations that support pagination:

- `GetPrefix()` - Get keys by prefix
- `GetRange()` - Get keys in range
- `List()` - List all keys  
- `DeletePrefix()` - Delete keys by prefix
- `DeleteRange()` - Delete keys in range
- `Stream()` - Get stream records
- `Scheduler.List()` - List tasks

## Graph Database

The `graph` package provides graph database functionality on top of the KV store, allowing you to model and query relationships between entities:

```go
import "github.com/a-h/kv/graph"

// Create graph instance.
g := graph.New(store)

// Add entities to the store first.
store.Put(ctx, "user/alice", -1, User{ID: "alice", Name: "Alice"})
store.Put(ctx, "user/bob", -1, User{ID: "bob", Name: "Bob"})

// Create relationships.
alice := graph.NewNodeRef("alice", "User")
bob := graph.NewNodeRef("bob", "User")

followEdge := graph.NewEdge(alice, bob, "follows", json.RawMessage(`{"since": "2024-01-01"}`))
err := g.AddEdge(ctx, followEdge)

// Query relationships.
for edge, err := range g.GetOutgoing(ctx, alice, "follows") {
    if err != nil {
        break
    }
    fmt.Printf("Alice follows: %s\n", edge.To.ID)
}

// Find shortest path between entities.
path, found, err := g.FindShortestPath(ctx, alice, bob, "follows")
```

Key features:

- **Directed edges** with typed relationships
- **Edge properties** using JSON metadata
- **Efficient lookups** for incoming/outgoing edges
- **Graph traversal** algorithms (BFS, shortest path)
- **Pagination support** for large graphs
- **Batch operations** for performance

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
  pool, err := sqlitex.NewPool("file:example.db?cache=shared&mode=rwc&_busy_timeout=5000&_txlock=immediate&_journal_mode=WAL", sqlitex.PoolOptions{})
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
pool, err := sqlitex.NewPool("file:data.db?cache=shared&mode=rwc&_busy_timeout=5000&_txlock=immediate&_journal_mode=WAL", sqlitex.PoolOptions{})
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

- `pending` - Task is waiting to be executed
- `running` - Task is currently being processed by a runner
- `completed` - Task finished successfully
- `failed` - Task failed after exhausting retries
- `cancelled` - Task was manually cancelled

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

- `log` - Logs the task payload to stdout
- `echo` - Echoes the task payload

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

- Only one runner processes each task
- Failed runners don't block tasks indefinitely
- Automatic retry with exponential backoff
- Configurable timeouts and retry limits

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

interactive: true

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

### docker-load

```bash
docker load < ./result
docker tag ghcr.io/a-h/kv:`version get` ghcr.io/a-h/kv:latest
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
