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
  benchmark get [<x> [<n> [<w>]]]          Benchmark getting records.
  benchmark put [<n> [<w>]]                Benchmark putting records.
  benchmark patch [<n> [<w>]]              Benchmark patching records.

Run "kv <command> --help" for more information on a command.
```

## Library Usage

`Store` is an interface with 3 implementations:

* `sqlitekv` - SQLite implementation
* `rqlitekv` - RQLite implementation  
* `postgreskv` - PostgreSQL implementation

### Simple Example

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
  store := sqlitekv.New(pool)

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
// SQLite.
pool, err := sqlitex.NewPool("file:data.db?mode=rwc", sqlitex.PoolOptions{})
if err != nil {
  return err
}
defer pool.Close()
store := sqlitekv.New(pool)
```

#### RQLite

```go
// RQLite.
client := rqlitehttp.NewClient("http://localhost:4001", nil)
client.SetBasicAuth("admin", "secret") // If auth is required.
store := rqlitekv.New(client)
```

#### PostgreSQL

```go
// PostgreSQL.
pool, err := pgxpool.New(ctx, "postgres://user:password@localhost:5432/dbname?sslmode=disable")
if err != nil {
  return err
}
defer pool.Close()
store := postgreskv.New(pool)
```

## Store Interface

The `Store` interface provides the following methods:

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

See `store.go` for mutation helpers and record helpers.

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
