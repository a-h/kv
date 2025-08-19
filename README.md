# kv

A KV store on top of sqlite / rqlite / postgres.

It can be used as a CLI tool, or as a Go library.

## CLI

The CLI tool can be used to interact with a sqlite, rqlite, or postgres database.

To connect to an rqlite database, use `--type rqlite --connection 'http://localhost:4001?user=admin&password=secret'`.

To connect to a sqlite database, use `--type sqlite --connection 'file:data.db?mode=rwc'`.

To connect to a postgres database, use `--type postgres --connection 'postgres://postgres:secret@localhost:5432/postgres?sslmode=disable'`.


```bash
# Create a new data.db file (use the --connection flag to specify a different file).
kv init

# Put a key into the store.
echo '{"hello": "world"}' | kv put --key hello

# Get the key back.
kv get key --key hello

# List all keys in the store.
kv list

# Delete the key.
kv delete hello

# Stream records (new):
kv stream get <seq> <limit>

# Get stream sequence number:
kv stream seq

# Trim stream:
kv stream trim <seq>

kv consumer get <name>
kv consumer status <name>
kv consumer commit <name> <seq>
kv consumer delete <name>
# Lock commands:
kv lock acquire <name> <lockedBy> [--duration=10m]
kv lock release <name> <lockedBy>
```

### CLI Usage

```bash
Usage: kv [global flags] <command> [subcommand] [flags]

Global Flags:
  --type        The type of KV store to use. One of: sqlite, rqlite, postgres. Default: sqlite
  --connection  The connection string to use. Default: file:data.db?mode=rwc

Commands:
  init                 Initialize the store.
  get key              Get a key.
  get prefix           Get all keys with a given prefix.
  get range            Get a range of keys.
  list                 List all keys.
  put                  Put a key.
  patch                Patch a key.
  delete key           Delete a key.
  delete prefix        Delete all keys with a given prefix.
  delete range         Delete a range of keys.
  count all            Count the number of keys.
  count prefix         Count the number of keys with a given prefix.
  count range          Count the number of keys in a range.
  stream get <seq> <limit>  Stream records (for replication or change feed).
  stream seq           Get the current stream sequence number.
  stream trim <seq>     Trim the stream to a sequence number.
  consumer get <name>            Get consumer state.
  consumer status <name>         Get consumer status.
  consumer commit <name> <seq>   Commit a consumer sequence.
  consumer delete <name>         Delete a consumer.
  lock acquire <name> <lockedBy> [--duration=10m]  Acquire a lock (default 10 minutes).
  lock release <name> <lockedBy>                   Release a lock.

Run "kv <command> --help" for more information on a command.
```

## Usage

`Store` is an interface with 3 implementations:

* `sqlitekv`
* `rqlitekv`
* `postgreskv`

```go
func runSqlite(ctx context.Context) (error) {
  // Create a new in-memory SQLite database.
  pool, err := sqlitex.NewPool("file::memory:?mode=memory&cache=shared", sqlitex.PoolOptions{})
  if err != nil {
    return fmt.Errorf("unexpected error creating SQLite pool: %w", err)
  }
  defer pool.Close()

  // Create a store.
  store := sqlitekv.New(pool)

  // Run the example.
  return run(ctx, store)
}

func runRqlite(ctx context.Context) (error) {
  // Create a new Rqlite client.
  client := rqlitehttp.NewClient("http://localhost:4001", nil)

  // Create a store.
  store := rqlitekv.New(db)

  // Run the example.
  return run(ctx, store)
}

func runPostres(ctx context.Context) (error) {
  // Create a new Postgres client.
  db, err := sqlx.Connect("postgres", "postgres://postgres:secret@localhost:5432/postgres?sslmode=disable")
  if err != nil {
    return fmt.Errorf("unexpected error creating Postgres client: %w", err)
  }
  defer db.Close()

  // Create a store.
  store := postgreskv.New(db)

  // Run the example.
  return run(ctx, store)
}

func run(ctx context.Context, store kv.Store) (error) {
  // Initalize the store.
  if err := store.Init(ctx); err != nil {
    return fmt.Errorf("unexpected error initializing store: %w", err)
  }

  // Create a new person.
  alice := Person{
    Name:         "Alice",
    PhoneNumbers: []string{"123-456-7890"},
  }
  err := store.Put(ctx, "person/alice", -1, alice)
  if err != nil {
    return fmt.Errorf("unexpected error putting data: %w", err)
  }

  // Get the person we just added.
  var p Person
  r, ok, err := store.Get(ctx, "person/alice", &p)
  if err != nil {
    return fmt.Errorf("unexpected error getting data: %w", err)
  }
  if !ok {
    return fmt.Errorf("expected data not found")
  }
  if p.Name != alice.Name {
    return fmt.Errorf("expected name %q, got %q", alice.Name, p.Name)
  }
  if r.Version != 1 {
    return fmt.Errorf("expected version 1, got %d", r.Version)
  }

  // List everything in the store.
  records, err := store.List(ctx, 0, 10)
  if err != nil {
    return fmt.Errorf("unexpected error listing data: %w", err)
  }
  // Convert the untyped records to a slice of the underlying values.
  values, err := ValuesOf[Person](records)
  if err != nil {
    return fmt.Errorf("failed to convert records to values: %w", err)
  }
  for _, person := range values {
    fmt.Printf("Person: %#v\n", person)
  }

  return nil
}
```

## Features

`Store` interface (see `store.go`):

```go
type Store interface {
  Init(ctx context.Context) error
  Get(ctx context.Context, key string, v any) (r Record, ok bool, err error)
  GetPrefix(ctx context.Context, prefix string, offset, limit int) (rows []Record, err error)
  GetRange(ctx context.Context, from, to string, offset, limit int) (rows []Record, err error)
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
  Stream(ctx context.Context, seq int, limit int) (rows []StreamRecord, err error)
  StreamSeq(ctx context.Context) (seq int, err error)
  StreamTrim(ctx context.Context, seq int) (err error)
  LockAcquire(ctx context.Context, name string, lockedBy string, duration time.Duration) (acquired bool, err error)
  LockRelease(ctx context.Context, name string, lockedBy string) (err error)
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

### docker-run-rqlite

```bash
docker run -v "$PWD/auth.json:/mnt/rqlite/auth.json" -v "$PWD/.rqlite:/mnt/data" -p 4001:4001 -p 4002:4002 -p 4003:4003 rqlite/rqlite:latest
```

### rqlite-db-shell

interactive: true

```bash
rqlite --user='admin:secret'
```

### docker-run-postgres

```bash
docker run -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=secret -e POSTGRES_DB=testdb -p 5432:5432 postgres:latest
```

### postgres-db-shell

Env: PGPASSWORD=secret
interactive: true

```bash
pgcli -h localhost -u postgres -d postgres
```
