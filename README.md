# kv

A KV store on top of sqlite / rqlite / postgres.

It can be used as a CLI tool, or as a Go library.

## CLI

The CLI tool can be used to interact with a sqlite, rqlite, or postgres database.

To connect to an rqlite database, use `--type 'rqlite' --connection 'http://localhost:4001?user=admin&password=secret'`.

To connect to a sqlite database, use `--type 'sqlite' --connection 'file:data.db?mode=rwc'`.

To connect to a postgres database, use `--type 'postgres' --connection 'postgres://postgres:secret@localhost:5432/postgres?sslmode=disable'`.

```bash
# Create a new data.db file (use the --connection flag to specify a different file).
kv init

# Put a key into the store.
echo '{"hello": "world"}' | kv put hello

# Get the key back.
kv get hello

# List all keys in the store.
kv list

# Delete the key.
kv delete hello
```

### CLI Usage

```bash
Usage: kv <command> [flags]

Flags:
  -h, --help             Show context-sensitive help.
      --type="sqlite"    The type of KV store to use.
      --connection="file:data.db?mode=rwc"
                         The connection string to use.

Commands:
  init [flags]
    Initialize the store.

  get <key> [flags]
    Get a key.

  get-prefix <prefix> [<offset> [<limit>]] [flags]
    Get all keys with a given prefix.

  get-range <from> <to> [<offset> [<limit>]] [flags]
    Get a range of keys.

  list [<offset> [<limit>]] [flags]
    List all keys.

  put <key> [flags]
    Put a key.

  delete <key> [flags]
    Delete a key.

  delete-prefix <prefix> [<offset> [<limit>]] [flags]
    Delete all keys with a given prefix.

  delete-range <from> <to> [<offset> [<limit>]] [flags]
    Delete a range of keys.

  count [flags]
    Count the number of keys.

  count-prefix <prefix> [flags]
    Count the number of keys with a given prefix.

  count-range <from> <to> [flags]
    Count the number of keys in a range.

  patch <key> [flags]
    Patch a key.

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

`Store` has the following methods:

```go
// Init initializes the store. It should be called before any other method, and creates the necessary table.
Init(ctx context.Context) error
// Get gets a key from the store, and populates v with the value. If the key does not exist, it returns ok=false.
Get(ctx context.Context, key string, v any) (r Record[T], ok bool, err error)
// GetPrefix gets all keys with a given prefix from the store.
GetPrefix(ctx context.Context, prefix string, offset, limit int) (records Records[T], err error)
// GetRange gets all keys between the key from (inclusive) and to (exclusive).
// e.g. select key from kv where key >= 'a' and key < 'c';
GetRange(ctx context.Context, from, to string, offset, limit int) (records Records[T], err error)
// List gets all keys from the store, starting from the given offset and limiting the number of results to the given limit.
List(ctx context.Context, offset, limit int) (records Records[T], err error)
// Put a key into the store. If the key already exists, it will update the value if the version matches, and increment the version.
//
// If the key does not exist, it will insert the key with version 1.
//
// If the key exists but the version does not match, it will return an error.
//
// If the version is -1, it will skip the version check.
//
// If the version is 0, it will only insert the key if it does not already exist.
Put(ctx context.Context, key string, version int64, value T) (err error)
// PutAll puts multiple keys into the store, in a single transaction.
PutAll(ctx context.Context, records Records[T]) (err error)
// Delete deletes keys from the store. If the keys do not exist, no error is returned.
Delete(ctx context.Context, keys ...string) (rowsAffected int64, err error)
// DeletePrefix deletes all keys with a given prefix from the store.
DeletePrefix(ctx context.Context, prefix string, offset, limit int) (rowsAffected int64, err error)
// DeleteRange deletes all keys between the key from (inclusive) and to (exclusive).
DeleteRange(ctx context.Context, from, to string, offset, limit int) (rowsAffected int64, err error)
// Count returns the number of keys in the store.
Count(ctx context.Context) (count int64, err error)
// CountPrefix returns the number of keys in the store with a given prefix.
CountPrefix(ctx context.Context, prefix string) (count int64, err error)
// CountRange returns the number of keys in the store between the key from (inclusive) and to (exclusive).
CountRange(ctx context.Context, from, to string) (count int64, err error)
// Patch patches a key in the store. The patch is a JSON merge patch (RFC 7396), so would look something like map[string]any{"key": "value"}.
Patch(ctx context.Context, key string, version int64, patch any) (err error)
// Query runs a select query against the store, and returns the results.
Query(ctx context.Context, query string, args map[string]any) (output []Record, err error)
// Mutate runs a mutation against the store, and returns the number of rows affected.
Mutate(ctx context.Context, query string, args map[string]any) (rowsAffected int64, err error)
// MutateAll runs the mutations against the store, in the order they are provided.
//
// Use the Put, Patch, PutPatches, Delete, DeleteKeys, DeletePrefix and DeleteRange functions to populate the operations argument.
MutateAll(ctx context.Context, mutations ...db.Mutation) (rowsAffected []int64, err error)
```

## Tasks

### db-run

You don't need to run rqlite. You can use sqlite which works directly with the file system, however, if you want a distributed database, you can use rqlite. This is useful if you have a load balanced web application, or want to share data between multiple services.

interactive: true

```bash
rqlited -auth=auth.json ~/kv.db
```

### db-shell

interactive: true

```bash
rqlite --user='admin:secret'
```

### build

```bash
go build ./...
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

### docker-run-postgres

```bash
docker run -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=secret -e POSTGRES_DB=testdb -p 5432:5432 postgres:latest
```
