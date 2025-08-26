# NATS Pusher

The `natspusher` package provides a way to consume events from a KV store stream and publish them to NATS. This is useful for building event-driven architectures where you want to propagate state changes from your KV store to other systems via NATS.

## Features

- **Stream Consumer**: Consumes records from a KV store stream with automatic consumer position tracking
- **NATS Publishing**: Publishes events to NATS with configurable subjects and headers
- **Record Value Payload**: Sends the record value as NATS message payload with metadata in headers
- **Subject Mapping**: Automatically maps KV store keys to NATS subjects with configurable prefixes
- **Retry Logic**: Built-in retry logic for failed publishes with exponential backoff
- **Batch Processing**: Processes records in batches for better performance
- **Graceful Shutdown**: Handles context cancellation for clean shutdown
- **External Connection**: Uses your existing NATS connection for maximum flexibility
- **At-Least-Once Delivery**: Only commits records after successful NATS publishing to prevent data loss
- **Testable Interface**: Publisher interface allows for easy mocking and testing

## Quick Start

```go
package main

import (
    "context"
    "log"

    "github.com/a-h/kv/natspusher"
    "github.com/a-h/kv/sqlitekv"
    "github.com/nats-io/nats.go"
    "zombiezen.com/go/sqlite/sqlitex"
)

func main() {
    ctx := context.Background()

    // Set up your KV store
    pool, _ := sqlitex.NewPool("file:data.db?mode=rwc", sqlitex.PoolOptions{})
    store := sqlitekv.New(pool)
    store.Init(ctx)

    // Connect to NATS with your preferred options
    nc, _ := nats.Connect("nats://localhost:4222")
    defer nc.Close()

    // Create a stream consumer
    consumer := kv.NewStreamConsumer(ctx, store, "events", "event-publisher", kv.TypeAll)
    consumer.MinBackoff = 1 * time.Second
    consumer.MaxBackoff = 30 * time.Second

    // Configure the NATS pusher
    config := natspusher.Config{
        SubjectPrefix: "app", // optional prefix for NATS subjects
        MaxRetries:    3,     // retry failed publishes
    }

    // Create and start the pusher
    pusher, _ := natspusher.New(nc, consumer, config)

    // This blocks until context is canceled
    pusher.Run(ctx)
}
```

## Configuration

The `Config` struct provides configuration options for NATS publishing:

```go
type Config struct {
    // SubjectPrefix is the subject prefix for published messages.
    SubjectPrefix string
    // Logger is used for structured logging.
    Logger *slog.Logger
    // MaxRetries is the number of retries for failed publishes.
    MaxRetries int
    // Headers are added to NATS messages.
    Headers nats.Header
}
```

Stream consumer configuration is handled when creating the `kv.StreamConsumer`:

```go
consumer := kv.NewStreamConsumer(ctx, store, "stream-name", "consumer-name", kv.TypeAll)
consumer.MinBackoff = 1 * time.Second  // wait time when no records available
consumer.MaxBackoff = 30 * time.Second // max backoff time
```

The pusher handles record committing automatically - it only commits records after successful NATS publishing to ensure at-least-once delivery.

## Testing with Mock Publishers

The pusher uses a `Publisher` interface for NATS publishing, making it easy to test:

```go
type Publisher interface {
    PublishMsg(msg *nats.Msg) error
}

func TestApp(t *testing.T) {
    mock := &mockPublisher{}
    config := natspusher.Config{}
    pusher := &natspusher.Pusher{Publisher: mock, Config: config}

    // Test your logic.
    err := pusher.publishRecord(ctx, record)

    // Verify mock received expected messages.
    assert.Equal(t, 1, len(mock.messages))
}
```

## Subject Mapping

The pusher automatically converts KV store keys to NATS subjects using the following rules:

1. Replace `/`, `:`, and `_` characters with `.`
2. Remove consecutive dots
3. Trim leading/trailing dots
4. Prepend the configured `SubjectPrefix`
5. Append the record type

**Examples (with no prefix - default):**

- Key: `user/123/profile`, Type: `Profile` → Subject: `user.123.profile.Profile`
- Key: `session:abc123`, Type: `Session` → Subject: `session.abc123.Session`
- Key: `cache_key_test`, Type: `Cache` → Subject: `cache.key.test.Cache`

**Examples (with prefix "kv"):**

- Key: `user/123/profile`, Type: `Profile` → Subject: `kv.user.123.profile.Profile`
- Key: `session:abc123`, Type: `Session` → Subject: `kv.session.abc123.Session`

## Message Format

The record value is sent as the NATS message payload, with metadata in headers:

**Payload**: The raw record value (e.g., `{"name": "John", "age": 30}`)

**Headers**:

- `kv-seq`: Stream sequence number
- `kv-action`: Action type (create, update, delete)
- `kv-type`: Record type
- `kv-key`: Original key
- `kv-version`: Record version
- `kv-created`: Creation timestamp

## NATS Connection Management

The pusher accepts an existing NATS connection, giving you full control over connection options including:

- **Authentication**: Credentials files, tokens, nkeys, username/password
- **TLS/Security**: Custom certificates, TLS configuration
- **Connection Options**: Timeouts, reconnect policies, connection names
- **Advanced Features**: JetStream, clustering, etc.
- **Consumer Isolation**: Automatically generates unique consumer names per hostname to avoid conflicts in distributed deployments

### Example Connection Setups

#### Basic Connection

```go
nc, err := nats.Connect("nats://localhost:4222")
```

#### With Credentials

```go
nc, err := nats.Connect("nats://localhost:4222",
nats.UserCredentials("/path/to/creds.jwt"))
```

#### With Custom TLS

```go
nc, err := nats.Connect("nats://localhost:4222",
nats.ClientCert("/path/to/cert.pem", "/path/to/key.pem"),
nats.RootCAs("/path/to/ca.pem"))
```

#### With NKeys

```go
opt, _ := nats.NkeyOptionFromSeed("/path/to/seed.txt")
nc, err := nats.Connect("nats://localhost:4222", opt)
```

## Error Handling

The pusher includes robust error handling:

- **Connection Errors**: Automatic reconnection with exponential backoff
- **Publish Errors**: Configurable retry logic with max attempts
- **Stream Errors**: Graceful handling of stream read failures
- **Context Cancellation**: Clean shutdown when context is canceled

Failed record publishes are logged but don't stop the overall processing, ensuring high availability.

## Monitoring

You can monitor the pusher's progress:

```go
// Get consumer status
status, ok, err := pusher.Status(ctx)
if ok {
    log.Printf("Consumer at sequence: %d", status.Seq)
}

// Delete consumer (cleanup)
err := pusher.Delete(ctx)
```

## Example Use Cases

1. **Event Sourcing**: Publish domain events from your KV store to NATS for event sourcing patterns
2. **Microservices Integration**: Notify other services about data changes
3. **Analytics Pipeline**: Stream events to analytics systems
4. **Audit Logging**: Publish audit events to centralized logging systems
5. **Cache Invalidation**: Notify distributed caches about data changes

## Running the Example

See the `example/main.go` file for a complete working example. To run it:

1. Start NATS server: `nats-server`
2. Run the example: `go run natspusher/example/main.go`
3. Watch NATS messages: `nats sub "events.>"`

The example creates sample events in the KV store and shows them being published to NATS.
