# NATS Pusher CLI

A standalone command-line tool that consumes records from a KV store stream and publishes them to NATS.

## Installation

From the repository root:

```bash
go install ./natspusher/cmd/natspusher
```

Or build locally:

```bash
cd natspusher/cmd/natspusher
go build -o natspusher .
```

## Setting Up NATS

### Using Project Docker Compose

The project includes a `compose.yaml` file at the repository root that sets up NATS along with other services:

```bash
docker compose up nats

docker compose up

docker compose up -d nats
```

The compose setup provides:

- NATS server with JetStream enabled on port 4222
- HTTP management interface on port 8222
- Persistent data storage

### Alternative: Manual Setup

If you prefer to run NATS separately:

```bash
docker run -p 4222:4222 -p 8222:8222 nats:latest -js -m 8222
```

### Subscribing to Messages

To view the messages being pushed by natspusher, you can use the NATS CLI:

```bash
# Install NATS CLI.
go install github.com/nats-io/natscli/nats@latest

# Subscribe to all messages with the default subject prefix.
nats sub "kv.>"

# Subscribe to a specific stream.
nats sub "kv.user-events.>"

# Subscribe with JSON output for better readability.
nats sub "kv.user-events.>" --translate

# View message headers as well.
nats sub "kv.user-events.>" -H
```

### Example Message Flow

When natspusher processes a record, it publishes to subjects like:

```text
kv.user-events.UserEvent.put
kv.orders.Order.delete
kv.analytics.PageView.put
```

You can subscribe to specific patterns:

```bash
# All user events.
nats sub "kv.user-events.*"

# All PUT operations across all streams.
nats sub "kv.*.*.put"

# Everything from a specific environment (if using headers).
nats sub "kv.>" --filter-header "environment:production"
```

### NATS Monitoring

NATS provides a monitoring interface at `http://localhost:8222` when started with the `-m 8222` flag. This shows:

- Server statistics
- Connection information
- Subject subscription details
- Message rates

## Usage

### Basic Usage

```bash
natspusher --stream-name "user-events" --nats-url "nats://localhost:4222"
```

### Full Configuration

```bash
natspusher \
  --store-type sqlite \
  --store-connection "file:data.db?mode=rwc" \
  --stream-name "user-events" \
  --consumer-name "natspusher" \
  --record-type "" \
  --batch-size 10 \
  --batch-timeout "5m" \
  --nats-url "nats://localhost:4222" \
  --subject-prefix "kv" \
  --max-retries 3 \
  --header-source "kv-store" \
  --header-environment "production" \
  --log-level info \
  --log-format text
```

## Configuration Options

### Store Configuration

- `--store-type`: Type of KV store (sqlite, rqlite, postgres) [default: sqlite]
- `--store-connection`: Connection string for the KV store [default: file:data.db?mode=rwc]

### Stream Configuration

- `--stream-name`: Name of the KV stream to consume from (required)
- `--consumer-name`: Name of the consumer for tracking position [default: natspusher]
- `--record-type`: Type filter for records (empty for all types) [default: ""]
- `--batch-size`: Number of records to process in each batch [default: 10]
- `--batch-timeout`: Maximum time to wait for a batch [default: 5m]

### NATS Configuration

- `--nats-url`: NATS server URL [default: nats://localhost:4222]
- `--nats-creds-file`: NATS credentials file path (env: NATS_CREDS)
- `--nats-token`: NATS authentication token (env: NATS_TOKEN)
- `--nats-user`: NATS username (env: NATS_USER)
- `--nats-password`: NATS password (env: NATS_PASS)
- `--subject-prefix`: NATS subject prefix for published messages [default: kv]
- `--max-retries`: Maximum number of retries for failed publishes [default: 3]

### Headers

- `--header-source`: Source header to add to NATS messages [default: kv-store]
- `--header-environment`: Environment header to add to NATS messages [default: development]

### Logging

- `--log-level`: Log level (debug, info, warn, error) [default: info]
- `--log-format`: Log format (json, text) [default: text]

## Environment Variables

Authentication can be configured using environment variables:

```bash
export NATS_CREDS="/path/to/nats.creds"
# or
export NATS_TOKEN="your-token"
# or
export NATS_USER="username"
export NATS_PASS="password"
```

## Examples

### SQLite to NATS

```bash
natspusher \
  --store-type sqlite \
  --store-connection "file:events.db?mode=rwc" \
  --stream-name "user-events" \
  --nats-url "nats://localhost:4222"
```

### PostgreSQL to NATS with Authentication

```bash
natspusher \
  --store-type postgres \
  --store-connection "postgres://user:pass@localhost/dbname" \
  --stream-name "orders" \
  --consumer-name "order-processor" \
  --record-type "Order" \
  --nats-url "nats://nats.example.com:4222" \
  --nats-creds-file "/etc/nats/nats.creds" \
  --subject-prefix "orders" \
  --header-environment "production"
```

### RQLite Cluster

```bash
natspusher \
  --store-type rqlite \
  --store-connection "http://leader.rqlite.cluster:4001?user=admin&password=secret" \
  --stream-name "analytics" \
  --nats-url "nats://nats1.example.com:4222,nats://nats2.example.com:4222"
```

## NATS Subject Mapping

Records are published to NATS subjects based on the following pattern:

```text
{subject-prefix}.{stream-name}.{record-type}.{action}
```

For example:

- `kv.user-events.UserEvent.put`
- `kv.user-events.UserEvent.delete`
- `kv.orders.Order.put`

## Message Headers

Each NATS message includes the following headers:

- `source`: Configured source identifier (default: "kv-store")
- `environment`: Configured environment (default: "development")

## Complete Example

Here's a complete example showing how to set up everything from scratch:

### 1. Start NATS Server

```bash
# Terminal 1: From the repository root, start NATS using compose.
docker compose up nats

# Or start all services in background.
docker compose up -d
```

### 2. Set Up NATS Subscription

```bash
# Terminal 2: Subscribe to all messages from the KV pusher.
nats sub "kv.>" -H

# Or for a specific stream.
nats sub "kv.user-events.>" --translate
```

### 3. Create Test Data

```bash
# Terminal 3: Create some test data in the KV store.
cd /path/to/your/kv-project

# Use the kv CLI to add some data to a stream.
echo '{"userId":"user123","action":"login","timestamp":"2024-01-01T10:00:00Z"}' | \
  kv put user-events/user123/login
  
echo '{"userId":"user456","action":"signup","timestamp":"2024-01-01T10:05:00Z"}' | \
  kv put user-events/user456/signup
```

### 4. Run NATS Pusher

```bash
# Terminal 4: Start the natspusher.
natspusher \
  --stream-name "user-events" \
  --log-level debug \
  --log-format text
```

### 5. Observe the Flow

You should see:

- **Terminal 2**: NATS messages appearing with your data
- **Terminal 4**: Debug logs showing records being processed and published
- **Browser**: Visit `http://localhost:8222` to see NATS monitoring dashboard

### Expected Output

In the NATS subscriber (Terminal 2), you'll see messages like:

```text
[#1] Received on "kv.user-events.object.put"
source: kv-store
environment: development

{"userId":"user123","action":"login","timestamp":"2024-01-01T10:00:00Z"}
```

### Troubleshooting

- **No messages appearing**: Check that data exists in the KV stream with `kv list`
- **Connection errors**: Verify NATS is running and accessible
- **Permission errors**: Ensure the KV store database file is readable/writable

## Signal Handling

The application gracefully handles SIGINT and SIGTERM signals, allowing it to complete processing of the current batch before shutting down.

## Error Handling

- Connection failures to the KV store or NATS are fatal and will cause the application to exit
- Individual record processing errors are logged but do not stop the application
- Failed NATS publishes are retried according to the `--max-retries` configuration
