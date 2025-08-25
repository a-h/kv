# State Machine

This package provides an event sourcing processor that builds on top of the kv Store interface. It offers similar functionality to a-h/stream but uses SQLite, RQLite, or PostgreSQL instead of DynamoDB.

## Overview

The processor implements an event sourced state machine pattern. State represents your entity such as a user account, shopping cart, or game state. InboundEvents are commands that modify the state. OutboundEvents are events published when state changes occur. The processor ensures atomicity and provides optimistic concurrency control.

## Basic Usage

### Define Your State

```go
type Counter struct {
    Count int `json:"count"`
}

func (c *Counter) Process(event statemachine.InboundEvent) ([]statemachine.OutboundEvent, error) {
    switch e := event.(type) {
    case Increment:
        oldCount := c.Count
        c.Count += e.Amount
        return []statemachine.OutboundEvent{
            CounterUpdated{OldCount: oldCount, NewCount: c.Count},
        }, nil
    }
    return nil, nil
}
```

### Define Your Events

```go
// Inbound events represent commands.
type Increment struct {
    Amount int `json:"amount"`
}

func (Increment) EventName() string { return "Increment" }
func (Increment) IsInbound()        {}

// Outbound events represent domain events.
type CounterUpdated struct {
    OldCount int `json:"oldCount"`
    NewCount int `json:"newCount"`
}

func (CounterUpdated) EventName() string { return "CounterUpdated" }
func (CounterUpdated) IsOutbound()       {}
```

### Create and Use the Processor

```go
// Set up store using SQLite.
pool, err := sqlitex.NewPool("database.db", sqlitex.PoolOptions{})
if err != nil {
    log.Fatal(err)
}
defer pool.Close()

store := sqlitekv.New(pool)
err = store.Init(context.Background())
if err != nil {
    log.Fatal(err)
}

// Create processor.
counter := &Counter{Count: 0}
processor, err := statemachine.New(store, "counter-1", counter)
if err != nil {
    log.Fatal(err)
}

// Process events.
err = processor.Process(context.Background(), Increment{Amount: 5})
if err != nil {
    log.Fatal(err)
}

fmt.Println("Count:", counter.Count) // Output: Count: 5
```

### Load Existing State

```go
// Load from storage.
loadedCounter := &Counter{}
loadedProcessor, err := statemachine.Load(context.Background(), store, "counter-1", loadedCounter)
if err != nil {
    log.Fatal(err)
}

fmt.Println("Loaded count:", loadedCounter.Count)
fmt.Println("Version:", loadedProcessor.Version)
```

## Key Features

### Optimistic Concurrency Control

The processor uses version numbers to prevent concurrent modifications.

```go
err := processor.Process(ctx, SomeEvent{})
if errors.Is(err, statemachine.ErrOptimisticConcurrency) {
    // Another process modified the state, reload and retry.
    processor, err = statemachine.Load(ctx, store, id, state)
    // Retry logic goes here.
}
```

### Event Auditing

All inbound and outbound events are stored for auditing purposes. Events are stored with keys that include the processor ID, version, and index.

### Multiple Events Per Operation

You can process multiple events atomically.

```go
err := processor.Process(ctx, 
    Event1{Data: "foo"},
    Event2{Data: "bar"},
    Event3{Data: "baz"},
)
```

## Examples

See the example directory for complete working examples:

- Counter: Simple increment and decrement counter
- Slot Machine: Game state machine with win and lose logic  
- Batch Processor: Accumulates inputs and emits batch events

## Comparison with a-h/stream

This processor provides similar functionality to a-h/stream but with key differences:

- Uses SQLite, PostgreSQL, or RQLite instead of DynamoDB
- Builds on the existing kv.Store interface without additional layers
- Simpler implementation with fewer abstractions
- Works with any backend supported by the kv package

The core concepts of State, InboundEvent, OutboundEvent, and Processor remain the same. This makes it easy to migrate from a-h/stream if needed.

## Storage Layout

The processor stores data using these key patterns:

```go
{id}/_state                          # Current state with version
{id}/events/{version}/inbound/{idx}  # Inbound events with index
{id}/events/{version}/outbound/{idx} # Outbound events with index
```

This provides a complete audit trail while keeping the current state easily accessible.
