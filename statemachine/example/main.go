package main

import (
	"context"
	"fmt"
	"log"

	"github.com/a-h/kv/sqlitekv"
	"github.com/a-h/kv/statemachine"
	"zombiezen.com/go/sqlite/sqlitex"
)

// Counter represents a simple counter state machine.
type Counter struct {
	Count int `json:"count"`
}

func (c *Counter) Process(event statemachine.InboundEvent) ([]statemachine.OutboundEvent, error) {
	switch e := event.(type) {
	case Increment:
		oldCount := c.Count
		c.Count += e.Amount
		return []statemachine.OutboundEvent{
			CounterUpdated{
				OldCount: oldCount,
				NewCount: c.Count,
			},
		}, nil
	case Decrement:
		oldCount := c.Count
		c.Count -= e.Amount
		return []statemachine.OutboundEvent{
			CounterUpdated{
				OldCount: oldCount,
				NewCount: c.Count,
			},
		}, nil
	}
	return nil, nil
}

// Increment is a command to increase the counter.
type Increment struct {
	Amount int `json:"amount"`
}

func (Increment) EventName() string { return "Increment" }
func (Increment) IsInbound()        {}

// Decrement is a command to decrease the counter.
type Decrement struct {
	Amount int `json:"amount"`
}

func (Decrement) EventName() string { return "Decrement" }
func (Decrement) IsInbound()        {}

// CounterUpdated is an event that indicates the counter has changed.
type CounterUpdated struct {
	OldCount int `json:"oldCount"`
	NewCount int `json:"newCount"`
}

func (CounterUpdated) EventName() string { return "CounterUpdated" }
func (CounterUpdated) IsOutbound()       {}

func main() {
	pool, err := sqlitex.NewPool("file::memory:?mode=memory&cache=shared", sqlitex.PoolOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	store := sqlitekv.New(pool)
	err = store.Init(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	// Create a new counter machine.
	counter := &Counter{Count: 0}
	processor := statemachine.New(store, "counter-1", counter)

	fmt.Println("Initial count:", counter.Count)

	// Process the initial command. This will update the counter's state and store
	// the results.
	err = processor.Process(context.Background(), Increment{Amount: 5})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("After increment:", counter.Count)

	// Process the next command.
	err = processor.Process(context.Background(), Decrement{Amount: 2})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("After decrement:", counter.Count)

	// Since the state is stored in the database, we can load it back.
	loadedCounter := &Counter{}
	loadedProcessor, err := statemachine.Load(context.Background(), store, "counter-1", loadedCounter)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Loaded count:", loadedCounter.Count)
	fmt.Println("Version:", loadedProcessor.Version)

	ctx := context.Background()

	// State can be loaded directly from the store.
	_, ok, err := store.Get(ctx, "counter-1/_state", &counter)
	if err != nil {
		log.Fatal(err)
	}
	if !ok {
		fmt.Printf("State record not found\n")
		return
	}
	fmt.Printf("Stored state: %+v\n", counter)

	// The commands / inbound events, and outbound events can be loaded from the store.
	inboundRecords, err := store.GetPrefix(ctx, "counter-1/events/", 0, 10)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found %d event records\n", len(inboundRecords))
}
