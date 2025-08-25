package statemachine_test

import (
	"context"
	"errors"
	"math/rand"
	"strings"
	"testing"

	"github.com/a-h/kv"
	"github.com/a-h/kv/sqlitekv"
	"github.com/a-h/kv/statemachine"
	"zombiezen.com/go/sqlite/sqlitex"
)

func NewSlotMachine(id string) *SlotMachine {
	return &SlotMachine{
		ID:        id,
		Balance:   0,
		Payout:    4,
		WinChance: 0.18,
	}
}

func Win(chance float64) bool {
	return rand.Float64() <= chance
}

var ErrCannotInsertCoin = errors.New("cannot insert coin")
var ErrCannotPullHandle = errors.New("cannot pull handle")

type SlotMachine struct {
	ID           string  `json:"id"`
	Balance      int     `json:"balance"`
	Payout       int     `json:"payout"`
	WinChance    float64 `json:"winChance"`
	Games        int     `json:"games"`
	Wins         int     `json:"wins"`
	Losses       int     `json:"losses"`
	IsCoinInSlot bool    `json:"isCoinInSlot"`
}

func (s *SlotMachine) Process(event statemachine.InboundEvent) (outbound []statemachine.OutboundEvent, err error) {
	switch e := event.(type) {
	case InsertCoin:
		return s.InsertCoin()
	case PullHandle:
		return s.PullHandle(e)
	}
	return
}

func (s *SlotMachine) InsertCoin() (outbound []statemachine.OutboundEvent, err error) {
	if s.IsCoinInSlot {
		return nil, ErrCannotInsertCoin
	}
	s.IsCoinInSlot = true
	return
}

func (s *SlotMachine) PullHandle(e PullHandle) (outbound []statemachine.OutboundEvent, err error) {
	if !s.IsCoinInSlot {
		return nil, ErrCannotPullHandle
	}

	// Take the coin.
	s.IsCoinInSlot = false

	// Update the stats.
	won := Win(s.WinChance)
	s.Games++
	if won {
		s.Wins++
		s.Balance -= (s.Payout - 1)
	} else {
		s.Losses++
		s.Balance++
	}

	// Send events.
	outbound = append(outbound, GamePlayed{
		MachineID: s.ID,
		Won:       won,
	})
	if won {
		outbound = append(outbound, PayoutMade{
			UserID: e.UserID,
			Amount: s.Payout,
		})
	}
	return
}

// InsertCoin represents a command to insert a coin into the slot machine.
type InsertCoin struct{}

func (InsertCoin) EventName() string { return "InsertCoin" }
func (InsertCoin) IsInbound()        {}

// PullHandle represents a command to pull the slot machine handle.
type PullHandle struct {
	UserID string `json:"userId"`
}

func (PullHandle) EventName() string { return "PullHandle" }
func (PullHandle) IsInbound()        {}

// GamePlayed represents an event indicating a game was played on the slot machine.
type GamePlayed struct {
	MachineID string `json:"machineId"`
	Won       bool   `json:"won"`
}

func (GamePlayed) EventName() string { return "GamePlayed" }
func (GamePlayed) IsOutbound()       {}

// PayoutMade represents an event indicating a payout was made to a user.
type PayoutMade struct {
	UserID string `json:"userId"`
	Amount int    `json:"amount"`
}

func (PayoutMade) EventName() string { return "PayoutMade" }
func (PayoutMade) IsOutbound()       {}

// TestSlotMachineProcessor tests the slot machine processor implementation.
func TestSlotMachineProcessor(t *testing.T) {
	// Create an in-memory SQLite database.
	pool, err := sqlitex.NewPool("file::memory:?mode=memory&cache=shared", sqlitex.PoolOptions{})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer pool.Close()

	store := sqlitekv.New(pool)
	err = store.Init(context.Background())
	if err != nil {
		t.Fatalf("failed to initialize store: %v", err)
	}

	// Create new slot machine.
	machine := NewSlotMachine("machine-1")
	machine.WinChance = 1.0 // Always win for testing.
	processor := statemachine.New(store, "machine-1", machine)

	// Process events.
	err = processor.Process(context.Background(), InsertCoin{})
	if err != nil {
		t.Fatal(err)
	}

	// Verify coin was inserted.
	if !machine.IsCoinInSlot {
		t.Error("expected coin to be in slot")
	}

	err = processor.Process(context.Background(), PullHandle{UserID: "user-123"})
	if err != nil {
		t.Fatal(err)
	}

	// Verify game state.
	if machine.Games != 1 {
		t.Errorf("expected 1 game, got %d", machine.Games)
	}
	if machine.Wins != 1 {
		t.Errorf("expected 1 win, got %d", machine.Wins)
	}
	if machine.IsCoinInSlot {
		t.Error("expected coin to be taken")
	}

	// Load from storage.
	loadedMachine := NewSlotMachine("machine-1")
	loadedProcessor, err := statemachine.Load(context.Background(), store, "machine-1", loadedMachine)
	if err != nil {
		t.Fatal(err)
	}

	// Verify state was loaded correctly.
	if loadedMachine.Games != 1 {
		t.Errorf("loaded machine: expected 1 game, got %d", loadedMachine.Games)
	}
	if loadedProcessor.Version != 2 {
		t.Errorf("expected version 2, got %d", loadedProcessor.Version)
	}
}

// BatchState represents a state machine that processes inputs in batches.
type BatchState struct {
	BatchSize      int   `json:"batchSize"`
	BatchesEmitted int   `json:"batchesEmitted"`
	Values         []int `json:"values"`
}

func NewBatchState() *BatchState {
	return &BatchState{
		BatchSize: 2,
	}
}

func (s *BatchState) Process(event statemachine.InboundEvent) (outbound []statemachine.OutboundEvent, err error) {
	switch e := event.(type) {
	case BatchInput:
		s.Values = append(s.Values, e.Number)
		if len(s.Values) >= s.BatchSize {
			outbound = append(outbound, BatchOutput{Numbers: s.Values})
			s.BatchesEmitted++
			s.Values = nil
		}
	}
	return
}

// BatchInput represents an input value to be processed in batches.
type BatchInput struct {
	Number int `json:"number"`
}

func (bi BatchInput) EventName() string { return "BatchInput" }
func (bi BatchInput) IsInbound()        {}

// BatchOutput represents a batch of processed values.
type BatchOutput struct {
	Numbers []int `json:"numbers"`
}

func (bo BatchOutput) EventName() string { return "BatchOutput" }
func (bo BatchOutput) IsOutbound()       {}

// TestBatchProcessor tests the batch processor implementation.
func TestBatchProcessor(t *testing.T) {
	// Create an in-memory SQLite database.
	pool, err := sqlitex.NewPool("file::memory:?mode=memory&cache=shared", sqlitex.PoolOptions{})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer pool.Close()

	store := sqlitekv.New(pool)
	err = store.Init(context.Background())
	if err != nil {
		t.Fatalf("failed to initialize store: %v", err)
	}

	// Create new batch state.
	batch := NewBatchState()
	processor := statemachine.New(store, "batch-1", batch)

	// Process multiple inputs.
	err = processor.Process(context.Background(),
		BatchInput{Number: 1},
		BatchInput{Number: 2},
		BatchInput{Number: 3},
		BatchInput{Number: 4},
	)
	if err != nil {
		t.Fatal(err)
	}

	// Verify 2 batch outputs were emitted: [1,2] and [3,4].
	if batch.BatchesEmitted != 2 {
		t.Errorf("expected 2 batches emitted, got %d", batch.BatchesEmitted)
	}
	// The state should have Values = [] (empty after emitting batches).
	if len(batch.Values) != 0 {
		t.Errorf("expected empty values, got %v", batch.Values)
	}

	// Load from storage and verify persistence.
	loadedBatch := NewBatchState()
	loadedProcessor, err := statemachine.Load(context.Background(), store, "batch-1", loadedBatch)
	if err != nil {
		t.Fatal(err)
	}

	if loadedBatch.BatchesEmitted != 2 {
		t.Errorf("loaded batch: expected 2 batches emitted, got %d", loadedBatch.BatchesEmitted)
	}
	if loadedProcessor.Version != 1 {
		t.Errorf("expected version 1, got %d", loadedProcessor.Version)
	}
}

// TestEventStorage tests that events are stored with consistent key structure.
func TestEventStorage(t *testing.T) {
	// Create an in-memory SQLite database.
	pool, err := sqlitex.NewPool("file::memory:?mode=memory&cache=shared", sqlitex.PoolOptions{})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer pool.Close()

	store := sqlitekv.New(pool)
	err = store.Init(context.Background())
	if err != nil {
		t.Fatalf("failed to initialize store: %v", err)
	}

	// Create and use a slot machine with proper initialization.
	machine := NewSlotMachine("machine-2")
	machine.WinChance = 1.0 // Always win for testing.

	proc := statemachine.New(store, "machine-2", machine)

	// Process some events.
	err = proc.Process(context.Background(), InsertCoin{})
	if err != nil {
		t.Fatalf("failed to process InsertCoin: %v", err)
	}

	err = proc.Process(context.Background(), PullHandle{UserID: "user-456"})
	if err != nil {
		t.Fatalf("failed to process PullHandle: %v", err)
	}

	// Query the events.
	eventsPrefix := "machine-2/events/"
	allEventRecords, err := store.GetPrefix(context.Background(), eventsPrefix, 0, -1)
	if err != nil {
		t.Fatalf("failed to query events: %v", err)
	}

	// Separate inbound and outbound events.
	var inboundRecords []kv.Record
	var outboundRecords []kv.Record
	for _, record := range allEventRecords {
		if strings.Contains(record.Key, "/inbound") {
			inboundRecords = append(inboundRecords, record)
		} else if strings.Contains(record.Key, "/outbound/") {
			outboundRecords = append(outboundRecords, record)
		}
	}

	t.Run("correct number of events are stored", func(t *testing.T) {
		// Verify we have the expected number of events.
		if len(inboundRecords) != 2 {
			t.Errorf("expected 2 inbound events, got %d", len(inboundRecords))
		}

		if len(outboundRecords) != 2 {
			t.Errorf("expected 2 outbound events, got %d", len(outboundRecords))
		}
	})

	t.Run("inbound keys follow expected pattern", func(t *testing.T) {
		expectedInboundKeys := []string{
			"machine-2/events/1/inbound/0",
			"machine-2/events/2/inbound/0",
		}

		for i, record := range inboundRecords {
			if record.Key != expectedInboundKeys[i] {
				t.Errorf("expected inbound key %s, got %s", expectedInboundKeys[i], record.Key)
			}
		}
	})

	t.Run("outbound keys follow expected pattern", func(t *testing.T) {
		// InsertCoin generates no outbound events, PullHandle generates GamePlayed and PayoutMade.
		expectedOutboundKeys := []string{
			"machine-2/events/2/outbound/0", // GamePlayed from PullHandle
			"machine-2/events/2/outbound/1", // PayoutMade from PullHandle (since WinChance = 1.0)
		}

		for i, record := range outboundRecords {
			if i < len(expectedOutboundKeys) && record.Key != expectedOutboundKeys[i] {
				t.Errorf("expected outbound key %s, got %s", expectedOutboundKeys[i], record.Key)
			}
		}
	})
}
