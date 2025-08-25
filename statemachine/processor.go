package statemachine

import (
	"context"
	"errors"
	"fmt"

	"github.com/a-h/kv"
)

// State represents the entity state that processes events.
type State interface {
	Process(event InboundEvent) (outbound []OutboundEvent, err error)
}

// InboundEvent represents events received from external systems.
type InboundEvent interface {
	EventName() string
	IsInbound()
}

// OutboundEvent represents events to be sent to external systems.
type OutboundEvent interface {
	EventName() string
	IsOutbound()
}

// Processor handles event-sourced state processing.
type Processor[T State] struct {
	store   kv.Store
	id      string
	state   T
	Version int
}

// ErrOptimisticConcurrency is returned when a state update conflicts with another update.
var ErrOptimisticConcurrency = errors.New("optimistic concurrency control: state was modified by another process")

// New creates a new processor with the provided initial state.
func New[T State](store kv.Store, id string, state T) *Processor[T] {
	return &Processor[T]{
		store:   store,
		id:      id,
		state:   state,
		Version: 0,
	}
}

// Load state from the store.
func Load[T State](ctx context.Context, store kv.Store, id string, state T) (*Processor[T], error) {
	stateKey := fmt.Sprintf("%s/_state", id)
	record, ok, err := store.Get(ctx, stateKey, state)
	if err != nil {
		return nil, fmt.Errorf("failed to load state: %w", err)
	}

	var version int
	if ok {
		version = record.Version
	}

	return &Processor[T]{
		store:   store,
		id:      id,
		state:   state,
		Version: version,
	}, nil
}

// Process handles inbound events, updating state and generating outbound events.
func (p *Processor[T]) Process(ctx context.Context, events ...InboundEvent) error {
	var outboundEvents []OutboundEvent
	for _, event := range events {
		outbound, err := p.state.Process(event)
		if err != nil {
			return fmt.Errorf("failed to process event %s: %w", event.EventName(), err)
		}
		outboundEvents = append(outboundEvents, outbound...)
	}

	var mutations []kv.Mutation

	stateKey := fmt.Sprintf("%s/_state", p.id)
	mutations = append(mutations, kv.Put(stateKey, p.Version, p.state))

	for i, event := range events {
		eventKey := fmt.Sprintf("%s/events/%d/inbound/%d", p.id, p.Version+1, i)
		mutations = append(mutations, kv.Put(eventKey, 0, event))
	}

	for i, event := range outboundEvents {
		eventKey := fmt.Sprintf("%s/events/%d/outbound/%d", p.id, p.Version+1, i)

		mutations = append(mutations, kv.Put(eventKey, 0, event))
	}

	_, err := p.store.MutateAll(ctx, mutations...)
	if err != nil {
		if errors.Is(err, kv.ErrVersionMismatch) {
			return ErrOptimisticConcurrency
		}
		return fmt.Errorf("failed to execute mutations: %w", err)
	}

	p.Version++
	return nil
}
