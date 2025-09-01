package kv

import (
	"context"
	"iter"
)

// Paginator provides paginated iteration over large result sets.
type Paginator struct {
	Store Store
	Limit int
}

// NewPaginator creates a new paginator with the given store and batch limit.
func NewPaginator(store Store, limit int) *Paginator {
	if limit <= 0 {
		limit = 1000
	}
	return &Paginator{
		Store: store,
		Limit: limit,
	}
}

// GetPrefix returns an iterator that streams records with the given prefix.
// It automatically handles pagination to avoid memory issues with large result sets.
func (p *Paginator) GetPrefix(ctx context.Context, prefix string) iter.Seq2[Record, error] {
	return func(yield func(Record, error) bool) {
		var offset int
		for {
			if err := ctx.Err(); err != nil {
				yield(Record{}, err)
				return
			}

			records, err := p.Store.GetPrefix(ctx, prefix, offset, p.Limit)
			if err != nil {
				yield(Record{}, err)
				return
			}

			if len(records) == 0 {
				return
			}

			for _, record := range records {
				if !yield(record, nil) {
					return
				}
			}

			if len(records) < p.Limit {
				return
			}

			offset += len(records)
		}
	}
}

// GetRange returns an iterator that streams records within the given range.
// It automatically handles pagination to avoid memory issues with large result sets.
func (p *Paginator) GetRange(ctx context.Context, from, to string) iter.Seq2[Record, error] {
	return func(yield func(Record, error) bool) {
		var offset int
		for {
			if err := ctx.Err(); err != nil {
				yield(Record{}, err)
				return
			}

			records, err := p.Store.GetRange(ctx, from, to, offset, p.Limit)
			if err != nil {
				yield(Record{}, err)
				return
			}

			if len(records) == 0 {
				return
			}

			for _, record := range records {
				if !yield(record, nil) {
					return
				}
			}

			if len(records) < p.Limit {
				return
			}

			offset += len(records)
		}
	}
}

// List returns an iterator that streams all records.
// It automatically handles pagination to avoid memory issues with large result sets.
func (p *Paginator) List(ctx context.Context) iter.Seq2[Record, error] {
	return func(yield func(Record, error) bool) {
		var offset int
		for {
			if err := ctx.Err(); err != nil {
				yield(Record{}, err)
				return
			}

			records, err := p.Store.List(ctx, offset, p.Limit)
			if err != nil {
				yield(Record{}, err)
				return
			}

			if len(records) == 0 {
				return
			}

			for _, record := range records {
				if !yield(record, nil) {
					return
				}
			}

			if len(records) < p.Limit {
				return
			}

			offset += len(records)
		}
	}
}

// GetType returns an iterator that streams records of the given type.
// It automatically handles pagination to avoid memory issues with large result sets.
func (p *Paginator) GetType(ctx context.Context, t Type) iter.Seq2[Record, error] {
	return func(yield func(Record, error) bool) {
		var offset int
		for {
			if err := ctx.Err(); err != nil {
				yield(Record{}, err)
				return
			}

			records, err := p.Store.GetType(ctx, t, offset, p.Limit)
			if err != nil {
				yield(Record{}, err)
				return
			}

			if len(records) == 0 {
				return
			}

			for _, record := range records {
				if !yield(record, nil) {
					return
				}
			}

			if len(records) < p.Limit {
				return
			}

			offset += len(records)
		}
	}
}
