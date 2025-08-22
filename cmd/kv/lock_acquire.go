package main

import (
	"context"
	"errors"
	"time"
)

type LockAcquireCommand struct {
	Name     string        `arg:"" help:"The lock name." required:"true"`
	LockedBy string        `arg:"" help:"The owner of the lock." required:"true"`
	Duration time.Duration `help:"The duration to acquire the lock for (e.g. 10m, 1h)." default:"10m"`
}

func (c *LockAcquireCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return err
	}
	acquired, err := store.LockAcquire(ctx, c.Name, c.LockedBy, c.Duration)
	if err != nil {
		return err
	}
	if !acquired {
		return errors.New("lock not acquired, it may already be held by another process")
	}
	return nil
}
