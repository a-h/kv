package main

import (
	"context"
	"fmt"
)

type StreamSeqCommand struct{}

func (c *StreamSeqCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return err
	}
	seq, err := store.StreamSeq(ctx)
	if err != nil {
		return err
	}
	fmt.Println(seq)
	return nil
}
