package main

type LockCommand struct {
	Acquire LockAcquireCommand `cmd:"acquire" help:"Acquire a lock."`
	Release LockReleaseCommand `cmd:"release" help:"Release a lock."`
	Status  LockStatusCommand  `cmd:"status" help:"Show the status of a lock."`
}
