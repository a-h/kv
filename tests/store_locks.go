package tests

import (
	"context"
	"testing"
	"time"

	"github.com/a-h/kv"
)

func newLockTest(ctx context.Context, store kv.Store) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("Locks can be acquired", func(t *testing.T) {
			t.Parallel()
			lockName := "queue_processor_acquire"
			acquired, err := store.LockAcquire(ctx, lockName, "server1", 2*time.Second)
			if err != nil {
				t.Fatalf("Unexpected error acquiring lock: %v.", err)
			}
			if !acquired {
				t.Error("Expected lock to be acquired.")
			}
		})
		t.Run("Once locked, another locker cannot acquire", func(t *testing.T) {
			t.Parallel()
			lockName := "queue_processor_conflict"
			acquired, err := store.LockAcquire(ctx, lockName, "server1", 2*time.Second)
			if err != nil || !acquired {
				t.Fatalf("Server1 failed to acquire: %v.", err)
			}
			acquired, err = store.LockAcquire(ctx, lockName, "server2", 2*time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v.", err)
			}
			if acquired {
				t.Error("Expected lock not to be acquired by server2.")
			}
		})
		t.Run("Once locked, the same locker can refresh", func(t *testing.T) {
			t.Parallel()
			lockName := "queue_processor_refresh"
			acquired, err := store.LockAcquire(ctx, lockName, "server1", 1*time.Second)
			if err != nil || !acquired {
				t.Fatalf("Server1 failed to acquire: %v.", err)
			}
			acquired, err = store.LockAcquire(ctx, lockName, "server1", 2*time.Second)
			if err != nil {
				t.Fatalf("Unexpected error refreshing lock: %v.", err)
			}
			if !acquired {
				t.Error("Expected lock to be refreshed by the same locker.")
			}
		})
		t.Run("Once unlocked, another locker can acquire", func(t *testing.T) {
			t.Parallel()
			lockName := "queue_processor_release"
			acquired, err := store.LockAcquire(ctx, lockName, "server1", 2*time.Second)
			if err != nil || !acquired {
				t.Fatalf("Server1 failed to acquire: %v.", err)
			}
			err = store.LockRelease(ctx, lockName, "server1")
			if err != nil {
				t.Fatalf("Unexpected error releasing: %v.", err)
			}
			acquired, err = store.LockAcquire(ctx, lockName, "server2", 2*time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v.", err)
			}
			if !acquired {
				t.Error("Expected lock to be acquired by server2 after release.")
			}
		})
		t.Run("Once expired, another locker can acquire", func(t *testing.T) {
			t.Parallel()
			lockName := "queue_processor_expiry"
			acquired, err := store.LockAcquire(ctx, lockName, "server1", 100*time.Millisecond)
			if err != nil || !acquired {
				t.Fatalf("Server1 failed to acquire: %v.", err)
			}
			time.Sleep(200 * time.Millisecond)
			acquired, err = store.LockAcquire(ctx, lockName, "server2", 2*time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v.", err)
			}
			if !acquired {
				t.Error("Expected lock to be acquired by server2 after expiry.")
			}
		})
		t.Run("Release by non-owner returns no-op", func(t *testing.T) {
			t.Parallel()
			lockName := "queue_processor_nonowner"
			acquired, err := store.LockAcquire(ctx, lockName, "server1", 2*time.Second)
			if err != nil || !acquired {
				t.Fatalf("Server1 failed to acquire: %v.", err)
			}
			err = store.LockRelease(ctx, lockName, "server2")
			if err != nil {
				t.Fatalf("Unexpected error releasing by non-owner: %v.", err)
			}
		})
		t.Run("LockStatus returns ok=false if lock does not exist", func(t *testing.T) {
			t.Parallel()
			lockName := "lockstatus_not_exist"
			status, ok, err := store.LockStatus(ctx, lockName)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if ok {
				t.Errorf("Expected ok=false for non-existent lock, got ok=true, status=%+v", status)
			}
		})
		t.Run("LockStatus returns ok=true and correct status if lock exists", func(t *testing.T) {
			t.Parallel()
			lockName := "lockstatus_exists"
			lockedBy := "server1"
			duration := 2 * time.Second
			acquired, err := store.LockAcquire(ctx, lockName, lockedBy, duration)
			if err != nil || !acquired {
				t.Fatalf("Failed to acquire lock: %v", err)
			}
			status, ok, err := store.LockStatus(ctx, lockName)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !ok {
				t.Fatalf("Expected ok=true for existing lock, got ok=false")
			}
			if status.Name != lockName {
				t.Errorf("Expected Name=%q, got %q", lockName, status.Name)
			}
			if status.LockedBy != lockedBy {
				t.Errorf("Expected LockedBy=%q, got %q", lockedBy, status.LockedBy)
			}
			if time.Since(status.LockedAt) > 5*time.Second {
				t.Errorf("LockedAt too far in the past: %v", status.LockedAt)
			}
			if status.ExpiresAt.Sub(status.LockedAt) < duration {
				t.Errorf("ExpiresAt not at least duration after LockedAt: got %v, want at least %v", status.ExpiresAt.Sub(status.LockedAt), duration)
			}
		})
	}
}
