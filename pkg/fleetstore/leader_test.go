//go:build fleetstore

package fleetstore

import (
	"context"
	"testing"
	"time"
)

func TestLeaderAcquire(t *testing.T) {
	pool := setupPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	le := NewLeaderElector(pool, LeaderConfig{
		TTL:       5 * time.Second,
		Heartbeat: 100 * time.Millisecond,
		Identity:  "pod-1",
	}, testLogger())

	if err := le.Acquire(ctx); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	// Verify we are the holder.
	var holder string
	err := pool.QueryRow(ctx, "SELECT holder FROM leader WHERE singleton = true").Scan(&holder)
	if err != nil {
		t.Fatalf("query holder: %v", err)
	}
	if holder != "pod-1" {
		t.Errorf("holder = %q, want pod-1", holder)
	}
}

func TestLeaderContention(t *testing.T) {
	pool := setupPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	le1 := NewLeaderElector(pool, LeaderConfig{
		TTL:       2 * time.Second,
		Heartbeat: 100 * time.Millisecond,
		Identity:  "pod-1",
	}, testLogger())

	le2 := NewLeaderElector(pool, LeaderConfig{
		TTL:       2 * time.Second,
		Heartbeat: 100 * time.Millisecond,
		Identity:  "pod-2",
	}, testLogger())

	if err := le1.Acquire(ctx); err != nil {
		t.Fatalf("le1 Acquire: %v", err)
	}

	// pod-2 should not acquire while pod-1 holds and lease is not expired.
	ok, err := le2.tryAcquire(ctx)
	if err != nil {
		t.Fatalf("le2 tryAcquire: %v", err)
	}
	if ok {
		t.Error("pod-2 acquired while pod-1 holds an unexpired lease")
	}
}

func TestLeaderExpiredTakeover(t *testing.T) {
	pool := setupPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	le1 := NewLeaderElector(pool, LeaderConfig{
		TTL:       1 * time.Second,
		Heartbeat: 100 * time.Millisecond,
		Identity:  "pod-1",
	}, testLogger())

	le2 := NewLeaderElector(pool, LeaderConfig{
		TTL:       5 * time.Second,
		Heartbeat: 100 * time.Millisecond,
		Identity:  "pod-2",
	}, testLogger())

	if err := le1.Acquire(ctx); err != nil {
		t.Fatalf("le1 Acquire: %v", err)
	}

	// Wait for pod-1's lease to expire.
	time.Sleep(1500 * time.Millisecond)

	ok, err := le2.tryAcquire(ctx)
	if err != nil {
		t.Fatalf("le2 tryAcquire: %v", err)
	}
	if !ok {
		t.Error("pod-2 failed to take over expired lease")
	}

	var holder string
	err = pool.QueryRow(ctx, "SELECT holder FROM leader WHERE singleton = true").Scan(&holder)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if holder != "pod-2" {
		t.Errorf("holder = %q, want pod-2", holder)
	}
}

func TestLeaderRenewal(t *testing.T) {
	pool := setupPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	le := NewLeaderElector(pool, LeaderConfig{
		TTL:       2 * time.Second,
		Heartbeat: 100 * time.Millisecond,
		Identity:  "pod-renew",
	}, testLogger())

	if err := le.Acquire(ctx); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	// Renew should succeed (same holder).
	ok, err := le.tryAcquire(ctx)
	if err != nil {
		t.Fatalf("tryAcquire (renew): %v", err)
	}
	if !ok {
		t.Error("renewal failed")
	}
}
