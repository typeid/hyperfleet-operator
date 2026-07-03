//go:build fleetstore

package fleetstore

import (
	"context"
	"testing"
	"time"

	v1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestWatcherDeliversEvents(t *testing.T) {
	pool := setupPostgres(t)
	logger := testLogger()
	c := NewDirectClient(pool, logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stores := make(map[string]*InformerStore)
	for _, kind := range RegisteredKinds() {
		stores[kind] = NewInformerStore(kind, logger)
	}

	watcher := NewWatcher(pool, WatchConfig{
		PollIdle: 100 * time.Millisecond,
	}, logger)
	for kind, store := range stores {
		watcher.RegisterStore(kind, store)
	}

	go func() {
		if err := watcher.Run(ctx); err != nil && ctx.Err() == nil {
			t.Errorf("watcher.Run: %v", err)
		}
	}()

	// Wait for initial list to complete (stores become synced).
	deadline := time.After(10 * time.Second)
	for {
		allSynced := true
		for _, s := range stores {
			if !s.HasSynced() {
				allSynced = false
				break
			}
		}
		if allSynced {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timeout waiting for cache sync")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Create a cluster — watcher should pick it up.
	cluster := newTestCluster("ns1", "watched", "111122223333")
	if err := c.Create(ctx, cluster); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Wait for the store to have the object.
	if !waitForStore(t, stores["Cluster"], "ns1/watched", 5*time.Second) {
		t.Fatal("cluster not in store after create")
	}

	obj, ok := stores["Cluster"].Get("ns1/watched")
	if !ok {
		t.Fatal("cluster not found in store")
	}
	gotCluster := obj.(*v1alpha1.Cluster)
	if gotCluster.Spec.AccountID != "111122223333" {
		t.Errorf("store cluster accountId = %q, want 111122223333", gotCluster.Spec.AccountID)
	}

	// Update — watcher should deliver modified event.
	cluster.Spec.Region = "eu-west-1"
	if err := c.Update(ctx, cluster); err != nil {
		t.Fatalf("Update: %v", err)
	}

	if !waitForCondition(t, func() bool {
		obj, ok := stores["Cluster"].Get("ns1/watched")
		if !ok {
			return false
		}
		return obj.(*v1alpha1.Cluster).Spec.Region == "eu-west-1"
	}, 5*time.Second) {
		t.Fatal("store not updated after Update")
	}

	// Delete — watcher should remove from store.
	if err := c.Delete(ctx, cluster); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if !waitForCondition(t, func() bool {
		_, ok := stores["Cluster"].Get("ns1/watched")
		return !ok
	}, 5*time.Second) {
		t.Fatal("cluster still in store after delete")
	}

	// Verify cursor advanced.
	if watcher.Cursor() == 0 {
		t.Error("cursor is 0 after events")
	}
}

func TestWatcherManagementCluster(t *testing.T) {
	pool := setupPostgres(t)
	logger := testLogger()
	c := NewDirectClient(pool, logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stores := make(map[string]*InformerStore)
	for _, kind := range RegisteredKinds() {
		stores[kind] = NewInformerStore(kind, logger)
	}

	watcher := NewWatcher(pool, WatchConfig{
		PollIdle: 100 * time.Millisecond,
	}, logger)
	for kind, store := range stores {
		watcher.RegisterStore(kind, store)
	}

	go func() {
		if err := watcher.Run(ctx); err != nil && ctx.Err() == nil {
			t.Errorf("watcher.Run: %v", err)
		}
	}()

	waitForSync(t, stores, 10*time.Second)

	mc := newTestManagementCluster("mc-watch")
	if err := c.Create(ctx, mc); err != nil {
		t.Fatalf("Create: %v", err)
	}

	key := GlobalNamespace + "/mc-watch"
	if !waitForStore(t, stores["ManagementCluster"], key, 5*time.Second) {
		t.Fatal("ManagementCluster not in store")
	}
}

// Helpers

func waitForStore(t *testing.T, store *InformerStore, key string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if _, ok := store.Get(key); ok {
			return true
		}
		select {
		case <-deadline:
			return false
		default:
			time.Sleep(20 * time.Millisecond)
		}
	}
}

func waitForCondition(t *testing.T, fn func() bool, timeout time.Duration) bool {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if fn() {
			return true
		}
		select {
		case <-deadline:
			return false
		default:
			time.Sleep(20 * time.Millisecond)
		}
	}
}

func waitForSync(t *testing.T, stores map[string]*InformerStore, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		allSynced := true
		for _, s := range stores {
			if !s.HasSynced() {
				allSynced = false
				break
			}
		}
		if allSynced {
			return
		}
		select {
		case <-deadline:
			t.Fatal("timeout waiting for cache sync")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// Ensure Get works through the cache path.
func TestCacheBackedGet(t *testing.T) {
	pool := setupPostgres(t)
	logger := testLogger()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stores := make(map[string]*InformerStore)
	for _, kind := range RegisteredKinds() {
		stores[kind] = NewInformerStore(kind, logger)
	}

	watcher := NewWatcher(pool, WatchConfig{
		PollIdle: 100 * time.Millisecond,
	}, logger)
	for kind, store := range stores {
		watcher.RegisterStore(kind, store)
	}

	cache := NewCache(stores)
	c := NewClient(pool, cache, logger)

	go func() {
		if err := watcher.Run(ctx); err != nil && ctx.Err() == nil {
			t.Errorf("watcher.Run: %v", err)
		}
	}()

	waitForSync(t, stores, 10*time.Second)

	// Wait for cache to notice sync.
	cache.WaitForCacheSync(ctx)

	cluster := newTestCluster("ns1", "cache-get", "111122223333")
	if err := c.Create(ctx, cluster); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Wait for watcher to deliver.
	if !waitForStore(t, stores["Cluster"], "ns1/cache-get", 5*time.Second) {
		t.Fatal("cluster not in store")
	}

	got := &v1alpha1.Cluster{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "ns1", Name: "cache-get"}, got); err != nil {
		t.Fatalf("cache-backed Get: %v", err)
	}
	if got.Spec.AccountID != "111122223333" {
		t.Errorf("AccountID = %q, want 111122223333", got.Spec.AccountID)
	}
}
