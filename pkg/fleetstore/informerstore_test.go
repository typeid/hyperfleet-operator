package fleetstore

import (
	"log/slog"
	"testing"
	"time"
)

func TestInformerStoreVersionGuard(t *testing.T) {
	store := NewInformerStore("Cluster", slog.Default())

	cluster := newTestCluster("ns1", "vg", "111122223333")
	row, err := Encode(cluster)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	row.Seq = 10
	row.UpdatedAt = time.Now()
	row.CreatedAt = time.Now()
	store.Apply(row, false)

	obj, ok := store.Get("ns1/vg")
	if !ok {
		t.Fatal("not in store after Apply seq=10")
	}
	if obj.GetResourceVersion() != "10" {
		t.Errorf("rv = %q, want 10", obj.GetResourceVersion())
	}

	// Seq regression — should be dropped.
	row.Seq = 5
	store.Apply(row, false)
	obj, _ = store.Get("ns1/vg")
	if obj.GetResourceVersion() != "10" {
		t.Errorf("rv after regression = %q, want 10 (unchanged)", obj.GetResourceVersion())
	}

	// Seq advance — should update.
	row.Seq = 15
	store.Apply(row, false)
	obj, _ = store.Get("ns1/vg")
	if obj.GetResourceVersion() != "15" {
		t.Errorf("rv after advance = %q, want 15", obj.GetResourceVersion())
	}
}

func TestInformerStoreTombstone(t *testing.T) {
	store := NewInformerStore("Cluster", slog.Default())

	cluster := newTestCluster("ns1", "tomb", "111122223333")
	row, err := Encode(cluster)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	row.Seq = 1
	row.UpdatedAt = time.Now()
	row.CreatedAt = time.Now()
	store.Apply(row, false)

	_, ok := store.Get("ns1/tomb")
	if !ok {
		t.Fatal("not in store after Apply")
	}

	// Tombstone the row.
	now := time.Now()
	row.DeletedAt = &now
	row.Seq = 2
	store.Apply(row, false)

	_, ok = store.Get("ns1/tomb")
	if ok {
		t.Error("still in store after tombstone")
	}
}
