//go:build fleetstore

package fleetstore

import (
	"context"
	"testing"
	"time"

	v1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestCreateAndGet(t *testing.T) {
	pool := setupPostgres(t)
	c := NewDirectClient(pool, testLogger())
	ctx := context.Background()

	cluster := newTestCluster("ns1", "c1", "111122223333")
	if err := c.Create(ctx, cluster); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if cluster.GetUID() == "" {
		t.Error("UID not populated after Create")
	}
	if cluster.GetResourceVersion() == "" {
		t.Error("ResourceVersion not populated after Create")
	}
	if cluster.GetGeneration() != 1 {
		t.Errorf("Generation = %d, want 1", cluster.GetGeneration())
	}

	got := &v1alpha1.Cluster{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "ns1", Name: "c1"}, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Spec.AccountID != "111122223333" {
		t.Errorf("AccountID = %q, want 111122223333", got.Spec.AccountID)
	}
	if got.Name != "c1" {
		t.Errorf("Name = %q, want c1", got.Name)
	}
}

func TestCreateDuplicate(t *testing.T) {
	pool := setupPostgres(t)
	c := NewDirectClient(pool, testLogger())
	ctx := context.Background()

	cluster := newTestCluster("ns1", "dup", "111122223333")
	if err := c.Create(ctx, cluster); err != nil {
		t.Fatalf("Create: %v", err)
	}

	dup := newTestCluster("ns1", "dup", "111122223333")
	err := c.Create(ctx, dup)
	if !apierrors.IsAlreadyExists(err) {
		t.Errorf("Create duplicate: got %v, want AlreadyExists", err)
	}
}

func TestGetNotFound(t *testing.T) {
	pool := setupPostgres(t)
	c := NewDirectClient(pool, testLogger())
	ctx := context.Background()

	got := &v1alpha1.Cluster{}
	err := c.Get(ctx, client.ObjectKey{Namespace: "ns1", Name: "nope"}, got)
	if !apierrors.IsNotFound(err) {
		t.Errorf("Get missing: got %v, want NotFound", err)
	}
}

func TestListAll(t *testing.T) {
	pool := setupPostgres(t)
	c := NewDirectClient(pool, testLogger())
	ctx := context.Background()

	for _, name := range []string{"a", "b", "c"} {
		cl := newTestCluster("ns1", name, "111122223333")
		if err := c.Create(ctx, cl); err != nil {
			t.Fatalf("Create %s: %v", name, err)
		}
	}

	list := &v1alpha1.ClusterList{}
	if err := c.List(ctx, list); err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list.Items) != 3 {
		t.Errorf("List len = %d, want 3", len(list.Items))
	}
}

func TestListByNamespace(t *testing.T) {
	pool := setupPostgres(t)
	c := NewDirectClient(pool, testLogger())
	ctx := context.Background()

	c1 := newTestCluster("ns-a", "c1", "111122223333")
	c2 := newTestCluster("ns-b", "c2", "111122223333")
	if err := c.Create(ctx, c1); err != nil {
		t.Fatalf("Create c1: %v", err)
	}
	if err := c.Create(ctx, c2); err != nil {
		t.Fatalf("Create c2: %v", err)
	}

	list := &v1alpha1.ClusterList{}
	if err := c.List(ctx, list, client.InNamespace("ns-a")); err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list.Items) != 1 {
		t.Errorf("List ns-a len = %d, want 1", len(list.Items))
	}
	if list.Items[0].Name != "c1" {
		t.Errorf("List ns-a[0].Name = %q, want c1", list.Items[0].Name)
	}
}

func TestListByLabel(t *testing.T) {
	pool := setupPostgres(t)
	c := NewDirectClient(pool, testLogger())
	ctx := context.Background()

	c1 := newTestCluster("ns1", "labeled", "111122223333")
	c1.SetLabels(map[string]string{"tier": "gold"})
	c2 := newTestCluster("ns1", "unlabeled", "111122223333")

	if err := c.Create(ctx, c1); err != nil {
		t.Fatalf("Create c1: %v", err)
	}
	if err := c.Create(ctx, c2); err != nil {
		t.Fatalf("Create c2: %v", err)
	}

	sel, _ := labels.Parse("tier=gold")
	list := &v1alpha1.ClusterList{}
	if err := c.List(ctx, list, client.MatchingLabelsSelector{Selector: sel}); err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list.Items) != 1 {
		t.Errorf("List with label len = %d, want 1", len(list.Items))
	}
	if list.Items[0].Name != "labeled" {
		t.Errorf("List[0].Name = %q, want labeled", list.Items[0].Name)
	}
}

func TestUpdateCAS(t *testing.T) {
	pool := setupPostgres(t)
	c := NewDirectClient(pool, testLogger())
	ctx := context.Background()

	cluster := newTestCluster("ns1", "cas-test", "111122223333")
	if err := c.Create(ctx, cluster); err != nil {
		t.Fatalf("Create: %v", err)
	}
	rv1 := cluster.GetResourceVersion()

	cluster.Spec.Region = "eu-west-1"
	if err := c.Update(ctx, cluster); err != nil {
		t.Fatalf("Update: %v", err)
	}
	rv2 := cluster.GetResourceVersion()
	if rv2 == rv1 {
		t.Error("ResourceVersion unchanged after Update")
	}

	got := &v1alpha1.Cluster{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "ns1", Name: "cas-test"}, got); err != nil {
		t.Fatalf("Get after Update: %v", err)
	}
	if got.Spec.Region != "eu-west-1" {
		t.Errorf("Region = %q, want eu-west-1", got.Spec.Region)
	}
}

func TestUpdateStaleResourceVersion(t *testing.T) {
	pool := setupPostgres(t)
	c := NewDirectClient(pool, testLogger())
	ctx := context.Background()

	cluster := newTestCluster("ns1", "stale", "111122223333")
	if err := c.Create(ctx, cluster); err != nil {
		t.Fatalf("Create: %v", err)
	}

	stale := cluster.DeepCopy()

	cluster.Spec.Region = "eu-west-1"
	if err := c.Update(ctx, cluster); err != nil {
		t.Fatalf("Update 1: %v", err)
	}

	stale.Spec.Region = "ap-south-1"
	err := c.Update(ctx, stale)
	if !apierrors.IsConflict(err) {
		t.Errorf("Update stale: got %v, want Conflict", err)
	}
}

func TestUpdateSpecChangeBumpsGeneration(t *testing.T) {
	pool := setupPostgres(t)
	c := NewDirectClient(pool, testLogger())
	ctx := context.Background()

	cluster := newTestCluster("ns1", "gen-test", "111122223333")
	if err := c.Create(ctx, cluster); err != nil {
		t.Fatalf("Create: %v", err)
	}
	gen1 := cluster.GetGeneration()

	cluster.Spec.Region = "us-west-2"
	if err := c.Update(ctx, cluster); err != nil {
		t.Fatalf("Update: %v", err)
	}
	gen2 := cluster.GetGeneration()
	if gen2 != gen1+1 {
		t.Errorf("generation after spec change = %d, want %d", gen2, gen1+1)
	}
}

func TestUpdateMetadataOnlyNoGenerationBump(t *testing.T) {
	pool := setupPostgres(t)
	c := NewDirectClient(pool, testLogger())
	ctx := context.Background()

	cluster := newTestCluster("ns1", "meta-only", "111122223333")
	if err := c.Create(ctx, cluster); err != nil {
		t.Fatalf("Create: %v", err)
	}
	gen1 := cluster.GetGeneration()

	cluster.SetLabels(map[string]string{"new": "label"})
	if err := c.Update(ctx, cluster); err != nil {
		t.Fatalf("Update: %v", err)
	}
	gen2 := cluster.GetGeneration()
	if gen2 != gen1 {
		t.Errorf("generation after metadata-only change = %d, want %d (unchanged)", gen2, gen1)
	}
}

func TestDeleteNoFinalizers(t *testing.T) {
	pool := setupPostgres(t)
	c := NewDirectClient(pool, testLogger())
	ctx := context.Background()

	cluster := newTestCluster("ns1", "del-nofin", "111122223333")
	if err := c.Create(ctx, cluster); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := c.Delete(ctx, cluster); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	got := &v1alpha1.Cluster{}
	err := c.Get(ctx, client.ObjectKey{Namespace: "ns1", Name: "del-nofin"}, got)
	if !apierrors.IsNotFound(err) {
		t.Errorf("Get after delete: got %v, want NotFound", err)
	}
}

func TestDeleteWithFinalizers(t *testing.T) {
	pool := setupPostgres(t)
	c := NewDirectClient(pool, testLogger())
	ctx := context.Background()

	cluster := newTestCluster("ns1", "del-fin", "111122223333")
	cluster.SetFinalizers([]string{"test.example.com/cleanup"})
	if err := c.Create(ctx, cluster); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := c.Delete(ctx, cluster); err != nil {
		t.Fatalf("Delete phase 1: %v", err)
	}

	got := &v1alpha1.Cluster{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "ns1", Name: "del-fin"}, got); err != nil {
		t.Fatalf("Get after phase 1: %v", err)
	}
	if got.GetDeletionTimestamp() == nil {
		t.Error("DeletionTimestamp not set after phase 1 delete")
	}
	if got.Spec.AccountID != "111122223333" {
		t.Errorf("Spec lost after delete phase 1: AccountID = %q", got.Spec.AccountID)
	}
}

func TestFinalizationFlow(t *testing.T) {
	pool := setupPostgres(t)
	c := NewDirectClient(pool, testLogger())
	ctx := context.Background()

	cluster := newTestCluster("ns1", "finalizer-flow", "111122223333")
	cluster.SetFinalizers([]string{"test.example.com/cleanup"})
	if err := c.Create(ctx, cluster); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := c.Delete(ctx, cluster); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	got := &v1alpha1.Cluster{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "ns1", Name: "finalizer-flow"}, got); err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if got.GetDeletionTimestamp() == nil {
		t.Fatal("DeletionTimestamp not set")
	}

	got.SetFinalizers(nil)
	if err := c.Update(ctx, got); err != nil {
		t.Fatalf("Update removing finalizer: %v", err)
	}

	err := c.Get(ctx, client.ObjectKey{Namespace: "ns1", Name: "finalizer-flow"}, &v1alpha1.Cluster{})
	if !apierrors.IsNotFound(err) {
		t.Errorf("Get after finalization: got %v, want NotFound", err)
	}
}

func TestTombstoneRevival(t *testing.T) {
	pool := setupPostgres(t)
	c := NewDirectClient(pool, testLogger())
	ctx := context.Background()

	cluster := newTestCluster("ns1", "revive", "111122223333")
	if err := c.Create(ctx, cluster); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := c.Delete(ctx, cluster); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	revived := newTestCluster("ns1", "revive", "111122223333")
	revived.Spec.Region = "ap-southeast-1"
	if err := c.Create(ctx, revived); err != nil {
		t.Fatalf("Create (revival): %v", err)
	}

	got := &v1alpha1.Cluster{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "ns1", Name: "revive"}, got); err != nil {
		t.Fatalf("Get after revival: %v", err)
	}
	if got.Spec.Region != "ap-southeast-1" {
		t.Errorf("Region = %q, want ap-southeast-1", got.Spec.Region)
	}
	if got.GetDeletionTimestamp() != nil {
		t.Error("DeletionTimestamp still set after revival")
	}
}

func TestDeleteNotFound(t *testing.T) {
	pool := setupPostgres(t)
	c := NewDirectClient(pool, testLogger())
	ctx := context.Background()

	cluster := &v1alpha1.Cluster{}
	cluster.SetNamespace("ns1")
	cluster.SetName("ghost")
	err := c.Delete(ctx, cluster)
	if !apierrors.IsNotFound(err) {
		t.Errorf("Delete non-existent: got %v, want NotFound", err)
	}
}

func TestStatusUpdate(t *testing.T) {
	pool := setupPostgres(t)
	c := NewDirectClient(pool, testLogger())
	ctx := context.Background()

	cluster := newTestCluster("ns1", "status-test", "111122223333")
	if err := c.Create(ctx, cluster); err != nil {
		t.Fatalf("Create: %v", err)
	}

	cluster.Status.Conditions = []metav1.Condition{
		{
			Type:               "Ready",
			Status:             metav1.ConditionTrue,
			Reason:             "AllGood",
			LastTransitionTime: metav1.Now(),
		},
	}
	if err := c.Status().Update(ctx, cluster); err != nil {
		t.Fatalf("Status.Update: %v", err)
	}

	got := &v1alpha1.Cluster{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "ns1", Name: "status-test"}, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got.Status.Conditions) != 1 {
		t.Fatalf("conditions len = %d, want 1", len(got.Status.Conditions))
	}
	if got.Status.Conditions[0].Type != "Ready" {
		t.Errorf("condition type = %q, want Ready", got.Status.Conditions[0].Type)
	}
}

func TestManagementClusterCRUD(t *testing.T) {
	pool := setupPostgres(t)
	c := NewDirectClient(pool, testLogger())
	ctx := context.Background()

	mc := newTestManagementCluster("mc-crud")
	if err := c.Create(ctx, mc); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got := &v1alpha1.ManagementCluster{}
	if err := c.Get(ctx, client.ObjectKey{Name: "mc-crud"}, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Spec.Region != "us-east-1" {
		t.Errorf("Region = %q, want us-east-1", got.Spec.Region)
	}

	list := &v1alpha1.ManagementClusterList{}
	if err := c.List(ctx, list); err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list.Items) < 1 {
		t.Error("List ManagementClusters returned 0 items")
	}

	if err := c.Delete(ctx, got); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	err := c.Get(ctx, client.ObjectKey{Name: "mc-crud"}, &v1alpha1.ManagementCluster{})
	if !apierrors.IsNotFound(err) {
		t.Errorf("Get after delete: got %v, want NotFound", err)
	}
}

func TestGCDeletesOrphans(t *testing.T) {
	pool := setupPostgres(t)
	c := NewDirectClient(pool, testLogger())
	ctx := context.Background()

	owner := newTestCluster("ns1", "gc-owner", "111122223333")
	if err := c.Create(ctx, owner); err != nil {
		t.Fatalf("Create owner: %v", err)
	}

	dependent := &v1alpha1.Placement{
		TypeMeta:   metav1.TypeMeta{Kind: "Placement", APIVersion: v1alpha1.SchemeGroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "gc-dep"},
		Spec:       v1alpha1.PlacementSpec{ClusterRef: "gc-owner", ManagementCluster: "mc01"},
	}
	dependent.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: v1alpha1.SchemeGroupVersion.String(),
		Kind:       "Cluster",
		Name:       owner.Name,
		UID:        owner.GetUID(),
	}})
	if err := c.Create(ctx, dependent); err != nil {
		t.Fatalf("Create dependent: %v", err)
	}

	// Dependent should exist before GC.
	got := &v1alpha1.Placement{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "ns1", Name: "gc-dep"}, got); err != nil {
		t.Fatalf("Get dependent before GC: %v", err)
	}

	// GC should not delete the dependent while the owner is alive.
	auditor := NewAuditor(pool, nil, nil, DefaultAuditConfig(), testLogger())
	if err := auditor.runGC(ctx); err != nil {
		t.Fatalf("runGC (owner alive): %v", err)
	}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "ns1", Name: "gc-dep"}, got); err != nil {
		t.Fatal("GC deleted dependent while owner was alive")
	}

	// Delete the owner (tombstone it).
	if err := c.Delete(ctx, owner); err != nil {
		t.Fatalf("Delete owner: %v", err)
	}

	// Run GC — dependent should be deleted.
	if err := auditor.runGC(ctx); err != nil {
		t.Fatalf("runGC (owner deleted): %v", err)
	}

	err := c.Get(ctx, client.ObjectKey{Namespace: "ns1", Name: "gc-dep"}, &v1alpha1.Placement{})
	if !apierrors.IsNotFound(err) {
		t.Errorf("Get dependent after GC: got %v, want NotFound", err)
	}
}

func TestGCPreservesOwnedWithLiveOwner(t *testing.T) {
	pool := setupPostgres(t)
	c := NewDirectClient(pool, testLogger())
	ctx := context.Background()

	owner := newTestCluster("ns1", "gc-alive", "111122223333")
	if err := c.Create(ctx, owner); err != nil {
		t.Fatalf("Create owner: %v", err)
	}

	dependent := &v1alpha1.Placement{
		TypeMeta:   metav1.TypeMeta{Kind: "Placement", APIVersion: v1alpha1.SchemeGroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "gc-kept"},
		Spec:       v1alpha1.PlacementSpec{ClusterRef: "gc-alive", ManagementCluster: "mc01"},
	}
	dependent.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: v1alpha1.SchemeGroupVersion.String(),
		Kind:       "Cluster",
		Name:       owner.Name,
		UID:        owner.GetUID(),
	}})
	if err := c.Create(ctx, dependent); err != nil {
		t.Fatalf("Create dependent: %v", err)
	}

	auditor := NewAuditor(pool, nil, nil, DefaultAuditConfig(), testLogger())
	if err := auditor.runGC(ctx); err != nil {
		t.Fatalf("runGC: %v", err)
	}

	got := &v1alpha1.Placement{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "ns1", Name: "gc-kept"}, got); err != nil {
		t.Errorf("GC incorrectly deleted dependent with live owner: %v", err)
	}
}

func TestListExcludesTombstoned(t *testing.T) {
	pool := setupPostgres(t)
	c := NewDirectClient(pool, testLogger())
	ctx := context.Background()

	alive := newTestCluster("ns1", "alive", "111122223333")
	dead := newTestCluster("ns1", "dead", "111122223333")
	if err := c.Create(ctx, alive); err != nil {
		t.Fatalf("Create alive: %v", err)
	}
	if err := c.Create(ctx, dead); err != nil {
		t.Fatalf("Create dead: %v", err)
	}
	if err := c.Delete(ctx, dead); err != nil {
		t.Fatalf("Delete dead: %v", err)
	}

	// Allow a moment for seq to advance.
	_ = time.Now()

	list := &v1alpha1.ClusterList{}
	if err := c.List(ctx, list, client.InNamespace("ns1")); err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, item := range list.Items {
		if item.Name == "dead" {
			t.Error("tombstoned cluster appeared in List")
		}
	}
}
