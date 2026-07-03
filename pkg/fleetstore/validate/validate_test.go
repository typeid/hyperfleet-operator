package validate

import (
	"strings"
	"testing"

	v1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestCreateValidNames(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
		errMsg  string
	}{
		{"my-cluster", false, ""},
		{"a", false, ""},
		{"abc-123-def", false, ""},
		{"", true, "name is required"},
		{"UPPER", true, "RFC1123"},
		{"has space", true, "RFC1123"},
		{"has_underscore", true, "RFC1123"},
		{"-leading-dash", true, "RFC1123"},
		{"trailing-dash-", true, "RFC1123"},
		{strings.Repeat("a", 254), true, "at most 253"},
		{strings.Repeat("a", 253), false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := &v1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: tt.name, Namespace: "ns"},
				Spec:       v1alpha1.ClusterSpec{AccountID: "111122223333"},
			}
			err := Create(obj, "111122223333")
			if (err != nil) != tt.wantErr {
				t.Errorf("Create(%q) error = %v, wantErr %v", tt.name, err, tt.wantErr)
			}
			if err != nil && tt.errMsg != "" && !strings.Contains(err.Error(), tt.errMsg) {
				t.Errorf("Create(%q) error = %q, want containing %q", tt.name, err, tt.errMsg)
			}
		})
	}
}

func TestCreateRequiresAccountID(t *testing.T) {
	obj := &v1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "ns"},
		Spec:       v1alpha1.ClusterSpec{AccountID: "111122223333"},
	}
	err := Create(obj, "")
	if err == nil {
		t.Error("expected error for missing accountID on namespaced kind")
	}
	if !strings.Contains(err.Error(), "aws_account_id is required") {
		t.Errorf("error = %q, want containing 'aws_account_id is required'", err)
	}
}

func TestCreateGlobalKindNoAccountID(t *testing.T) {
	mc := &v1alpha1.ManagementCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "mc-1"},
		Spec:       v1alpha1.ManagementClusterSpec{Region: "us-east-1", AccountID: "111122223333"},
	}
	if err := Create(mc, ""); err != nil {
		t.Errorf("Create ManagementCluster without accountID should succeed: %v", err)
	}
}

func TestCreateClusterRefValidation(t *testing.T) {
	np := &v1alpha1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: "np-1", Namespace: "my-cluster"},
		Spec:       v1alpha1.NodePoolSpec{ClusterRef: "wrong-cluster"},
	}
	err := Create(np, "111122223333")
	if err == nil {
		t.Error("expected error for clusterRef != namespace")
	}
	if !strings.Contains(err.Error(), "clusterRef") {
		t.Errorf("error = %q, want containing 'clusterRef'", err)
	}

	np.Spec.ClusterRef = "my-cluster"
	if err := Create(np, "111122223333"); err != nil {
		t.Errorf("valid clusterRef should pass: %v", err)
	}
}

func TestUpdateImmutableFields(t *testing.T) {
	old := &v1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "c1",
			Namespace: "ns",
			UID:       types.UID("uid-1"),
		},
		Spec: v1alpha1.ClusterSpec{AccountID: "111122223333"},
	}

	t.Run("namespace change", func(t *testing.T) {
		new := old.DeepCopy()
		new.Namespace = "other"
		if err := Update(old, new); err == nil {
			t.Error("expected error for namespace change")
		}
	})

	t.Run("name change", func(t *testing.T) {
		new := old.DeepCopy()
		new.Name = "other"
		if err := Update(old, new); err == nil {
			t.Error("expected error for name change")
		}
	})

	t.Run("uid change", func(t *testing.T) {
		new := old.DeepCopy()
		new.UID = "uid-2"
		if err := Update(old, new); err == nil {
			t.Error("expected error for uid change")
		}
	})

	t.Run("accountId change", func(t *testing.T) {
		new := old.DeepCopy()
		new.Spec.AccountID = "999988887777"
		if err := Update(old, new); err == nil {
			t.Error("expected error for accountId change")
		}
	})
}

func TestUpdateDeletionTimestamp(t *testing.T) {
	now := metav1.Now()

	t.Run("cannot unset", func(t *testing.T) {
		old := &v1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "c1", Namespace: "ns", UID: "uid-1",
				DeletionTimestamp: &now,
			},
			Spec: v1alpha1.ClusterSpec{AccountID: "111122223333"},
		}
		new := old.DeepCopy()
		new.DeletionTimestamp = nil
		if err := Update(old, new); err == nil {
			t.Error("expected error for unsetting deletion_timestamp")
		}
	})

	t.Run("cannot set via Update", func(t *testing.T) {
		old := &v1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "c1", Namespace: "ns", UID: "uid-1",
			},
			Spec: v1alpha1.ClusterSpec{AccountID: "111122223333"},
		}
		new := old.DeepCopy()
		new.DeletionTimestamp = &now
		if err := Update(old, new); err == nil {
			t.Error("expected error for setting deletion_timestamp via Update")
		}
	})
}
