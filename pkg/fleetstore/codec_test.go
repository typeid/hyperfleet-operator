package fleetstore

import (
	"encoding/json"
	"testing"

	v1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestCodecRoundtripCluster(t *testing.T) {
	c := newTestCluster("my-ns", "test-cluster", "111122223333")
	c.SetLabels(map[string]string{"env": "prod"})
	c.SetAnnotations(map[string]string{"note": "hello"})
	c.SetFinalizers([]string{"cleanup.example.com"})
	c.SetUID(types.UID("aaaa-bbbb"))
	c.SetGeneration(5)
	c.SetOwnerReferences([]metav1.OwnerReference{
		{APIVersion: "v1", Kind: "Namespace", Name: "owner", UID: "cccc"},
	})

	row, err := Encode(c)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	if row.Kind != "Cluster" {
		t.Errorf("kind = %q, want Cluster", row.Kind)
	}
	if row.Namespace != "my-ns" {
		t.Errorf("namespace = %q, want my-ns", row.Namespace)
	}
	if row.AWSAccountID == nil || *row.AWSAccountID != "111122223333" {
		t.Errorf("aws_account_id = %v, want 111122223333", row.AWSAccountID)
	}

	row.Seq = 42
	decoded, err := Decode(row)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	dc := decoded.(*v1alpha1.Cluster)
	if dc.Name != "test-cluster" {
		t.Errorf("name = %q, want test-cluster", dc.Name)
	}
	if dc.Namespace != "my-ns" {
		t.Errorf("namespace = %q, want my-ns", dc.Namespace)
	}
	if dc.Spec.AccountID != "111122223333" {
		t.Errorf("accountId = %q, want 111122223333", dc.Spec.AccountID)
	}
	if dc.ResourceVersion != "42" {
		t.Errorf("resourceVersion = %q, want 42", dc.ResourceVersion)
	}
	if dc.Labels["env"] != "prod" {
		t.Errorf("labels = %v, want env=prod", dc.Labels)
	}
	if dc.Annotations["note"] != "hello" {
		t.Errorf("annotations = %v, want note=hello", dc.Annotations)
	}
	if len(dc.Finalizers) != 1 || dc.Finalizers[0] != "cleanup.example.com" {
		t.Errorf("finalizers = %v", dc.Finalizers)
	}
	if len(dc.OwnerReferences) != 1 || dc.OwnerReferences[0].Name != "owner" {
		t.Errorf("ownerReferences = %v", dc.OwnerReferences)
	}
	if dc.TypeMeta.Kind != "Cluster" {
		t.Errorf("TypeMeta.Kind = %q, want Cluster", dc.TypeMeta.Kind)
	}
}

func TestCodecRoundtripManagementCluster(t *testing.T) {
	mc := newTestManagementCluster("mc-1")
	mc.SetUID(types.UID("mc-uid"))

	row, err := Encode(mc)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	if row.Namespace != GlobalNamespace {
		t.Errorf("namespace = %q, want %q (global)", row.Namespace, GlobalNamespace)
	}
	if row.AWSAccountID != nil {
		t.Errorf("aws_account_id = %v, want nil for ManagementCluster", row.AWSAccountID)
	}

	row.Seq = 10
	decoded, err := Decode(row)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	dmc := decoded.(*v1alpha1.ManagementCluster)
	if dmc.Namespace != "" {
		t.Errorf("decoded namespace = %q, want empty for global kind", dmc.Namespace)
	}
	if dmc.Name != "mc-1" {
		t.Errorf("name = %q, want mc-1", dmc.Name)
	}
	if dmc.Spec.Region != "us-east-1" {
		t.Errorf("region = %q, want us-east-1", dmc.Spec.Region)
	}
}

func TestCodecNullNormalization(t *testing.T) {
	c := newTestCluster("ns", "c1", "111122223333")
	// Leave labels, annotations, ownerReferences nil.
	c.SetLabels(nil)
	c.SetAnnotations(nil)
	c.SetOwnerReferences(nil)

	row, err := Encode(c)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	if string(row.Labels) != "{}" {
		t.Errorf("labels = %s, want {}", row.Labels)
	}
	if string(row.Annotations) != "{}" {
		t.Errorf("annotations = %s, want {}", row.Annotations)
	}
	if string(row.OwnerRefs) != "[]" {
		t.Errorf("ownerRefs = %s, want []", row.OwnerRefs)
	}
	if row.Finalizers == nil {
		t.Error("finalizers is nil, want empty slice")
	}
}

func TestSeqFromResourceVersion(t *testing.T) {
	tests := []struct {
		rv      string
		want    int64
		wantErr bool
	}{
		{"42", 42, false},
		{"0", 0, false},
		{"", 0, true},
		{"abc", 0, true},
	}

	for _, tt := range tests {
		got, err := SeqFromResourceVersion(tt.rv)
		if (err != nil) != tt.wantErr {
			t.Errorf("SeqFromResourceVersion(%q) error = %v, wantErr %v", tt.rv, err, tt.wantErr)
			continue
		}
		if !tt.wantErr && got != tt.want {
			t.Errorf("SeqFromResourceVersion(%q) = %d, want %d", tt.rv, got, tt.want)
		}
	}
}

func TestCodecSpecPreserved(t *testing.T) {
	c := newTestCluster("ns", "c1", "111122223333")
	c.Spec.Region = "eu-west-1"
	c.Spec.VpcID = "vpc-xyz789"

	row, err := Encode(c)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	var spec map[string]interface{}
	if err := json.Unmarshal(row.Spec, &spec); err != nil {
		t.Fatalf("unmarshal spec: %v", err)
	}
	if spec["region"] != "eu-west-1" {
		t.Errorf("spec.region = %v, want eu-west-1", spec["region"])
	}

	row.Seq = 1
	decoded, err := Decode(row)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	dc := decoded.(*v1alpha1.Cluster)
	if dc.Spec.Region != "eu-west-1" {
		t.Errorf("decoded region = %q, want eu-west-1", dc.Spec.Region)
	}
	if dc.Spec.VpcID != "vpc-xyz789" {
		t.Errorf("decoded vpcId = %q, want vpc-xyz789", dc.Spec.VpcID)
	}
}
