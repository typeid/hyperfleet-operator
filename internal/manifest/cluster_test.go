package manifest

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	hyperfleetv1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
)

func testCluster() *hyperfleetv1alpha1.Cluster {
	return &hyperfleetv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "abc12345"},
		Spec: hyperfleetv1alpha1.ClusterSpec{
			Name:             "my-cluster",
			AccountID:        "123456789012",
			Region:           "us-east-1",
			Zone:             "us-east-1a",
			BaseDomain:       "example.com",
			VpcID:            "vpc-abc",
			PrivateSubnetIds: "subnet-1",
			OIDCIssuerURL:    "https://oidc.example.com/abc12345",
			Release:          hyperfleetv1alpha1.ReleaseSpec{Image: "quay.io/ocp:4.17"},
			CreatorARN:       "arn:aws:iam::123456789012:user/admin",
			Networking: hyperfleetv1alpha1.NetworkingSpec{
				ClusterNetwork: []hyperfleetv1alpha1.NetworkEntry{{CIDR: "10.128.0.0/14"}},
				ServiceNetwork: []hyperfleetv1alpha1.NetworkEntry{{CIDR: "172.30.0.0/16"}},
				MachineNetwork: []hyperfleetv1alpha1.NetworkEntry{{CIDR: "10.0.0.0/16"}},
			},
			Platform: hyperfleetv1alpha1.PlatformSpec{
				AWS: hyperfleetv1alpha1.AWSPlatformSpec{
					Roles: hyperfleetv1alpha1.AWSRolesSpec{
						ControlPlaneOperatorARN: "arn:cpo",
						IngressARN:              "arn:ingress",
						ImageRegistryARN:        "arn:registry",
						KubeCloudControllerARN:  "arn:kccm",
						NodePoolManagementARN:   "arn:npm",
						NetworkARN:              "arn:network",
						StorageARN:              "arn:storage",
					},
				},
			},
		},
	}
}

func TestClusterManifestsCount(t *testing.T) {
	manifests := ClusterManifests(testCluster())
	if got := len(manifests); got != 7 {
		t.Errorf("expected 7 manifests, got %d", got)
	}
}

func TestClusterManifestsTypes(t *testing.T) {
	manifests := ClusterManifests(testCluster())

	expected := []struct {
		resource string
		name     string
	}{
		{"namespaces", "clusters-abc12345"},
		{"configmaps", "cluster-config"},
		{"configmaps", "aws-iam-auth-config"},
		{"externalsecrets", "pull-secret"},
		{"certificates", "api-serving-cert"},
		{"hostedclusters", "my-cluster"},
		{"secrets", "ssh-key"},
	}

	for i, e := range expected {
		if manifests[i].Resource != e.resource {
			t.Errorf("manifest[%d]: expected resource %q, got %q", i, e.resource, manifests[i].Resource)
		}
		if manifests[i].Name != e.name {
			t.Errorf("manifest[%d]: expected name %q, got %q", i, e.name, manifests[i].Name)
		}
	}
}

func TestHash4(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"abc12345", "abc1"},
		{"ab", "ab"},
		{"abcd", "abcd"},
		{"", ""},
	}
	for _, tt := range tests {
		if got := hash4(tt.in); got != tt.want {
			t.Errorf("hash4(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestHostedClusterDNS(t *testing.T) {
	manifests := ClusterManifests(testCluster())

	var hc map[string]any
	for _, m := range manifests {
		if m.Resource == "hostedclusters" {
			hc = m.Object
			break
		}
	}
	if hc == nil {
		t.Fatal("no hostedcluster manifest found")
	}

	spec := hc["spec"].(map[string]any)

	dns := spec["dns"].(map[string]string)
	if got := dns["baseDomain"]; got != "abc1.example.com" {
		t.Errorf("dns.baseDomain = %q, want %q", got, "abc1.example.com")
	}

	if got := spec["kubeAPIServerDNSName"]; got != "api.my-cluster.abc1.example.com" {
		t.Errorf("kubeAPIServerDNSName = %q, want %q", got, "api.my-cluster.abc1.example.com")
	}

	if got := spec["issuerURL"]; got != "https://oidc.example.com/abc12345" {
		t.Errorf("issuerURL = %q, want %q", got, "https://oidc.example.com/abc12345")
	}
}

func TestCreatorARNInAuthConfig(t *testing.T) {
	manifests := ClusterManifests(testCluster())

	var cm map[string]any
	for _, m := range manifests {
		if m.Name == "aws-iam-auth-config" {
			cm = m.Object
			break
		}
	}

	data := cm["data"].(map[string]string)
	cfg := data["config.yaml"]
	if cfg == "" {
		t.Fatal("config.yaml is empty")
	}
	if !contains(cfg, "arn:aws:iam::123456789012:user/admin") {
		t.Error("config.yaml should contain the creator ARN")
	}
	if !contains(cfg, "cluster-creator") {
		t.Error("config.yaml should contain the cluster-creator username")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
