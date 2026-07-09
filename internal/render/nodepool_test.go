package render

import (
	"testing"

	hypershiftv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	hyperfleetv1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func testNodePool() *hyperfleetv1alpha1.NodePool {
	return &hyperfleetv1alpha1.NodePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "workers",
			Namespace: "cluster-abc12345",
		},
		Spec: hyperfleetv1alpha1.NodePoolSpec{
			NodePool: hypershiftv1beta1.NodePoolSpec{
				Replicas: ptr.To(int32(3)),
				Management: hypershiftv1beta1.NodePoolManagement{
					AutoRepair:  true,
					UpgradeType: hypershiftv1beta1.UpgradeTypeReplace,
				},
				Release: hypershiftv1beta1.Release{Image: "quay.io/ocp:4.17"},
				Platform: hypershiftv1beta1.NodePoolPlatform{
					Type: hypershiftv1beta1.AWSPlatform,
					AWS: &hypershiftv1beta1.AWSNodePoolPlatform{
						InstanceType:    "m6a.xlarge",
						RootVolume:      &hypershiftv1beta1.Volume{Size: 120, Type: "gp3"},
						InstanceProfile: "worker-profile",
						Subnet: hypershiftv1beta1.AWSResourceReference{
							ID: ptr.To("subnet-1"),
						},
						SecurityGroups: []hypershiftv1beta1.AWSResourceReference{
							{ID: ptr.To("sg-abc")},
							{ID: ptr.To("sg-def")},
						},
					},
				},
			},
		},
	}
}

func TestNodePoolResourceGVR(t *testing.T) {
	r := NodePoolResource(testNodePool(), testCluster())

	if r.Group != "hypershift.openshift.io" {
		t.Errorf("Group = %q, want %q", r.Group, "hypershift.openshift.io")
	}
	if r.Version != "v1beta1" {
		t.Errorf("Version = %q, want %q", r.Version, "v1beta1")
	}
	if r.Resource != "nodepools" {
		t.Errorf("Resource = %q, want %q", r.Resource, "nodepools")
	}
}

func TestNodePoolResourceNaming(t *testing.T) {
	r := NodePoolResource(testNodePool(), testCluster())

	wantName := "my-cluster-workers"
	if r.Name != wantName {
		t.Errorf("Name = %q, want %q", r.Name, wantName)
	}
	wantNS := "cluster-abc12345"
	if r.Namespace != wantNS {
		t.Errorf("Namespace = %q, want %q", r.Namespace, wantNS)
	}
}

func TestNodePoolResourceObject(t *testing.T) {
	r := NodePoolResource(testNodePool(), testCluster())
	np, ok := r.Object.(*hypershiftv1beta1.NodePool)
	if !ok {
		t.Fatalf("Object is %T, want *NodePool", r.Object)
	}

	if np.Spec.ClusterName != "my-cluster" {
		t.Errorf("ClusterName = %q, want %q", np.Spec.ClusterName, "my-cluster")
	}
	if np.Spec.Replicas == nil || *np.Spec.Replicas != 3 {
		t.Errorf("Replicas = %v, want 3", np.Spec.Replicas)
	}
	if np.Spec.Platform.Type != hypershiftv1beta1.AWSPlatform {
		t.Errorf("Platform.Type = %q, want AWS", np.Spec.Platform.Type)
	}
	if np.Spec.Platform.AWS == nil {
		t.Fatal("Platform.AWS is nil")
	}
	if np.Spec.Platform.AWS.InstanceType != "m6a.xlarge" {
		t.Errorf("InstanceType = %q, want %q", np.Spec.Platform.AWS.InstanceType, "m6a.xlarge")
	}
	if got := len(np.Spec.Platform.AWS.SecurityGroups); got != 2 {
		t.Errorf("SecurityGroups count = %d, want 2", got)
	}
}

func TestNodePoolResourceLabels(t *testing.T) {
	r := NodePoolResource(testNodePool(), testCluster())
	np := r.Object.(*hypershiftv1beta1.NodePool)

	if np.Labels["hyperfleet.io/cluster-id"] != "abc12345" {
		t.Errorf("cluster-id label = %q, want %q", np.Labels["hyperfleet.io/cluster-id"], "abc12345")
	}
}
