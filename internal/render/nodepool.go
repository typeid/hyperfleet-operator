package render

import (
	"fmt"

	hypershiftv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	hyperfleetv1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
)

// NodePoolResource generates the HyperShift NodePool resource for the MC.
func NodePoolResource(nodePool *hyperfleetv1alpha1.NodePool, cluster *hyperfleetv1alpha1.Cluster) Resource {
	clusterID := cluster.Name
	clusterName := cluster.Spec.Name
	ns := clusterNamespace(clusterID)
	npName := fmt.Sprintf("%s-%s", clusterName, nodePool.Name)

	securityGroups := make([]hypershiftv1beta1.AWSResourceReference, 0, len(nodePool.Spec.Platform.AWS.SecurityGroups))
	for _, sg := range nodePool.Spec.Platform.AWS.SecurityGroups {
		securityGroups = append(securityGroups, hypershiftv1beta1.AWSResourceReference{
			ID: ptr.To(sg),
		})
	}

	return Resource{
		Group: "hypershift.openshift.io", Version: "v1beta1", Resource: "nodepools",
		Name: npName, Namespace: ns,
		Object: &hypershiftv1beta1.NodePool{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "hypershift.openshift.io/v1beta1",
				Kind:       "NodePool",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      npName,
				Namespace: ns,
				Labels: map[string]string{
					"hyperfleet.io/cluster-id": clusterID,
				},
			},
			Spec: hypershiftv1beta1.NodePoolSpec{
				ClusterName: clusterName,
				Replicas:    ptr.To(nodePool.Spec.Replicas),
				Management:  nodePool.Spec.Management,
				Release:     nodePool.Spec.Release,
				Platform: hypershiftv1beta1.NodePoolPlatform{
					Type: hypershiftv1beta1.AWSPlatform,
					AWS: &hypershiftv1beta1.AWSNodePoolPlatform{
						InstanceType:    nodePool.Spec.Platform.AWS.InstanceType,
						InstanceProfile: nodePool.Spec.Platform.AWS.InstanceProfile,
						Subnet: hypershiftv1beta1.AWSResourceReference{
							ID: ptr.To(nodePool.Spec.Platform.AWS.SubnetID),
						},
						RootVolume:     &nodePool.Spec.Platform.AWS.RootVolume,
						SecurityGroups: securityGroups,
						ResourceTags: []hypershiftv1beta1.AWSResourceTag{
							{Key: "red-hat-managed", Value: "true"},
						},
					},
				},
			},
		},
	}
}
