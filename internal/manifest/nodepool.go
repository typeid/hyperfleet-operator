package manifest

import (
	"fmt"

	hyperfleetv1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
)

// NodePoolManifest generates the HyperShift NodePool resource for the MC.
// TODO: Use github.com/openshift/hypershift/api/hypershift/v1beta1.NodePool for type safety.
func NodePoolManifest(nodePool *hyperfleetv1alpha1.NodePool, cluster *hyperfleetv1alpha1.Cluster) Manifest {
	clusterID := cluster.Name
	clusterName := cluster.Spec.Name
	ns := clusterNamespace(clusterID)
	npName := fmt.Sprintf("%s-%s", clusterName, nodePool.Name)

	securityGroups := make([]map[string]string, 0, len(nodePool.Spec.Platform.AWS.SecurityGroups))
	for _, sg := range nodePool.Spec.Platform.AWS.SecurityGroups {
		securityGroups = append(securityGroups, map[string]string{"id": sg})
	}

	return Manifest{
		Group: "hypershift.openshift.io", Version: "v1beta1", Resource: "nodepools", Name: npName,
		Object: map[string]any{
			"apiVersion": "hypershift.openshift.io/v1beta1",
			"kind":       "NodePool",
			"metadata": map[string]any{
				"name":      npName,
				"namespace": ns,
				"labels": map[string]string{
					"hyperfleet.io/cluster-id": clusterID,
				},
			},
			"spec": map[string]any{
				"clusterName": clusterName,
				"replicas":    nodePool.Spec.Replicas,
				"management": map[string]any{
					"autoRepair":  nodePool.Spec.Management.AutoRepair,
					"upgradeType": nodePool.Spec.Management.UpgradeType,
				},
				"release": map[string]string{
					"image": nodePool.Spec.Release.Image,
				},
				"platform": map[string]any{
					"type": "AWS",
					"aws": map[string]any{
						"instanceType": nodePool.Spec.Platform.AWS.InstanceType,
						"rootVolume": map[string]any{
							"size": nodePool.Spec.Platform.AWS.RootVolume.Size,
							"type": nodePool.Spec.Platform.AWS.RootVolume.Type,
						},
						"subnet": map[string]string{
							"id": nodePool.Spec.Platform.AWS.SubnetId,
						},
						"instanceProfile": nodePool.Spec.Platform.AWS.InstanceProfile,
						"securityGroups":  securityGroups,
						"resourceTags": []map[string]string{
							{"key": "red-hat-managed", "value": "true"},
						},
					},
				},
			},
		},
	}
}
