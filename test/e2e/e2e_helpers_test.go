//go:build e2e

package e2e

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	hypershiftv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	hyperfleetv1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
)

var _ = BeforeEach(func() {
	purgeDynamoTables()
	dynamoCli.ResetCache()
})

func scanTable(tableName string) []map[string]dynamodbtypes.AttributeValue {
	out, err := dynamoDBCli.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	})
	Expect(err).NotTo(HaveOccurred())
	return out.Items
}

func attrString(item map[string]dynamodbtypes.AttributeValue, keys ...string) string {
	current := item
	for i, key := range keys {
		av, ok := current[key]
		if !ok {
			return ""
		}
		if i == len(keys)-1 {
			if sv, ok := av.(*dynamodbtypes.AttributeValueMemberS); ok {
				return sv.Value
			}
			return ""
		}
		if mv, ok := av.(*dynamodbtypes.AttributeValueMemberM); ok {
			current = mv.Value
		} else {
			return ""
		}
	}
	return ""
}

func purgeTable(tableName string) {
	items := scanTable(tableName)
	for _, item := range items {
		if docID, ok := item["documentID"]; ok {
			_, _ = dynamoDBCli.DeleteItem(ctx, &dynamodb.DeleteItemInput{
				TableName: aws.String(tableName),
				Key:       map[string]dynamodbtypes.AttributeValue{"documentID": docID},
			})
		}
	}
}

func purgeDynamoTables() {
	suffixes := []string{"-applydesires", "-deletedesires", "-readdesires"}
	for _, prefix := range []string{mc + "-specs", mc + "-status"} {
		for _, suffix := range suffixes {
			purgeTable(prefix + suffix)
		}
	}
}

func newE2ECluster(name string) *hyperfleetv1alpha1.Cluster {
	return &hyperfleetv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "111222333444"},
		Spec: hyperfleetv1alpha1.ClusterSpec{
			Name:                      "my-e2e-cluster",
			AccountID:                 "111222333444",
			Region:                    "us-east-1",
			VpcID:                     "vpc-e2e0001",
			PrivateSubnetIDs:          []string{"subnet-e2e0001"},
			WorkerInstanceProfileName: "worker-profile",
			WorkerSecurityGroupID:     "sg-e2e0001",
			OIDCIssuerURL:             "https://oidc.e2e.example.com/" + name,
			Release:                   hypershiftv1beta1.Release{Image: "quay.io/ocp:4.17"},
			CreatorARN:                "arn:aws:iam::111222333444:user/e2etester",
			Networking: hyperfleetv1alpha1.NetworkingSpec{
				ClusterNetwork: []hyperfleetv1alpha1.NetworkEntry{{CIDR: "10.128.0.0/14"}},
				ServiceNetwork: []hyperfleetv1alpha1.NetworkEntry{{CIDR: "172.30.0.0/16"}},
				MachineNetwork: []hyperfleetv1alpha1.NetworkEntry{{CIDR: "10.0.0.0/16"}},
			},
			Platform: hyperfleetv1alpha1.PlatformSpec{
				AWS: hyperfleetv1alpha1.AWSPlatformSpec{
					Roles: hypershiftv1beta1.AWSRolesRef{
						ControlPlaneOperatorARN: "arn:aws:iam::111222333444:role/cpo",
						IngressARN:              "arn:aws:iam::111222333444:role/ingress",
						ImageRegistryARN:        "arn:aws:iam::111222333444:role/registry",
						KubeCloudControllerARN:  "arn:aws:iam::111222333444:role/kccm",
						NodePoolManagementARN:   "arn:aws:iam::111222333444:role/npm",
						NetworkARN:              "arn:aws:iam::111222333444:role/network",
						StorageARN:              "arn:aws:iam::111222333444:role/storage",
					},
				},
			},
		},
	}
}

func newE2ENodePool(clusterRef string) *hyperfleetv1alpha1.NodePool {
	return &hyperfleetv1alpha1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: "e2e-nodepool", Namespace: "111222333444"},
		Spec: hyperfleetv1alpha1.NodePoolSpec{
			ClusterRef: clusterRef,
			Replicas:   3,
			Management: hypershiftv1beta1.NodePoolManagement{
				AutoRepair:  true,
				UpgradeType: hypershiftv1beta1.UpgradeTypeReplace,
			},
			Release: hypershiftv1beta1.Release{Image: "quay.io/ocp:4.17"},
			Platform: hyperfleetv1alpha1.NodePoolPlatformSpec{
				AWS: hyperfleetv1alpha1.AWSNodePoolSpec{
					InstanceType:    "m6a.xlarge",
					RootVolume:      hypershiftv1beta1.Volume{Size: 120, Type: "gp3"},
					SubnetID:        "subnet-e2e0001",
					InstanceProfile: "worker-profile",
					SecurityGroups:  []string{"sg-e2e0001"},
				},
			},
		},
	}
}

func newE2EManifest(name string) *hyperfleetv1alpha1.Manifest {
	return &hyperfleetv1alpha1.Manifest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "111222333444",
		},
		Spec: hyperfleetv1alpha1.ManifestSpec{
			ManagementCluster: "mc01",
			Resources: []hyperfleetv1alpha1.ResourceTemplate{
				{
					Resource: "serviceaccounts",
					Content:  runtime.RawExtension{Raw: []byte(`{"apiVersion":"v1","kind":"ServiceAccount","metadata":{"name":"e2e-runner","namespace":"e2e-actions"}}`)},
				},
				{
					Resource: "roles",
					Content:  runtime.RawExtension{Raw: []byte(`{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"Role","metadata":{"name":"e2e-runner","namespace":"e2e-actions"},"rules":[{"apiGroups":[""],"resources":["pods/log"],"verbs":["get"]}]}`)},
				},
				{
					Resource: "rolebindings",
					Content:  runtime.RawExtension{Raw: []byte(`{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"RoleBinding","metadata":{"name":"e2e-runner","namespace":"e2e-actions"},"roleRef":{"apiGroup":"rbac.authorization.k8s.io","kind":"Role","name":"e2e-runner"},"subjects":[{"kind":"ServiceAccount","name":"e2e-runner","namespace":"e2e-actions"}]}`)},
				},
				{
					Resource: "jobs",
					Content:  runtime.RawExtension{Raw: []byte(`{"apiVersion":"batch/v1","kind":"Job","metadata":{"name":"e2e-job-abc123","namespace":"e2e-actions"},"spec":{"template":{"spec":{"serviceAccountName":"e2e-runner","containers":[{"name":"runner","image":"registry.example.com/e2e-runner:latest"}],"restartPolicy":"Never"}}}}`)},
					Watch:    true,
				},
			},
		},
	}
}
