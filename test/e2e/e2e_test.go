//go:build e2e

package e2e

import (
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	hyperfleetv1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
	dynamo "github.com/typeid/hyperfleet-operator/internal/dynamo"
)

var _ = Describe("Cluster lifecycle", func() {
	const (
		clusterName = "e2e-test-01"
		testNS      = "111222333444"
	)

	AfterEach(func() {
		// Clean up: remove finalizers and delete CRs.
		np := &hyperfleetv1alpha1.NodePool{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "e2e-nodepool"}, np); err == nil {
			controllerutil.RemoveFinalizer(np, "hyperfleet.io/operator")
			_ = k8sClient.Update(ctx, np)
			_ = k8sClient.Delete(ctx, np)
		}
		cluster := &hyperfleetv1alpha1.Cluster{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: clusterName}, cluster); err == nil {
			controllerutil.RemoveFinalizer(cluster, "hyperfleet.io/operator")
			_ = k8sClient.Update(ctx, cluster)
			_ = k8sClient.Delete(ctx, cluster)
		}
		placement := &hyperfleetv1alpha1.Placement{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: clusterName + "-placement"}, placement); err == nil {
			_ = k8sClient.Delete(ctx, placement)
		}
	})

	It("should write correct ApplyDesires to DynamoDB when a Cluster is created", func() {
		By("creating a Cluster CR")
		cluster := newE2ECluster(clusterName)
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

		By("waiting for Placement to be created and Bound")
		Eventually(func(g Gomega) {
			var p hyperfleetv1alpha1.Placement
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: clusterName + "-placement"}, &p)).To(Succeed())
			g.Expect(p.Status.Phase).To(Equal("Bound"))
			g.Expect(p.Spec.ManagementCluster).To(Equal("mc01"))
		}).Should(Succeed())

		By("waiting for ApplyDesires to appear in DynamoDB")
		specsTable := mc + "-specs-applydesires"
		Eventually(func(g Gomega) {
			items := scanTable(specsTable)
			g.Expect(len(items)).To(BeNumerically(">=", 7), "expected at least 7 ApplyDesires, got %d", len(items))
		}).Should(Succeed())

		By("verifying the 7 expected resources are present")
		items := scanTable(specsTable)
		resourceNames := map[string]bool{}
		for _, item := range items {
			name := attrString(item, "targetItem", "name")
			resource := attrString(item, "targetItem", "resource")
			resourceNames[resource+"/"+name] = true
		}

		expectedResources := []string{
			"namespaces/clusters-" + clusterName,
			"configmaps/cluster-config",
			"configmaps/aws-iam-auth-config",
			"externalsecrets/pull-secret",
			"certificates/api-serving-cert",
			"hostedclusters/my-e2e-cluster",
			"secrets/ssh-key",
		}
		for _, expected := range expectedResources {
			Expect(resourceNames).To(HaveKey(expected), "missing resource: %s", expected)
		}

		By("verifying HostedCluster content in DynamoDB")
		var hcContent map[string]any
		for _, item := range items {
			resource := attrString(item, "targetItem", "resource")
			if resource == "hostedclusters" {
				raw := attrStringDirect(item, "spec_kubeContent")
				Expect(raw).NotTo(BeEmpty(), "kubeContent should not be empty")
				Expect(json.Unmarshal([]byte(raw), &hcContent)).To(Succeed())
				break
			}
		}
		Expect(hcContent).NotTo(BeNil(), "HostedCluster not found in DynamoDB")

		spec := hcContent["spec"].(map[string]any)
		Expect(spec["issuerURL"]).To(Equal("https://oidc.e2e.example.com/e2e-test-01"))
		Expect(spec["infraID"]).To(Equal(clusterName))

		dns := spec["dns"].(map[string]any)
		Expect(dns["baseDomain"]).To(Equal("e2e-.e2e.example.com"))

		By("verifying ReadDesire for HostedCluster status feedback")
		readTable := mc + "-specs-readdesires"
		Eventually(func(g Gomega) {
			readItems := scanTable(readTable)
			g.Expect(len(readItems)).To(BeNumerically(">=", 1))
			resource := attrString(readItems[0], "targetItem", "resource")
			g.Expect(resource).To(Equal("hostedclusters"))
		}).Should(Succeed())

		By("verifying document IDs are deterministic")
		nsDocID := dynamo.NewDocumentID("hyperfleet-operator", "", "v1", "namespaces", "", "clusters-"+clusterName)
		found := false
		for _, item := range items {
			if docID, ok := item["documentID"]; ok {
				if sv, ok := docID.(*dynamodbtypes.AttributeValueMemberS); ok && sv.Value == nsDocID {
					found = true
					break
				}
			}
		}
		Expect(found).To(BeTrue(), "namespace desire should have deterministic document ID %s", nsDocID)
	})

	It("should propagate HostedCluster status from ReadDesire to Cluster CR", func() {
		By("creating a Cluster CR")
		cluster := newE2ECluster(clusterName)
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

		By("waiting for ReadDesire to appear in DynamoDB")
		readTable := mc + "-specs-readdesires"
		var readDocID string
		Eventually(func(g Gomega) {
			items := scanTable(readTable)
			g.Expect(len(items)).To(BeNumerically(">=", 1))
			readDocID = attrStringDirect(items[0], "documentID")
			g.Expect(readDocID).NotTo(BeEmpty())
		}).Should(Succeed())

		By("simulating kube-applier-aws writing HostedCluster status to status-readdesires")
		hcStatus := map[string]any{
			"status": map[string]any{
				"conditions": []map[string]any{
					{
						"type":               "Available",
						"status":             "True",
						"reason":             "HostedClusterAsExpected",
						"message":            "The hosted cluster is available",
						"lastTransitionTime": "2026-06-24T00:00:00Z",
					},
				},
				"controlPlaneEndpoint": map[string]any{
					"host": "api.e2e-test.example.com",
				},
				"version": map[string]any{
					"history": []map[string]any{
						{"version": "4.17.3"},
					},
				},
			},
		}
		hcJSON, err := json.Marshal(hcStatus)
		Expect(err).NotTo(HaveOccurred())

		statusTable := mc + "-status-readdesires"
		_, err = dynamoDBCli.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(statusTable),
			Item: map[string]dynamodbtypes.AttributeValue{
				"documentID":  &dynamodbtypes.AttributeValueMemberS{Value: readDocID},
				"kubeContent": &dynamodbtypes.AttributeValueMemberB{Value: hcJSON},
			},
		})
		Expect(err).NotTo(HaveOccurred())

		By("verifying Cluster CR status is updated with HostedCluster data")
		Eventually(func(g Gomega) {
			var c hyperfleetv1alpha1.Cluster
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: clusterName}, &c)).To(Succeed())
			g.Expect(c.Status.ControlPlaneEndpoint).To(Equal("api.e2e-test.example.com"))
			g.Expect(c.Status.Version).To(Equal("4.17.3"))
			g.Expect(c.Status.Phase).To(Equal("Ready"))
		}).Should(Succeed())
	})

	It("should write NodePool ApplyDesire when NodePool CR is created", func() {
		By("creating a Cluster CR with PlacementRef")
		cluster := newE2ECluster(clusterName)
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

		Eventually(func(g Gomega) {
			var c hyperfleetv1alpha1.Cluster
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: clusterName}, &c)).To(Succeed())
			g.Expect(c.Status.PlacementRef).NotTo(BeNil())
		}).Should(Succeed())

		By("creating a NodePool CR")
		np := newE2ENodePool(clusterName)
		Expect(k8sClient.Create(ctx, np)).To(Succeed())

		By("waiting for NodePool ApplyDesire in DynamoDB")
		specsTable := mc + "-specs-applydesires"
		Eventually(func(g Gomega) {
			items := scanTable(specsTable)
			for _, item := range items {
				resource := attrString(item, "targetItem", "resource")
				if resource == "nodepools" {
					name := attrString(item, "targetItem", "name")
					g.Expect(name).To(Equal("my-e2e-cluster-e2e-nodepool"))
					return
				}
			}
			g.Expect(false).To(BeTrue(), "nodepool desire not found")
		}).Should(Succeed())
	})

	It("should cascade delete NodePools, write DeleteDesire, and remove Placement when Cluster is deleted", func() {
		By("creating a Cluster CR")
		cluster := newE2ECluster(clusterName)
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

		By("waiting for PlacementRef to be set")
		Eventually(func(g Gomega) {
			var c hyperfleetv1alpha1.Cluster
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: clusterName}, &c)).To(Succeed())
			g.Expect(c.Status.PlacementRef).NotTo(BeNil())
		}).Should(Succeed())

		By("creating a NodePool CR")
		np := newE2ENodePool(clusterName)
		Expect(k8sClient.Create(ctx, np)).To(Succeed())

		By("waiting for NodePool ApplyDesire to confirm both CRs are reconciled")
		specsApply := mc + "-specs-applydesires"
		Eventually(func(g Gomega) {
			items := scanTable(specsApply)
			for _, item := range items {
				if attrString(item, "targetItem", "resource") == "nodepools" {
					return
				}
			}
			g.Expect(false).To(BeTrue(), "nodepool desire not found yet")
		}).Should(Succeed())

		By("deleting the Cluster CR")
		var toDelete hyperfleetv1alpha1.Cluster
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: clusterName}, &toDelete)).To(Succeed())
		Expect(k8sClient.Delete(ctx, &toDelete)).To(Succeed())

		By("verifying NodePool CR is deleted")
		Eventually(func() error {
			return k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "e2e-nodepool"}, &hyperfleetv1alpha1.NodePool{})
		}).ShouldNot(Succeed())

		By("verifying DeleteDesires appear in DynamoDB")
		specsDelete := mc + "-specs-deletedesires"
		Eventually(func(g Gomega) {
			items := scanTable(specsDelete)
			resources := map[string]bool{}
			for _, item := range items {
				resources[attrString(item, "targetItem", "resource")] = true
			}
			g.Expect(resources).To(HaveKey("namespaces"), "expected namespace DeleteDesire")
			g.Expect(resources).To(HaveKey("nodepools"), "expected nodepool DeleteDesire")
		}).Should(Succeed())

		By("verifying Placement CR is deleted")
		Eventually(func() error {
			return k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: clusterName + "-placement"}, &hyperfleetv1alpha1.Placement{})
		}).ShouldNot(Succeed())

		By("verifying Cluster CR is fully gone")
		Eventually(func() error {
			return k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: clusterName}, &hyperfleetv1alpha1.Cluster{})
		}).ShouldNot(Succeed())
	})

	It("should write NodePool DeleteDesire when only the NodePool is deleted", func() {
		By("creating a Cluster CR")
		cluster := newE2ECluster(clusterName)
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

		By("waiting for PlacementRef")
		Eventually(func(g Gomega) {
			var c hyperfleetv1alpha1.Cluster
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: clusterName}, &c)).To(Succeed())
			g.Expect(c.Status.PlacementRef).NotTo(BeNil())
		}).Should(Succeed())

		By("creating a NodePool CR")
		np := newE2ENodePool(clusterName)
		Expect(k8sClient.Create(ctx, np)).To(Succeed())

		By("waiting for NodePool ApplyDesire in DynamoDB")
		specsApply := mc + "-specs-applydesires"
		Eventually(func(g Gomega) {
			items := scanTable(specsApply)
			for _, item := range items {
				if attrString(item, "targetItem", "resource") == "nodepools" {
					return
				}
			}
			g.Expect(false).To(BeTrue(), "nodepool desire not found")
		}).Should(Succeed())

		By("deleting only the NodePool CR")
		var npToDelete hyperfleetv1alpha1.NodePool
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "e2e-nodepool"}, &npToDelete)).To(Succeed())
		Expect(k8sClient.Delete(ctx, &npToDelete)).To(Succeed())

		By("verifying NodePool DeleteDesire appears in DynamoDB")
		specsDelete := mc + "-specs-deletedesires"
		Eventually(func(g Gomega) {
			items := scanTable(specsDelete)
			found := false
			for _, item := range items {
				if attrString(item, "targetItem", "resource") == "nodepools" {
					found = true
					break
				}
			}
			g.Expect(found).To(BeTrue(), "nodepool DeleteDesire not found")
		}).Should(Succeed())

		By("verifying NodePool CR is fully gone")
		Eventually(func() error {
			return k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "e2e-nodepool"}, &hyperfleetv1alpha1.NodePool{})
		}).ShouldNot(Succeed())

		By("verifying Cluster and Placement are still alive")
		var c hyperfleetv1alpha1.Cluster
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: clusterName}, &c)).To(Succeed())
		var p hyperfleetv1alpha1.Placement
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: clusterName + "-placement"}, &p)).To(Succeed())
	})
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

func attrStringDirect(item map[string]dynamodbtypes.AttributeValue, key string) string {
	av, ok := item[key]
	if !ok {
		return ""
	}
	if sv, ok := av.(*dynamodbtypes.AttributeValueMemberS); ok {
		return sv.Value
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

var _ = BeforeEach(func() {
	purgeDynamoTables()
})

func newE2ECluster(name string) *hyperfleetv1alpha1.Cluster {
	return &hyperfleetv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "111222333444"},
		Spec: hyperfleetv1alpha1.ClusterSpec{
			Name:                      "my-e2e-cluster",
			AccountID:                 "111222333444",
			Region:                    "us-east-1",
			Zone:                      "us-east-1a",
			BaseDomain:                "e2e.example.com",
			VpcID:                     "vpc-e2e0001",
			PrivateSubnetIds:          "subnet-e2e0001",
			WorkerInstanceProfileName: "worker-profile",
			WorkerSecurityGroupId:     "sg-e2e0001",
			OIDCIssuerURL:             "https://oidc.e2e.example.com/" + name,
			Release:                   hyperfleetv1alpha1.ReleaseSpec{Image: "quay.io/ocp:4.17"},
			CreatorARN:                "arn:aws:iam::111222333444:user/e2etester",
			Networking: hyperfleetv1alpha1.NetworkingSpec{
				ClusterNetwork: []hyperfleetv1alpha1.NetworkEntry{{CIDR: "10.128.0.0/14"}},
				ServiceNetwork: []hyperfleetv1alpha1.NetworkEntry{{CIDR: "172.30.0.0/16"}},
				MachineNetwork: []hyperfleetv1alpha1.NetworkEntry{{CIDR: "10.0.0.0/16"}},
			},
			Platform: hyperfleetv1alpha1.PlatformSpec{
				AWS: hyperfleetv1alpha1.AWSPlatformSpec{
					Roles: hyperfleetv1alpha1.AWSRolesSpec{
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
			Management: hyperfleetv1alpha1.NodePoolManagementSpec{
				AutoRepair:  true,
				UpgradeType: "Replace",
			},
			Release: hyperfleetv1alpha1.ReleaseSpec{Image: "quay.io/ocp:4.17"},
			Platform: hyperfleetv1alpha1.NodePoolPlatformSpec{
				AWS: hyperfleetv1alpha1.AWSNodePoolSpec{
					InstanceType:    "m6a.xlarge",
					RootVolume:      hyperfleetv1alpha1.RootVolumeSpec{Size: 120, Type: "gp3"},
					SubnetId:        "subnet-e2e0001",
					InstanceProfile: "worker-profile",
					SecurityGroups:  []string{"sg-e2e0001"},
				},
			},
		},
	}
}
