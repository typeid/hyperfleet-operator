package integration

import (
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"

	hyperfleetv1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
	dynamo "github.com/typeid/hyperfleet-operator/internal/dynamo"
)

var _ = Describe("Cluster lifecycle", func() {
	const (
		clusterName = "e2e-test-01"
		testNS      = "e2e-cluster-id"
	)

	AfterEach(func() {
		purgeResources()
		purgeDynamoTables()
		dynamoCli.ResetCache()
	})

	It("should write correct ApplyDesires to DynamoDB when a Cluster is created", func() {
		By("creating a Cluster CR")
		cluster := newTestCluster(clusterName)
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

		By("waiting for Placement to be created and Bound")
		Eventually(func(g Gomega) {
			var p hyperfleetv1alpha1.Placement
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: clusterName + "-placement"}, &p)).To(Succeed())
			g.Expect(p.Status.Phase).To(Equal(hyperfleetv1alpha1.PlacementPhaseBound))
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
			name := attrString(item, "spec", "targetItem", "name")
			resource := attrString(item, "spec", "targetItem", "resource")
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
			resource := attrString(item, "spec", "targetItem", "resource")
			if resource == "hostedclusters" {
				raw := attrString(item, "spec_kubeContent")
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
		Expect(dns["baseDomain"]).To(Equal("e2e-.0.e2e.example.com"))

		By("verifying ReadDesire for HostedCluster status feedback")
		readTable := mc + "-specs-readdesires"
		Eventually(func(g Gomega) {
			readItems := scanTable(readTable)
			g.Expect(len(readItems)).To(BeNumerically(">=", 1))
			resource := attrString(readItems[0], "spec", "targetItem", "resource")
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
		cluster := newTestCluster(clusterName)
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

		By("waiting for ReadDesire to appear in DynamoDB")
		readTable := mc + "-specs-readdesires"
		var readDocID string
		Eventually(func(g Gomega) {
			items := scanTable(readTable)
			g.Expect(len(items)).To(BeNumerically(">=", 1))
			readDocID = attrString(items[0], "documentID")
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

		// DynamoDB Local streams are not reliable enough to deliver events
		// within test timeouts, so dispatch manually to trigger re-reconciliation.
		eventRouter.Dispatch(readDocID)

		By("verifying Cluster CR status is updated with HostedCluster data")
		Eventually(func(g Gomega) {
			var c hyperfleetv1alpha1.Cluster
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: clusterName}, &c)).To(Succeed())
			g.Expect(c.Status.ControlPlaneEndpoint.Host).To(Equal("api.e2e-test.example.com"))
			g.Expect(c.Status.Version).To(Equal("4.17.3"))
			g.Expect(c.Status.Phase).To(Equal(hyperfleetv1alpha1.ClusterPhaseReady))
		}).Should(Succeed())
	})

	It("should cascade delete NodePools, write DeleteDesire, and remove Placement when Cluster is deleted", func() {
		By("creating a Cluster CR")
		cluster := newTestCluster(clusterName)
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

		By("waiting for PlacementRef to be set")
		Eventually(func(g Gomega) {
			var c hyperfleetv1alpha1.Cluster
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: clusterName}, &c)).To(Succeed())
			g.Expect(c.Status.PlacementRef).NotTo(BeNil())
		}).Should(Succeed())

		By("creating a NodePool CR")
		np := newTestNodePool(clusterName)
		Expect(k8sClient.Create(ctx, np)).To(Succeed())

		By("waiting for NodePool ApplyDesire to confirm both CRs are reconciled")
		specsApply := mc + "-specs-applydesires"
		Eventually(func(g Gomega) {
			items := scanTable(specsApply)
			for _, item := range items {
				if attrString(item, "spec", "targetItem", "resource") == "nodepools" {
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

		By("verifying DeleteDesires appear in DynamoDB (HostedCluster, namespace, nodepool)")
		specsDelete := mc + "-specs-deletedesires"
		Eventually(func(g Gomega) {
			items := scanTable(specsDelete)
			resources := map[string]bool{}
			for _, item := range items {
				resources[attrString(item, "spec", "targetItem", "resource")] = true
			}
			g.Expect(resources).To(HaveKey("hostedclusters"), "expected HostedCluster DeleteDesire")
			g.Expect(resources).To(HaveKey("namespaces"), "expected namespace DeleteDesire")
			g.Expect(resources).To(HaveKey("nodepools"), "expected nodepool DeleteDesire")
		}).Should(Succeed())

		By("verifying ApplyDesire specs are cleaned up from DynamoDB")
		Eventually(func(g Gomega) {
			items := scanTable(specsApply)
			g.Expect(items).To(BeEmpty(), "all ApplyDesire specs should be cleaned up on deletion")
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

	It("should write NodePool ApplyDesire when NodePool CR is created", func() {
		By("creating a Cluster CR with PlacementRef")
		cluster := newTestCluster(clusterName)
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

		Eventually(func(g Gomega) {
			var c hyperfleetv1alpha1.Cluster
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: clusterName}, &c)).To(Succeed())
			g.Expect(c.Status.PlacementRef).NotTo(BeNil())
		}).Should(Succeed())

		By("creating a NodePool CR")
		np := newTestNodePool(clusterName)
		Expect(k8sClient.Create(ctx, np)).To(Succeed())

		By("waiting for NodePool ApplyDesire in DynamoDB")
		specsTable := mc + "-specs-applydesires"
		Eventually(func(g Gomega) {
			items := scanTable(specsTable)
			for _, item := range items {
				resource := attrString(item, "spec", "targetItem", "resource")
				if resource == "nodepools" {
					name := attrString(item, "spec", "targetItem", "name")
					g.Expect(name).To(Equal("my-e2e-cluster-e2e-nodepool"))
					return
				}
			}
			g.Expect(false).To(BeTrue(), "nodepool desire not found")
		}).Should(Succeed())
	})

	It("should write NodePool DeleteDesire when only the NodePool is deleted", func() {
		By("creating a Cluster CR")
		cluster := newTestCluster(clusterName)
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

		By("waiting for PlacementRef")
		Eventually(func(g Gomega) {
			var c hyperfleetv1alpha1.Cluster
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: clusterName}, &c)).To(Succeed())
			g.Expect(c.Status.PlacementRef).NotTo(BeNil())
		}).Should(Succeed())

		By("creating a NodePool CR")
		np := newTestNodePool(clusterName)
		Expect(k8sClient.Create(ctx, np)).To(Succeed())

		By("waiting for NodePool ApplyDesire in DynamoDB")
		specsApply := mc + "-specs-applydesires"
		Eventually(func(g Gomega) {
			items := scanTable(specsApply)
			for _, item := range items {
				if attrString(item, "spec", "targetItem", "resource") == "nodepools" {
					return
				}
			}
			g.Expect(false).To(BeTrue(), "nodepool desire not found")
		}).Should(Succeed())

		By("deleting only the NodePool CR")
		var npToDelete hyperfleetv1alpha1.NodePool
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "e2e-nodepool"}, &npToDelete)).To(Succeed())
		Expect(k8sClient.Delete(ctx, &npToDelete)).To(Succeed())

		By("verifying NodePool ApplyDesire is cleaned up from DynamoDB")
		Eventually(func(g Gomega) {
			items := scanTable(specsApply)
			for _, item := range items {
				g.Expect(attrString(item, "spec", "targetItem", "resource")).NotTo(Equal("nodepools"),
					"nodepool ApplyDesire should be cleaned up on deletion")
			}
		}).Should(Succeed())

		By("verifying NodePool DeleteDesire appears in DynamoDB")
		specsDelete := mc + "-specs-deletedesires"
		Eventually(func(g Gomega) {
			items := scanTable(specsDelete)
			found := false
			for _, item := range items {
				if attrString(item, "spec", "targetItem", "resource") == "nodepools" {
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
