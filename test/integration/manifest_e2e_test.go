//go:build integration

package e2e

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"

	hyperfleetv1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
	"github.com/typeid/hyperfleet-operator/internal/controller"
	dynamo "github.com/typeid/hyperfleet-operator/internal/dynamo"
)

var _ = Describe("Manifest lifecycle", func() {
	const (
		manifestName = "e2e-monitoring"
		testNS       = "111222333444"
	)

	AfterEach(func() {
		purgeFleetstore()
		purgeDynamoTables()
		dynamoCli.ResetCache()
	})

	It("should write ApplyDesires to DynamoDB when a Manifest is created", func() {
		By("creating a Manifest CR")
		hfm := newE2EManifest(manifestName)
		Expect(k8sClient.Create(ctx, hfm)).To(Succeed())

		By("waiting for ApplyDesires to appear in DynamoDB")
		specsTable := mc + "-specs-applydesires"
		Eventually(func(g Gomega) {
			items := scanTable(specsTable)
			resources := map[string]bool{}
			for _, item := range items {
				resource := attrString(item, "spec", "targetItem", "resource")
				name := attrString(item, "spec", "targetItem", "name")
				resources[resource+"/"+name] = true
			}
			g.Expect(resources).To(HaveKey("serviceaccounts/e2e-runner"))
			g.Expect(resources).To(HaveKey("roles/e2e-runner"))
			g.Expect(resources).To(HaveKey("rolebindings/e2e-runner"))
			g.Expect(resources).To(HaveKey("jobs/e2e-job-abc123"))
		}).Should(Succeed())

		By("verifying KubeContent is the raw JSON")
		items := scanTable(specsTable)
		for _, item := range items {
			if attrString(item, "spec", "targetItem", "resource") == "jobs" {
				content := attrString(item, "spec_kubeContent")
				Expect(content).To(ContainSubstring("e2e-job-abc123"))
				Expect(content).To(ContainSubstring("e2e-runner"))
			}
		}

		By("verifying document IDs use scoped taskKey")
		expectedDocID := dynamo.NewDocumentID(
			"hyperfleet-manifest/"+testNS+"/"+manifestName,
			"", "v1", "serviceaccounts", "e2e-actions", "e2e-runner",
		)
		found := false
		for _, item := range items {
			if attrString(item, "documentID") == expectedDocID {
				found = true
				break
			}
		}
		Expect(found).To(BeTrue(), "namespace desire should have scoped document ID %s", expectedDocID)

		By("verifying status and Synced condition")
		Eventually(func(g Gomega) {
			var updated hyperfleetv1alpha1.Manifest
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: manifestName}, &updated)).To(Succeed())
			g.Expect(updated.Status.Phase).To(Equal(hyperfleetv1alpha1.ManifestPhaseApplied))
			g.Expect(updated.Status.AppliedResources).To(Equal(int32(4)))
			synced := meta.FindStatusCondition(updated.Status.Conditions, controller.ConditionSynced)
			g.Expect(synced).NotTo(BeNil())
			g.Expect(synced.Status).To(Equal(metav1.ConditionTrue))
			g.Expect(synced.Reason).To(Equal("AllSynced"))
		}).Should(Succeed())
	})

	It("should update ApplyDesires when content changes", func() {
		By("creating a Manifest CR")
		hfm := newE2EManifest(manifestName)
		Expect(k8sClient.Create(ctx, hfm)).To(Succeed())

		By("waiting for initial reconcile to complete")
		specsTable := mc + "-specs-applydesires"
		Eventually(func(g Gomega) {
			var h hyperfleetv1alpha1.Manifest
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: manifestName}, &h)).To(Succeed())
			g.Expect(h.Status.Phase).To(Equal(hyperfleetv1alpha1.ManifestPhaseApplied))
		}).Should(Succeed())

		By("updating Job image")
		Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
			var toUpdate hyperfleetv1alpha1.Manifest
			if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: manifestName}, &toUpdate); err != nil {
				return err
			}
			toUpdate.Spec.Resources[3].Content = runtime.RawExtension{
				Raw: []byte(`{"apiVersion":"batch/v1","kind":"Job","metadata":{"name":"e2e-job-abc123","namespace":"e2e-actions"},"spec":{"template":{"spec":{"serviceAccountName":"e2e-runner","containers":[{"name":"runner","image":"registry.example.com/e2e-runner:v2"}],"restartPolicy":"Never"}}}}`),
			}
			return k8sClient.Update(ctx, &toUpdate)
		})).To(Succeed())

		By("verifying updated content in DynamoDB")
		Eventually(func(g Gomega) {
			items := scanTable(specsTable)
			for _, item := range items {
				if attrString(item, "spec", "targetItem", "name") == "e2e-job-abc123" {
					content := attrString(item, "spec_kubeContent")
					g.Expect(content).To(ContainSubstring("e2e-runner:v2"))
					return
				}
			}
			g.Expect(false).To(BeTrue(), "updated job desire not found")
		}).Should(Succeed())
	})

	It("should write DeleteDesires and clean up when Manifest is deleted", func() {
		By("creating a Manifest CR")
		hfm := newE2EManifest(manifestName)
		Expect(k8sClient.Create(ctx, hfm)).To(Succeed())

		By("waiting for Applied status")
		Eventually(func(g Gomega) {
			var h hyperfleetv1alpha1.Manifest
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: manifestName}, &h)).To(Succeed())
			g.Expect(h.Status.Phase).To(Equal(hyperfleetv1alpha1.ManifestPhaseApplied))
		}).Should(Succeed())

		By("deleting the Manifest CR")
		var toDelete hyperfleetv1alpha1.Manifest
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: manifestName}, &toDelete)).To(Succeed())
		Expect(k8sClient.Delete(ctx, &toDelete)).To(Succeed())

		By("verifying ApplyDesire specs are cleaned up from DynamoDB")
		specsApply := mc + "-specs-applydesires"
		Eventually(func(g Gomega) {
			items := scanTable(specsApply)
			g.Expect(items).To(BeEmpty(), "all ApplyDesire specs should be cleaned up on deletion")
		}).Should(Succeed())

		By("verifying DeleteDesires appear in DynamoDB")
		specsDelete := mc + "-specs-deletedesires"
		Eventually(func(g Gomega) {
			items := scanTable(specsDelete)
			resources := map[string]bool{}
			for _, item := range items {
				resources[attrString(item, "spec", "targetItem", "resource")] = true
			}
			g.Expect(resources).To(HaveKey("serviceaccounts"), "expected SA DeleteDesire")
			g.Expect(resources).To(HaveKey("roles"), "expected Role DeleteDesire")
			g.Expect(resources).To(HaveKey("rolebindings"), "expected RoleBinding DeleteDesire")
			g.Expect(resources).To(HaveKey("jobs"), "expected Job DeleteDesire")
		}).Should(Succeed())

		By("verifying Manifest CR is fully gone")
		Eventually(func() error {
			return k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: manifestName}, &hyperfleetv1alpha1.Manifest{})
		}).ShouldNot(Succeed())
	})

	It("should not collide when two Manifests deploy the same resource", func() {
		By("creating two Manifest CRs deploying the same namespace")
		hfmA := newE2EManifest(manifestName)
		hfmB := newE2EManifest("e2e-monitoring-b")
		Expect(k8sClient.Create(ctx, hfmA)).To(Succeed())
		Expect(k8sClient.Create(ctx, hfmB)).To(Succeed())

		By("waiting for both to be Applied")
		for _, name := range []string{manifestName, "e2e-monitoring-b"} {
			Eventually(func(g Gomega) {
				var h hyperfleetv1alpha1.Manifest
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, &h)).To(Succeed())
				g.Expect(h.Status.Phase).To(Equal(hyperfleetv1alpha1.ManifestPhaseApplied))
			}).Should(Succeed())
		}

		By("verifying 8 distinct ApplyDesires exist (4 per CR)")
		specsTable := mc + "-specs-applydesires"
		items := scanTable(specsTable)
		saDesireIDs := []string{}
		for _, item := range items {
			if attrString(item, "spec", "targetItem", "resource") == "serviceaccounts" {
				saDesireIDs = append(saDesireIDs, attrString(item, "documentID"))
			}
		}
		Expect(saDesireIDs).To(HaveLen(2), "expected 2 SA ApplyDesires, one per CR")
		Expect(saDesireIDs[0]).NotTo(Equal(saDesireIDs[1]), "document IDs must differ between CRs")

		By("deleting only hfm-a and verifying hfm-b's desires survive")
		var toDelete hyperfleetv1alpha1.Manifest
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: manifestName}, &toDelete)).To(Succeed())
		Expect(k8sClient.Delete(ctx, &toDelete)).To(Succeed())

		Eventually(func() error {
			return k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: manifestName}, &hyperfleetv1alpha1.Manifest{})
		}).ShouldNot(Succeed())

		expectedB := dynamo.NewDocumentID(
			"hyperfleet-manifest/"+testNS+"/e2e-monitoring-b",
			"", "v1", "serviceaccounts", "e2e-actions", "e2e-runner",
		)
		items = scanTable(specsTable)
		found := false
		for _, item := range items {
			if attrString(item, "documentID") == expectedB {
				found = true
				break
			}
		}
		Expect(found).To(BeTrue(), "hfm-b's namespace desire should still exist")
	})

	It("should write ReadDesires for watched resources and populate resourceStatuses", func() {
		By("creating a Manifest with a watched Job")
		hfm := newE2EManifest(manifestName)
		Expect(k8sClient.Create(ctx, hfm)).To(Succeed())

		By("verifying ReadDesire appears in DynamoDB")
		readSpecs := mc + "-specs-readdesires"
		Eventually(func(g Gomega) {
			items := scanTable(readSpecs)
			g.Expect(len(items)).To(BeNumerically(">=", 1))
			found := false
			for _, item := range items {
				if attrString(item, "spec", "targetItem", "resource") == "jobs" &&
					attrString(item, "spec", "targetItem", "name") == "e2e-job-abc123" {
					found = true
				}
			}
			g.Expect(found).To(BeTrue(), "ReadDesire for Job should exist")
		}).Should(Succeed())

		By("verifying resourceStatuses is populated with mirrored status")
		Eventually(func(g Gomega) {
			var updated hyperfleetv1alpha1.Manifest
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: manifestName}, &updated)).To(Succeed())
			g.Expect(updated.Status.ResourceStatuses).To(HaveLen(1))
			g.Expect(updated.Status.ResourceStatuses[0].Resource).To(Equal("jobs"))
			g.Expect(updated.Status.ResourceStatuses[0].Name).To(Equal("e2e-job-abc123"))
			g.Expect(string(updated.Status.ResourceStatuses[0].Status.Raw)).To(ContainSubstring(`"succeeded":1`))
		}).Should(Succeed())
	})
})
