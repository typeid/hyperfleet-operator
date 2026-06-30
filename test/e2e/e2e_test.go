//go:build e2e

package e2e

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	hyperfleetv1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
	dynamo "github.com/typeid/hyperfleet-operator/internal/dynamo"
)

var _ = Describe("Cross-component interaction", func() {
	const testNS = "111222333444"

	AfterEach(func() {
		c := &hyperfleetv1alpha1.Cluster{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "e2e-sep-test"}, c); err == nil {
			controllerutil.RemoveFinalizer(c, "hyperfleet.io/cluster")
			_ = k8sClient.Update(ctx, c)
			_ = k8sClient.Delete(ctx, c)
		}
		p := &hyperfleetv1alpha1.Placement{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "e2e-sep-test-placement"}, p); err == nil {
			_ = k8sClient.Delete(ctx, p)
		}
		h := &hyperfleetv1alpha1.Manifest{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "e2e-monitoring"}, h); err == nil {
			controllerutil.RemoveFinalizer(h, "hyperfleet.io/manifest")
			_ = k8sClient.Update(ctx, h)
			_ = k8sClient.Delete(ctx, h)
		}
	})

	It("should keep manifest and cluster desires separate", func() {
		By("creating a Cluster CR")
		cluster := newE2ECluster("e2e-sep-test")
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

		By("creating a Manifest CR on the same MC")
		hfm := newE2EManifest("e2e-monitoring")
		Expect(k8sClient.Create(ctx, hfm)).To(Succeed())

		By("waiting for both to write desires")
		specsTable := mc + "-specs-applydesires"
		Eventually(func(g Gomega) {
			items := scanTable(specsTable)
			hasClusterNS := false
			hasManifestSA := false
			for _, item := range items {
				resource := attrString(item, "spec", "targetItem", "resource")
				name := attrString(item, "spec", "targetItem", "name")
				if resource == "namespaces" && name == "clusters-e2e-sep-test" {
					hasClusterNS = true
				}
				if resource == "serviceaccounts" && name == "e2e-runner" {
					hasManifestSA = true
				}
			}
			g.Expect(hasClusterNS).To(BeTrue(), "cluster namespace desire missing")
			g.Expect(hasManifestSA).To(BeTrue(), "manifest SA desire missing")
		}).Should(Succeed())

		By("verifying document IDs don't collide")
		clusterDocID := dynamo.NewDocumentID("hyperfleet-operator", "", "v1", "namespaces", "", "clusters-e2e-sep-test")
		manifestDocID := dynamo.NewDocumentID("hyperfleet-manifest/"+testNS+"/e2e-monitoring", "", "v1", "serviceaccounts", "e2e-actions", "e2e-runner")
		Expect(clusterDocID).NotTo(Equal(manifestDocID))

		items := scanTable(specsTable)
		docIDs := map[string]bool{}
		for _, item := range items {
			docIDs[attrString(item, "documentID")] = true
		}
		Expect(docIDs).To(HaveKey(clusterDocID))
		Expect(docIDs).To(HaveKey(manifestDocID))
	})
})
