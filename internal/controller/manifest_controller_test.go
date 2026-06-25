/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hyperfleetv1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
	"github.com/typeid/hyperfleet-operator/internal/dynamo"
)

var _ = Describe("HyperFleetManifest Controller", func() {
	Context("When reconciling a HyperFleetManifest (ZOA deploy)", func() {
		const (
			manifestName = "test-monitoring"
			testNS       = "123456789012"
		)

		ctx := context.Background()

		BeforeEach(func() {
			ensureNamespace(ctx, testNS)
		})

		AfterEach(func() {
			resource := &hyperfleetv1alpha1.HyperFleetManifest{}
			err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: manifestName}, resource)
			if err == nil {
				controllerutil.RemoveFinalizer(resource, manifestFinalizer)
				_ = k8sClient.Update(ctx, resource)
				_ = k8sClient.Delete(ctx, resource)
			}
			// Clean up the second manifest used in the collision test.
			other := &hyperfleetv1alpha1.HyperFleetManifest{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "test-monitoring-b"}, other); err == nil {
				controllerutil.RemoveFinalizer(other, manifestFinalizer)
				_ = k8sClient.Update(ctx, other)
				_ = k8sClient.Delete(ctx, other)
			}
		})

		It("should add a finalizer on first reconcile", func() {
			resource := newTestManifest(manifestName)
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			reconciler := &HyperFleetManifestReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				Dynamo: &fakeDynamo{},
			}

			result, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: testNS, Name: manifestName},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeTrue())

			var updated hyperfleetv1alpha1.HyperFleetManifest
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: manifestName}, &updated)).To(Succeed())
			Expect(controllerutil.ContainsFinalizer(&updated, manifestFinalizer)).To(BeTrue())
		})

		It("should write ApplyDesires for each resource", func() {
			resource := newTestManifest(manifestName)
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			fd := &fakeDynamo{}
			reconciler := &HyperFleetManifestReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				Dynamo: fd,
			}

			// First reconcile: adds finalizer.
			_, _ = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: testNS, Name: manifestName},
			})
			// Second reconcile: writes desires.
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: testNS, Name: manifestName},
			})
			Expect(err).NotTo(HaveOccurred())

			// 4 resources → 4 ApplyDesires (SA, Role, RoleBinding, Job).
			Expect(fd.applyCount).To(Equal(4))

			Expect(fd.applies[0].Spec.TargetItem.Resource).To(Equal("serviceaccounts"))
			Expect(fd.applies[0].Spec.TargetItem.Name).To(Equal("zoa-runner"))
			Expect(fd.applies[0].Spec.TargetItem.Version).To(Equal("v1"))

			Expect(fd.applies[1].Spec.TargetItem.Resource).To(Equal("roles"))
			Expect(fd.applies[1].Spec.TargetItem.Name).To(Equal("zoa-runner"))
			Expect(fd.applies[1].Spec.TargetItem.Version).To(Equal("v1"))
			Expect(fd.applies[1].Spec.TargetItem.Group).To(Equal("rbac.authorization.k8s.io"))

			Expect(fd.applies[3].Spec.TargetItem.Resource).To(Equal("jobs"))
			Expect(fd.applies[3].Spec.TargetItem.Name).To(Equal("collect-logs-abc123"))
			Expect(fd.applies[3].Spec.TargetItem.Namespace).To(Equal("zoa-actions"))

			// Verify KubeContent is the raw JSON from spec.
			for _, a := range fd.applies {
				Expect(a.Spec.KubeContent).NotTo(BeEmpty())
			}
		})

		It("should set status after writing desires", func() {
			resource := newTestManifest(manifestName)
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			fd := &fakeDynamo{}
			reconciler := &HyperFleetManifestReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				Dynamo: fd,
			}

			// First reconcile: finalizer.
			_, _ = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: testNS, Name: manifestName},
			})
			// Second reconcile: desires + status.
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: testNS, Name: manifestName},
			})
			Expect(err).NotTo(HaveOccurred())

			var updated hyperfleetv1alpha1.HyperFleetManifest
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: manifestName}, &updated)).To(Succeed())
			Expect(updated.Status.Phase).To(Equal(hyperfleetv1alpha1.ManifestPhaseApplied))
			Expect(updated.Status.AppliedResources).To(Equal(int32(4)))
			Expect(meta.IsStatusConditionTrue(updated.Status.Conditions, "DesiresWritten")).To(BeTrue())
		})

		It("should scope document IDs to the CR identity", func() {
			// Two different HyperFleetManifest CRs deploying the same resource
			// must produce different document IDs to avoid DynamoDB overwrites.
			hfmA := newTestManifest(manifestName)
			hfmB := newTestManifest("test-monitoring-b")
			Expect(k8sClient.Create(ctx, hfmA)).To(Succeed())
			Expect(k8sClient.Create(ctx, hfmB)).To(Succeed())

			fdA := &fakeDynamo{}
			fdB := &fakeDynamo{}

			reconcilerA := &HyperFleetManifestReconciler{
				Client: k8sClient, Scheme: k8sClient.Scheme(), Dynamo: fdA,
			}
			reconcilerB := &HyperFleetManifestReconciler{
				Client: k8sClient, Scheme: k8sClient.Scheme(), Dynamo: fdB,
			}

			// Reconcile both (finalizer + desires).
			_, _ = reconcilerA.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: testNS, Name: manifestName},
			})
			_, _ = reconcilerA.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: testNS, Name: manifestName},
			})
			_, _ = reconcilerB.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: testNS, Name: "test-monitoring-b"},
			})
			_, _ = reconcilerB.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: testNS, Name: "test-monitoring-b"},
			})

			Expect(fdA.applyCount).To(Equal(4))
			Expect(fdB.applyCount).To(Equal(4))

			// The document IDs for the same resource (SA "zoa-runner") must differ between CRs.
			docIDA := fdA.applies[0].DynamoDBMetadata.DocumentID
			docIDB := fdB.applies[0].DynamoDBMetadata.DocumentID
			Expect(docIDA).NotTo(Equal(docIDB), "document IDs should differ between CRs")

			expectedA := dynamo.NewDocumentID("hyperfleet-manifest/"+testNS+"/"+manifestName, "", "v1", "serviceaccounts", "zoa-actions", "zoa-runner")
			expectedB := dynamo.NewDocumentID("hyperfleet-manifest/"+testNS+"/test-monitoring-b", "", "v1", "serviceaccounts", "zoa-actions", "zoa-runner")
			Expect(docIDA).To(Equal(expectedA))
			Expect(docIDB).To(Equal(expectedB))
		})

		It("should write DeleteDesires and requeue when waiting for confirmation", func() {
			resource := newTestManifest(manifestName)
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			fd := &fakeDynamo{}
			reconciler := &HyperFleetManifestReconciler{
				Client: k8sClient, Scheme: k8sClient.Scheme(), Dynamo: fd,
			}

			// Add finalizer.
			_, _ = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: testNS, Name: manifestName},
			})

			// Delete the CR.
			var toDelete hyperfleetv1alpha1.HyperFleetManifest
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: manifestName}, &toDelete)).To(Succeed())
			Expect(k8sClient.Delete(ctx, &toDelete)).To(Succeed())

			// Deletion reconcile: writes DeleteDesires but no confirmation → requeues.
			result, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: testNS, Name: manifestName},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(fd.deleteCount).To(Equal(4)) // All DeleteDesires written before checking status.
			Expect(fd.deletes[0].Spec.TargetItem.Resource).To(Equal("serviceaccounts"))
			Expect(fd.deletes[1].Spec.TargetItem.Resource).To(Equal("roles"))
			Expect(fd.deletes[2].Spec.TargetItem.Resource).To(Equal("rolebindings"))
			Expect(fd.deletes[3].Spec.TargetItem.Resource).To(Equal("jobs"))
			Expect(result.RequeueAfter).NotTo(BeZero(), "should requeue while waiting for confirmation")
		})

		It("should remove finalizer after all DeleteDesire confirmations", func() {
			resource := newTestManifest(manifestName)
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			fd := &fakeDynamo{}
			reconciler := &HyperFleetManifestReconciler{
				Client: k8sClient, Scheme: k8sClient.Scheme(), Dynamo: fd,
			}

			// Add finalizer.
			_, _ = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: testNS, Name: manifestName},
			})

			// Delete the CR.
			var toDelete hyperfleetv1alpha1.HyperFleetManifest
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: manifestName}, &toDelete)).To(Succeed())
			Expect(k8sClient.Delete(ctx, &toDelete)).To(Succeed())

			// Simulate all DeleteDesires confirmed.
			fd.deleteStatus = &dynamo.DeleteDesireStatus{}

			// Reconcile deletion with confirmation.
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: testNS, Name: manifestName},
			})
			Expect(err).NotTo(HaveOccurred())

			// Verify CR is gone (finalizer removed).
			err = k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: manifestName}, &hyperfleetv1alpha1.HyperFleetManifest{})
			Expect(err).To(HaveOccurred())

			// Verify ReadDesire specs were cleaned up for the watched Job.
			Expect(fd.deletedSpecs).To(HaveLen(1))
			Expect(fd.deletedSpecs[0]).To(ContainSubstring("-readdesires"))
		})

		It("should error when Content is missing apiVersion", func() {
			resource := &hyperfleetv1alpha1.HyperFleetManifest{
				ObjectMeta: metav1.ObjectMeta{Name: manifestName, Namespace: testNS},
				Spec: hyperfleetv1alpha1.HyperFleetManifestSpec{
					ManagementCluster: "mc01",
					Resources: []hyperfleetv1alpha1.ResourceTemplate{{
						Resource: "configmaps",
						Content:  runtime.RawExtension{Raw: []byte(`{"kind":"ConfigMap","metadata":{"name":"test"}}`)},
					}},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			fd := &fakeDynamo{}
			reconciler := &HyperFleetManifestReconciler{
				Client: k8sClient, Scheme: k8sClient.Scheme(), Dynamo: fd,
			}

			// First reconcile: adds finalizer.
			_, _ = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: testNS, Name: manifestName},
			})
			// Second reconcile: should fail on extractResourceMeta.
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: testNS, Name: manifestName},
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("content missing apiVersion"))
			Expect(fd.applyCount).To(Equal(0))
		})

		It("should error when Content is missing metadata.name", func() {
			resource := &hyperfleetv1alpha1.HyperFleetManifest{
				ObjectMeta: metav1.ObjectMeta{Name: manifestName, Namespace: testNS},
				Spec: hyperfleetv1alpha1.HyperFleetManifestSpec{
					ManagementCluster: "mc01",
					Resources: []hyperfleetv1alpha1.ResourceTemplate{{
						Resource: "configmaps",
						Content:  runtime.RawExtension{Raw: []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{}}`)},
					}},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			fd := &fakeDynamo{}
			reconciler := &HyperFleetManifestReconciler{
				Client: k8sClient, Scheme: k8sClient.Scheme(), Dynamo: fd,
			}

			_, _ = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: testNS, Name: manifestName},
			})
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: testNS, Name: manifestName},
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("content missing metadata.name"))
			Expect(fd.applyCount).To(Equal(0))
		})

		It("should write ReadDesires for watched resources", func() {
			resource := newTestManifest(manifestName)
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			fd := &fakeDynamo{}
			reconciler := &HyperFleetManifestReconciler{
				Client: k8sClient, Scheme: k8sClient.Scheme(), Dynamo: fd,
			}

			// Finalizer + desires.
			_, _ = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: testNS, Name: manifestName},
			})
			result, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: testNS, Name: manifestName},
			})
			Expect(err).NotTo(HaveOccurred())

			// Only the Job has watch: true → 1 ReadDesire.
			Expect(fd.readCount).To(Equal(1))
			Expect(fd.reads[0].Spec.TargetItem.Resource).To(Equal("jobs"))
			Expect(fd.reads[0].Spec.TargetItem.Name).To(Equal("collect-logs-abc123"))
			Expect(fd.reads[0].Spec.TargetItem.Namespace).To(Equal("zoa-actions"))
			Expect(fd.reads[0].Spec.TargetItem.Group).To(Equal("batch"))
			Expect(fd.reads[0].Spec.TargetItem.Version).To(Equal("v1"))

			// Should requeue for status polling.
			Expect(result.RequeueAfter).NotTo(BeZero(), "should requeue when watched resources exist")
		})

		It("should populate resourceStatuses from ReadDesire feedback", func() {
			resource := newTestManifest(manifestName)
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			kubeContent := []byte(`{"apiVersion":"batch/v1","kind":"Job","metadata":{"name":"collect-logs-abc123","namespace":"zoa-actions"},"status":{"succeeded":1,"startTime":"2026-06-25T10:00:00Z","completionTime":"2026-06-25T10:00:05Z"}}`)
			fd := &fakeDynamo{
				readStatus: &dynamo.ReadDesireStatus{
					KubeContent: kubeContent,
				},
			}
			reconciler := &HyperFleetManifestReconciler{
				Client: k8sClient, Scheme: k8sClient.Scheme(), Dynamo: fd,
			}

			// Finalizer + desires.
			_, _ = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: testNS, Name: manifestName},
			})
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: testNS, Name: manifestName},
			})
			Expect(err).NotTo(HaveOccurred())

			var updated hyperfleetv1alpha1.HyperFleetManifest
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: manifestName}, &updated)).To(Succeed())
			Expect(updated.Status.ResourceStatuses).To(HaveLen(1))
			Expect(updated.Status.ResourceStatuses[0].Resource).To(Equal("jobs"))
			Expect(updated.Status.ResourceStatuses[0].Name).To(Equal("collect-logs-abc123"))
			Expect(updated.Status.ResourceStatuses[0].Namespace).To(Equal("zoa-actions"))
			Expect(updated.Status.ResourceStatuses[0].Status.Raw).To(MatchJSON(`{"succeeded":1,"startTime":"2026-06-25T10:00:00Z","completionTime":"2026-06-25T10:00:05Z"}`))
		})

		It("should not write ReadDesires for unwatched resources", func() {
			resource := newTestManifestUnwatched(manifestName)
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			fd := &fakeDynamo{}
			reconciler := &HyperFleetManifestReconciler{
				Client: k8sClient, Scheme: k8sClient.Scheme(), Dynamo: fd,
			}

			_, _ = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: testNS, Name: manifestName},
			})
			result, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: testNS, Name: manifestName},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(fd.readCount).To(Equal(0))
			Expect(result.RequeueAfter).To(BeZero(), "should not requeue without watched resources")
		})

		It("should handle not-found gracefully", func() {
			reconciler := &HyperFleetManifestReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				Dynamo: &fakeDynamo{},
			}

			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: testNS, Name: "does-not-exist"},
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})
})

// newTestManifest models a ZOA trusted action: deploy a ServiceAccount, Role,
// RoleBinding, and a runner Job to an MC. The Job is watched so the platform-api
// can detect completion via status.resourceStatuses.
func newTestManifest(name string) *hyperfleetv1alpha1.HyperFleetManifest {
	return &hyperfleetv1alpha1.HyperFleetManifest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "123456789012",
		},
		Spec: hyperfleetv1alpha1.HyperFleetManifestSpec{
			ManagementCluster: "mc01",
			Resources: []hyperfleetv1alpha1.ResourceTemplate{
				{
					Resource: "serviceaccounts",
					Content:  runtime.RawExtension{Raw: []byte(`{"apiVersion":"v1","kind":"ServiceAccount","metadata":{"name":"zoa-runner","namespace":"zoa-actions"}}`)},
				},
				{
					Resource: "roles",
					Content:  runtime.RawExtension{Raw: []byte(`{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"Role","metadata":{"name":"zoa-runner","namespace":"zoa-actions"},"rules":[{"apiGroups":[""],"resources":["pods/log"],"verbs":["get"]}]}`)},
				},
				{
					Resource: "rolebindings",
					Content:  runtime.RawExtension{Raw: []byte(`{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"RoleBinding","metadata":{"name":"zoa-runner","namespace":"zoa-actions"},"roleRef":{"apiGroup":"rbac.authorization.k8s.io","kind":"Role","name":"zoa-runner"},"subjects":[{"kind":"ServiceAccount","name":"zoa-runner","namespace":"zoa-actions"}]}`)},
				},
				{
					Resource: "jobs",
					Content:  runtime.RawExtension{Raw: []byte(`{"apiVersion":"batch/v1","kind":"Job","metadata":{"name":"collect-logs-abc123","namespace":"zoa-actions"},"spec":{"template":{"spec":{"serviceAccountName":"zoa-runner","containers":[{"name":"runner","image":"registry.example.com/zoa-runner:latest"}],"restartPolicy":"Never"}}}}`)},
					Watch:    true,
				},
			},
		},
	}
}

// newTestManifestUnwatched models a pure infrastructure deploy (no status feedback).
func newTestManifestUnwatched(name string) *hyperfleetv1alpha1.HyperFleetManifest {
	return &hyperfleetv1alpha1.HyperFleetManifest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "123456789012",
		},
		Spec: hyperfleetv1alpha1.HyperFleetManifestSpec{
			ManagementCluster: "mc01",
			Resources: []hyperfleetv1alpha1.ResourceTemplate{
				{
					Resource: "namespaces",
					Content:  runtime.RawExtension{Raw: []byte(`{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"monitoring"}}`)},
				},
				{
					Resource: "configmaps",
					Content:  runtime.RawExtension{Raw: []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"prometheus-config","namespace":"monitoring"},"data":{"scrape.yml":"global: {}"}}`)},
				},
			},
		},
	}
}
