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
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	hyperfleetv1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
)

var _ = Describe("NodePool Controller", func() {
	Context("When reconciling a NodePool", func() {
		const (
			clusterName  = "test-np-cluster"
			nodePoolName = "test-nodepool"
		)

		ctx := context.Background()

		AfterEach(func() {
			np := &hyperfleetv1alpha1.NodePool{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: nodePoolName}, np); err == nil {
				controllerutil.RemoveFinalizer(np, nodePoolFinalizer)
				_ = k8sClient.Update(ctx, np)
				_ = k8sClient.Delete(ctx, np)
			}
			cluster := &hyperfleetv1alpha1.Cluster{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: clusterName}, cluster); err == nil {
				controllerutil.RemoveFinalizer(cluster, clusterFinalizer)
				_ = k8sClient.Update(ctx, cluster)
				_ = k8sClient.Delete(ctx, cluster)
			}
			placement := &hyperfleetv1alpha1.Placement{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-placement"}, placement); err == nil {
				_ = k8sClient.Delete(ctx, placement)
			}
		})

		It("should add a finalizer on first reconcile", func() {
			cluster := newTestCluster(clusterName)
			Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

			np := newTestNodePool(nodePoolName, clusterName)
			Expect(k8sClient.Create(ctx, np)).To(Succeed())

			reconciler := &NodePoolReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				Dynamo: &fakeDynamo{},
			}

			result, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: nodePoolName},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeTrue())

			var updated hyperfleetv1alpha1.NodePool
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodePoolName}, &updated)).To(Succeed())
			Expect(controllerutil.ContainsFinalizer(&updated, nodePoolFinalizer)).To(BeTrue())
		})

		It("should set WaitingForCluster when parent Cluster has no PlacementRef", func() {
			cluster := newTestCluster(clusterName)
			Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

			np := newTestNodePool(nodePoolName, clusterName)
			Expect(k8sClient.Create(ctx, np)).To(Succeed())

			fd := &fakeDynamo{}
			reconciler := &NodePoolReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				Dynamo: fd,
			}

			// First reconcile: adds finalizer.
			_, _ = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: nodePoolName},
			})
			// Second reconcile: cluster exists but no PlacementRef.
			result, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: nodePoolName},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).NotTo(BeZero())
			Expect(fd.applyCount).To(Equal(0))
		})

		It("should create ApplyDesire when parent Cluster has a Bound Placement", func() {
			cluster := newTestCluster(clusterName)
			Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

			// Set placementRef on the cluster status.
			cluster.Status.PlacementRef = &hyperfleetv1alpha1.PlacementReference{
				Name:              clusterName + "-placement",
				ManagementCluster: "mc01",
			}
			Expect(k8sClient.Status().Update(ctx, cluster)).To(Succeed())

			np := newTestNodePool(nodePoolName, clusterName)
			Expect(k8sClient.Create(ctx, np)).To(Succeed())

			fd := &fakeDynamo{}
			reconciler := &NodePoolReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				Dynamo: fd,
			}

			// First reconcile: adds finalizer.
			_, _ = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: nodePoolName},
			})
			// Second reconcile: creates desire.
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: nodePoolName},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(fd.applyCount).To(Equal(1))
		})
	})
})

func newTestNodePool(name, clusterRef string) *hyperfleetv1alpha1.NodePool {
	return &hyperfleetv1alpha1.NodePool{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: hyperfleetv1alpha1.NodePoolSpec{
			ClusterRef: clusterRef,
			Replicas:   2,
			Management: hyperfleetv1alpha1.NodePoolManagementSpec{
				AutoRepair:  true,
				UpgradeType: "Replace",
			},
			Release: hyperfleetv1alpha1.ReleaseSpec{
				Image: "quay.io/openshift-release-dev/ocp-release:4.17.0-ec.2-x86_64",
			},
			Platform: hyperfleetv1alpha1.NodePoolPlatformSpec{
				AWS: hyperfleetv1alpha1.AWSNodePoolSpec{
					InstanceType:    "m6a.xlarge",
					RootVolume:      hyperfleetv1alpha1.RootVolumeSpec{Size: 120, Type: "gp3"},
					SubnetId:        "subnet-abc123",
					InstanceProfile: "worker-profile",
					SecurityGroups:  []string{"sg-abc123"},
				},
			},
		},
	}
}
