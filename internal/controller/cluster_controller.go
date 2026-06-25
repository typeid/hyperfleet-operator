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
	"encoding/json"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hyperfleetv1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
	"github.com/typeid/hyperfleet-operator/internal/dynamo"
	"github.com/typeid/hyperfleet-operator/internal/render"
)

const (
	clusterFinalizer   = "hyperfleet.io/cluster"
	statusRefreshDelay = 30 * time.Second
	taskKey            = "hyperfleet-operator"
)

// ClusterReconciler reconciles a Cluster object by creating DynamoDB desires
// that kube-applier-aws applies to the management cluster.
type ClusterReconciler struct {
	client.Client
	Scheme         *runtime.Scheme
	Dynamo         dynamo.DesireClient
	RegionalConfig render.RegionalConfig
}

// +kubebuilder:rbac:groups=hyperfleet.io,resources=clusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=hyperfleet.io,resources=clusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=hyperfleet.io,resources=clusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=hyperfleet.io,resources=nodepools,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups=hyperfleet.io,resources=placements,verbs=get;list;watch;delete

func (r *ClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var cluster hyperfleetv1alpha1.Cluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Handle deletion via standard Kubernetes DeletionTimestamp.
	if !cluster.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &cluster)
	}

	// Ensure finalizer.
	if !controllerutil.ContainsFinalizer(&cluster, clusterFinalizer) {
		controllerutil.AddFinalizer(&cluster, clusterFinalizer)
		if err := r.Update(ctx, &cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Look up Placement — if none or not Bound, wait.
	placementName := fmt.Sprintf("%s-placement", cluster.Name)
	var placement hyperfleetv1alpha1.Placement
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: placementName}, &placement); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Waiting for Placement", "cluster", cluster.Name)
			r.setPhase(ctx, &cluster, hyperfleetv1alpha1.ClusterPhaseWaitingForPlacement)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get placement: %w", err)
	}
	if placement.Status.Phase != hyperfleetv1alpha1.PlacementPhaseBound {
		log.Info("Placement not yet Bound", "placement", placementName)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	mc := placement.Spec.ManagementCluster
	specsPrefix := dynamo.SpecsPrefix(mc)
	statusPrefix := dynamo.StatusPrefix(mc)

	// Skip full reconcile when spec hasn't changed (status-only update).
	if cluster.Generation == cluster.Status.ObservedGeneration {
		r.updateStatusFromDynamo(ctx, &cluster, statusPrefix,
			dynamo.NewDocumentID(taskKey+"-read", "hypershift.openshift.io", "v1beta1", "hostedclusters",
				fmt.Sprintf("clusters-%s", cluster.Name), cluster.Spec.Name))
		return ctrl.Result{RequeueAfter: statusRefreshDelay}, nil
	}

	// Generate the 7 cluster resources and create ApplyDesires.
	resources, err := render.ClusterResources(&cluster, r.RegionalConfig)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("render cluster resources: %w", err)
	}
	for _, m := range resources {
		ns := m.Namespace

		docID := dynamo.NewDocumentID(taskKey, m.Group, m.Version, m.Resource, ns, m.Name)
		content, err := json.Marshal(m.Object)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("marshal resource %s: %w", m.Name, err)
		}

		desire := &dynamo.ApplyDesire{
			DynamoDBMetadata: dynamo.DynamoDBMetadata{DocumentID: docID},
			Spec: dynamo.ApplyDesireSpec{
				ManagementCluster: mc,
				ClusterID:         cluster.Name,
				TargetItem: dynamo.ResourceReference{
					Group:     m.Group,
					Version:   m.Version,
					Resource:  m.Resource,
					Namespace: ns,
					Name:      m.Name,
				},
				KubeContent: content,
			},
		}
		if err := r.Dynamo.PutApplyDesire(ctx, specsPrefix, desire); err != nil {
			return ctrl.Result{}, fmt.Errorf("put apply desire %s: %w", m.Name, err)
		}
	}

	// Set DesiresWritten condition.
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var latest hyperfleetv1alpha1.Cluster
		if err := r.Get(ctx, client.ObjectKeyFromObject(&cluster), &latest); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type:               "DesiresWritten",
			Status:             metav1.ConditionTrue,
			Reason:             "ApplyDesiresCreated",
			Message:            fmt.Sprintf("Wrote %d ApplyDesires to DynamoDB", len(resources)),
			ObservedGeneration: latest.Generation,
		})
		return r.Status().Update(ctx, &latest)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("set DesiresWritten condition: %w", err)
	}

	// Create ReadDesire for HostedCluster status feedback.
	hcName := cluster.Spec.Name
	hcNs := fmt.Sprintf("clusters-%s", cluster.Name)
	readDocID := dynamo.NewDocumentID(taskKey+"-read", "hypershift.openshift.io", "v1beta1", "hostedclusters", hcNs, hcName)
	readDesire := &dynamo.ReadDesire{
		DynamoDBMetadata: dynamo.DynamoDBMetadata{DocumentID: readDocID},
		Spec: dynamo.ReadDesireSpec{
			ManagementCluster: mc,
			ClusterID:         cluster.Name,
			TargetItem: dynamo.ResourceReference{
				Group:     "hypershift.openshift.io",
				Version:   "v1beta1",
				Resource:  "hostedclusters",
				Namespace: hcNs,
				Name:      hcName,
			},
		},
	}
	if err := r.Dynamo.PutReadDesire(ctx, specsPrefix, readDesire); err != nil {
		return ctrl.Result{}, fmt.Errorf("put read desire: %w", err)
	}

	// Read status feedback from DynamoDB and update Cluster status.
	r.updateStatusFromDynamo(ctx, &cluster, statusPrefix, readDocID)

	// Set phase to Provisioning if not yet available.
	if cluster.Status.Phase == "" || cluster.Status.Phase == hyperfleetv1alpha1.ClusterPhaseWaitingForPlacement {
		r.setPhase(ctx, &cluster, hyperfleetv1alpha1.ClusterPhaseProvisioning)
	}

	return ctrl.Result{RequeueAfter: statusRefreshDelay}, nil
}

func (r *ClusterReconciler) reconcileDelete(ctx context.Context, cluster *hyperfleetv1alpha1.Cluster) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(cluster, clusterFinalizer) {
		return ctrl.Result{}, nil
	}

	log.Info("Cluster deleting", "cluster", cluster.Name)
	r.setPhase(ctx, cluster, hyperfleetv1alpha1.ClusterPhaseDeleting)

	// Step 1: Delete all associated NodePools. Each NodePool has its own
	// finalizer that creates a DeleteDesire before clearing, so we wait
	// for all NodePools to be fully gone before proceeding.
	var nodePools hyperfleetv1alpha1.NodePoolList
	if err := r.List(ctx, &nodePools, client.InNamespace(cluster.Namespace)); err != nil {
		return ctrl.Result{}, fmt.Errorf("list nodepools: %w", err)
	}
	pendingNodePools := 0
	for i := range nodePools.Items {
		np := &nodePools.Items[i]
		if np.Spec.ClusterRef != cluster.Name {
			continue
		}
		if np.DeletionTimestamp.IsZero() {
			log.Info("Deleting NodePool", "nodePool", np.Name)
			if err := r.Delete(ctx, np); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf("delete nodepool %s: %w", np.Name, err)
			}
		}
		pendingNodePools++
	}
	if pendingNodePools > 0 {
		log.Info("Waiting for NodePools to be deleted", "count", pendingNodePools)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Step 2: Create DeleteDesire for the cluster namespace and wait for confirmation.
	// Use PlacementRef from status; fall back to looking up the Placement directly
	// to avoid skipping DynamoDB cleanup if the PlacementReconciler hasn't run yet.
	mc := ""
	if cluster.Status.PlacementRef != nil {
		mc = cluster.Status.PlacementRef.ManagementCluster
	} else {
		placementName := fmt.Sprintf("%s-placement", cluster.Name)
		var placement hyperfleetv1alpha1.Placement
		if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: placementName}, &placement); err == nil {
			mc = placement.Spec.ManagementCluster
		}
	}

	if mc != "" {
		specsPrefix := dynamo.SpecsPrefix(mc)
		statusPrefix := dynamo.StatusPrefix(mc)

		ns := fmt.Sprintf("clusters-%s", cluster.Name)
		docID := dynamo.NewDocumentID(taskKey+"-delete", "", "v1", "namespaces", "", ns)

		// Write the DeleteDesire (idempotent — same docID overwrites).
		deleteDesire := &dynamo.DeleteDesire{
			DynamoDBMetadata: dynamo.DynamoDBMetadata{DocumentID: docID},
			Spec: dynamo.DeleteDesireSpec{
				ManagementCluster: mc,
				ClusterID:         cluster.Name,
				TargetItem: dynamo.ResourceReference{
					Group:    "",
					Version:  "v1",
					Resource: "namespaces",
					Name:     ns,
				},
			},
		}
		if err := r.Dynamo.PutDeleteDesire(ctx, specsPrefix, deleteDesire); err != nil {
			return ctrl.Result{}, fmt.Errorf("put delete desire: %w", err)
		}

		// Wait for kube-applier-aws to confirm the deletion.
		if _, err := r.Dynamo.GetDeleteDesireStatus(ctx, statusPrefix, docID); err != nil {
			log.Info("Waiting for DeleteDesire confirmation", "namespace", ns)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}

		// Clean up the HostedCluster ReadDesire spec from DynamoDB.
		hcName := cluster.Spec.Name
		readDocID := dynamo.NewDocumentID(taskKey+"-read", "hypershift.openshift.io", "v1beta1", "hostedclusters", ns, hcName)
		if err := r.Dynamo.DeleteDesireSpec(ctx, specsPrefix, "-readdesires", readDocID); err != nil {
			log.Error(err, "failed to clean up ReadDesire spec", "hostedcluster", hcName)
		}
	}

	// Step 3: Delete the Placement explicitly rather than relying on OwnerReference GC.
	// GC ordering is non-deterministic and envtest doesn't run GC at all; OwnerReference is a safety net.
	placementName := fmt.Sprintf("%s-placement", cluster.Name)
	var placement hyperfleetv1alpha1.Placement
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: placementName}, &placement); err == nil {
		log.Info("Deleting Placement", "placement", placementName)
		if err := r.Delete(ctx, &placement); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("delete placement: %w", err)
		}
	}

	// Remove finalizer with RetryOnConflict to handle stale resourceVersion.
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var latest hyperfleetv1alpha1.Cluster
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), &latest); err != nil {
			return err
		}
		if !controllerutil.ContainsFinalizer(&latest, clusterFinalizer) {
			return nil
		}
		controllerutil.RemoveFinalizer(&latest, clusterFinalizer)
		return r.Update(ctx, &latest)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
	}

	return ctrl.Result{}, nil
}

func (r *ClusterReconciler) updateStatusFromDynamo(ctx context.Context, cluster *hyperfleetv1alpha1.Cluster, statusPrefix, readDocID string) {
	log := logf.FromContext(ctx)

	readStatus, err := r.Dynamo.GetReadDesireStatus(ctx, statusPrefix, readDocID)
	if err != nil {
		log.V(1).Info("ReadDesire status not yet available", "error", err)
		return
	}

	if readStatus.KubeContent == nil {
		return
	}

	var hc struct {
		Status struct {
			Conditions []metav1.Condition `json:"conditions"`
			Version    struct {
				History []struct {
					Version string `json:"version"`
				} `json:"history"`
			} `json:"version"`
			ControlPlaneEndpoint struct {
				Host string `json:"host"`
			} `json:"controlPlaneEndpoint"`
		} `json:"status"`
	}
	if err := json.Unmarshal(readStatus.KubeContent, &hc); err != nil {
		log.Error(err, "Failed to unmarshal HostedCluster status")
		return
	}

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var latest hyperfleetv1alpha1.Cluster
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), &latest); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		for _, cond := range hc.Status.Conditions {
			if cond.Type == "Available" || cond.Type == "Degraded" {
				meta.SetStatusCondition(&latest.Status.Conditions, cond)
			}
		}
		if hc.Status.ControlPlaneEndpoint.Host != "" {
			latest.Status.ControlPlaneEndpoint = hc.Status.ControlPlaneEndpoint.Host
		}
		if len(hc.Status.Version.History) > 0 {
			latest.Status.Version = hc.Status.Version.History[0].Version
		}
		if meta.IsStatusConditionTrue(latest.Status.Conditions, "Available") &&
			!meta.IsStatusConditionTrue(latest.Status.Conditions, "Degraded") {
			latest.Status.Phase = hyperfleetv1alpha1.ClusterPhaseReady
		}
		latest.Status.ObservedGeneration = latest.Generation
		return r.Status().Update(ctx, &latest)
	}); err != nil {
		log.Error(err, "Failed to update cluster status from DynamoDB feedback")
	}
}

func (r *ClusterReconciler) setPhase(ctx context.Context, cluster *hyperfleetv1alpha1.Cluster, phase hyperfleetv1alpha1.ClusterPhase) {
	if cluster.Status.Phase == phase {
		return
	}
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var latest hyperfleetv1alpha1.Cluster
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), &latest); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if latest.Status.Phase == phase {
			return nil
		}
		latest.Status.Phase = phase
		latest.Status.ObservedGeneration = latest.Generation
		return r.Status().Update(ctx, &latest)
	}); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to update cluster phase", "phase", phase)
	}
}

func (r *ClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&hyperfleetv1alpha1.Cluster{}).
		Watches(&hyperfleetv1alpha1.Placement{}, handler.EnqueueRequestsFromMapFunc(
			func(ctx context.Context, obj client.Object) []reconcile.Request {
				placement, ok := obj.(*hyperfleetv1alpha1.Placement)
				if !ok {
					return nil
				}
				if placement.Spec.ClusterRef == "" {
					return nil
				}
				return []reconcile.Request{
					{NamespacedName: types.NamespacedName{
						Namespace: placement.Namespace,
						Name:      placement.Spec.ClusterRef,
					}},
				}
			},
		)).
		Named("cluster").
		Complete(r)
}
