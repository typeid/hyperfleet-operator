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
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	hyperfleetv1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
	"github.com/typeid/hyperfleet-operator/internal/dynamo"
	"github.com/typeid/hyperfleet-operator/internal/render"
)

const (
	nodePoolFinalizer = "hyperfleet.io/nodepool"
)

// NodePoolReconciler reconciles a NodePool object by creating DynamoDB desires
// that kube-applier-aws applies to the management cluster.
type NodePoolReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Dynamo dynamo.DesireClient
}

// +kubebuilder:rbac:groups=hyperfleet.io,resources=nodepools,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=hyperfleet.io,resources=nodepools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=hyperfleet.io,resources=nodepools/finalizers,verbs=update
// +kubebuilder:rbac:groups=hyperfleet.io,resources=clusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=hyperfleet.io,resources=placements,verbs=get;list;watch

func (r *NodePoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var nodePool hyperfleetv1alpha1.NodePool
	if err := r.Get(ctx, req.NamespacedName, &nodePool); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !nodePool.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &nodePool)
	}

	if !controllerutil.ContainsFinalizer(&nodePool, nodePoolFinalizer) {
		controllerutil.AddFinalizer(&nodePool, nodePoolFinalizer)
		if err := r.Update(ctx, &nodePool); err != nil {
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Look up parent Cluster.
	var cluster hyperfleetv1alpha1.Cluster
	if err := r.Get(ctx, types.NamespacedName{Namespace: nodePool.Namespace, Name: nodePool.Spec.ClusterRef}, &cluster); err != nil {
		log.Info("Waiting for parent Cluster", "clusterRef", nodePool.Spec.ClusterRef)
		r.setPhase(ctx, &nodePool, hyperfleetv1alpha1.NodePoolPhaseWaitingForCluster)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Cluster must have a Placement before we can target an MC.
	if cluster.Status.PlacementRef == nil {
		log.Info("Cluster has no Placement yet", "cluster", cluster.Name)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	mc := cluster.Status.PlacementRef.ManagementCluster
	specsPrefix := dynamo.SpecsPrefix(mc)
	statusPrefix := dynamo.StatusPrefix(mc)

	// Generate NodePool resource and create ApplyDesire.
	m := render.NodePoolResource(&nodePool, &cluster)
	ns := m.Namespace

	docID := dynamo.NewDocumentID(taskKey, m.Group, m.Version, m.Resource, ns, m.Name)

	// Skip full reconcile when spec hasn't changed.
	if nodePool.Generation == nodePool.Status.ObservedGeneration {
		r.updateStatusFromDynamo(ctx, &nodePool, statusPrefix, docID)
		return ctrl.Result{RequeueAfter: statusRefreshDelay}, nil
	}

	content, err := json.Marshal(m.Object)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("marshal nodepool resource: %w", err)
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
		return ctrl.Result{}, fmt.Errorf("put apply desire: %w", err)
	}

	// Read status feedback from DynamoDB.
	r.updateStatusFromDynamo(ctx, &nodePool, statusPrefix, docID)

	if nodePool.Status.Phase == "" || nodePool.Status.Phase == hyperfleetv1alpha1.NodePoolPhaseWaitingForCluster {
		r.setPhase(ctx, &nodePool, hyperfleetv1alpha1.NodePoolPhaseProvisioning)
	}

	return ctrl.Result{RequeueAfter: statusRefreshDelay}, nil
}

func (r *NodePoolReconciler) reconcileDelete(ctx context.Context, nodePool *hyperfleetv1alpha1.NodePool) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(nodePool, nodePoolFinalizer) {
		return ctrl.Result{}, nil
	}

	log.Info("NodePool deleting", "nodePool", nodePool.Name)
	r.setPhase(ctx, nodePool, hyperfleetv1alpha1.NodePoolPhaseDeleting)

	// Look up parent Cluster for MC target.
	var cluster hyperfleetv1alpha1.Cluster
	if err := r.Get(ctx, types.NamespacedName{Namespace: nodePool.Namespace, Name: nodePool.Spec.ClusterRef}, &cluster); err == nil && cluster.Status.PlacementRef != nil {
		mc := cluster.Status.PlacementRef.ManagementCluster
		specsPrefix := dynamo.SpecsPrefix(mc)
		statusPrefix := dynamo.StatusPrefix(mc)

		m := render.NodePoolResource(nodePool, &cluster)
		ns := m.Namespace

		docID := dynamo.NewDocumentID(taskKey+"-delete", m.Group, m.Version, m.Resource, ns, m.Name)

		deleteDesire := &dynamo.DeleteDesire{
			DynamoDBMetadata: dynamo.DynamoDBMetadata{DocumentID: docID},
			Spec: dynamo.DeleteDesireSpec{
				ManagementCluster: mc,
				ClusterID:         cluster.Name,
				TargetItem: dynamo.ResourceReference{
					Group:     m.Group,
					Version:   m.Version,
					Resource:  m.Resource,
					Namespace: ns,
					Name:      m.Name,
				},
			},
		}
		if err := r.Dynamo.PutDeleteDesire(ctx, specsPrefix, deleteDesire); err != nil {
			return ctrl.Result{}, fmt.Errorf("put delete desire: %w", err)
		}

		if _, err := r.Dynamo.GetDeleteDesireStatus(ctx, statusPrefix, docID); err != nil {
			log.Info("Waiting for DeleteDesire confirmation", "nodePool", nodePool.Name)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}

	// Remove finalizer with RetryOnConflict to handle stale resourceVersion.
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var latest hyperfleetv1alpha1.NodePool
		if err := r.Get(ctx, client.ObjectKeyFromObject(nodePool), &latest); err != nil {
			return err
		}
		if !controllerutil.ContainsFinalizer(&latest, nodePoolFinalizer) {
			return nil
		}
		controllerutil.RemoveFinalizer(&latest, nodePoolFinalizer)
		return r.Update(ctx, &latest)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
	}

	return ctrl.Result{}, nil
}

func (r *NodePoolReconciler) updateStatusFromDynamo(ctx context.Context, nodePool *hyperfleetv1alpha1.NodePool, statusPrefix, docID string) {
	log := logf.FromContext(ctx)

	applyStatus, err := r.Dynamo.GetApplyDesireStatus(ctx, statusPrefix, docID)
	if err != nil {
		log.V(1).Info("ApplyDesire status not yet available", "error", err)
		return
	}

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var latest hyperfleetv1alpha1.NodePool
		if err := r.Get(ctx, client.ObjectKeyFromObject(nodePool), &latest); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if applyStatus.AppliedResourceGeneration > 0 {
			meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
				Type:               "Applied",
				Status:             metav1.ConditionTrue,
				Reason:             "DesireApplied",
				Message:            "NodePool applied to management cluster",
				ObservedGeneration: latest.Generation,
			})
		}
		latest.Status.ObservedGeneration = latest.Generation
		return r.Status().Update(ctx, &latest)
	}); err != nil {
		log.Error(err, "Failed to update nodepool status from DynamoDB feedback")
	}
}

func (r *NodePoolReconciler) setPhase(ctx context.Context, nodePool *hyperfleetv1alpha1.NodePool, phase hyperfleetv1alpha1.NodePoolPhase) {
	if nodePool.Status.Phase == phase {
		return
	}
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var latest hyperfleetv1alpha1.NodePool
		if err := r.Get(ctx, client.ObjectKeyFromObject(nodePool), &latest); err != nil {
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
		logf.FromContext(ctx).Error(err, "Failed to update nodepool phase", "phase", phase)
	}
}

func (r *NodePoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&hyperfleetv1alpha1.NodePool{}).
		Named("nodepool").
		Complete(r)
}
