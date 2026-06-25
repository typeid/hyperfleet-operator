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
	"errors"
	"fmt"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	hyperfleetv1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
	"github.com/typeid/hyperfleet-operator/internal/dynamo"
)

const (
	manifestFinalizer          = "hyperfleet.io/manifest"
	manifestTaskKey            = "hyperfleet-manifest"
	manifestStatusRefreshDelay = 30 * time.Second
)

// HyperFleetManifestReconciler reconciles HyperFleetManifest objects by creating
// DynamoDB desires that kube-applier-aws applies to the target management cluster.
// Unlike ClusterReconciler and NodePoolReconciler which generate typed manifests,
// this controller accepts arbitrary Kubernetes resources as raw JSON, enabling
// infrastructure-level resources (ZOA) to be deployed to MCs without new controller code.
type HyperFleetManifestReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Dynamo dynamo.DesireClient
}

// +kubebuilder:rbac:groups=hyperfleet.io,resources=hyperfleetmanifests,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=hyperfleet.io,resources=hyperfleetmanifests/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=hyperfleet.io,resources=hyperfleetmanifests/finalizers,verbs=update

func (r *HyperFleetManifestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var hfm hyperfleetv1alpha1.HyperFleetManifest
	if err := r.Get(ctx, req.NamespacedName, &hfm); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !hfm.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &hfm)
	}

	if !controllerutil.ContainsFinalizer(&hfm, manifestFinalizer) {
		controllerutil.AddFinalizer(&hfm, manifestFinalizer)
		if err := r.Update(ctx, &hfm); err != nil {
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	mc := hfm.Spec.ManagementCluster
	specsPrefix := dynamo.SpecsPrefix(mc)
	scopedTaskKey := manifestScopedTaskKey(&hfm)

	// Build set of current resource document IDs to detect orphans.
	currentDocIDs := make(map[string]struct{}, len(hfm.Spec.Resources))

	for _, res := range hfm.Spec.Resources {
		group, version, name, namespace, err := extractResourceMeta(res.Content.Raw)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("extract metadata from resource %s: %w", res.Resource, err)
		}

		docID := dynamo.NewDocumentID(scopedTaskKey, group, version, res.Resource, namespace, name)
		currentDocIDs[docID] = struct{}{}

		desire := &dynamo.ApplyDesire{
			DynamoDBMetadata: dynamo.DynamoDBMetadata{DocumentID: docID},
			Spec: dynamo.ApplyDesireSpec{
				ManagementCluster: mc,
				ClusterID:         hfm.Name,
				TargetItem: dynamo.ResourceReference{
					Group:     group,
					Version:   version,
					Resource:  res.Resource,
					Namespace: namespace,
					Name:      name,
				},
				KubeContent: res.Content.Raw,
			},
		}
		if err := r.Dynamo.PutApplyDesire(ctx, specsPrefix, desire); err != nil {
			return ctrl.Result{}, fmt.Errorf("put apply desire %s/%s: %w", res.Resource, name, err)
		}
	}

	// H1: Clean up orphaned ApplyDesire specs from resources removed since last generation.
	r.cleanupOrphanedDesires(ctx, &hfm, specsPrefix, scopedTaskKey, currentDocIDs)

	log.Info("ApplyDesires written", "count", len(hfm.Spec.Resources), "mc", mc)

	// Write ReadDesires for watched resources.
	hasWatched := false
	for _, res := range hfm.Spec.Resources {
		if !res.Watch {
			continue
		}
		hasWatched = true
		group, version, name, namespace, err := extractResourceMeta(res.Content.Raw)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("extract metadata for ReadDesire %s: %w", res.Resource, err)
		}
		readDocID := dynamo.NewDocumentID(scopedTaskKey+"-read", group, version, res.Resource, namespace, name)
		readDesire := &dynamo.ReadDesire{
			DynamoDBMetadata: dynamo.DynamoDBMetadata{DocumentID: readDocID},
			Spec: dynamo.ReadDesireSpec{
				ManagementCluster: mc,
				ClusterID:         hfm.Name,
				TargetItem: dynamo.ResourceReference{
					Group:     group,
					Version:   version,
					Resource:  res.Resource,
					Namespace: namespace,
					Name:      name,
				},
			},
		}
		if err := r.Dynamo.PutReadDesire(ctx, specsPrefix, readDesire); err != nil {
			return ctrl.Result{}, fmt.Errorf("put read desire %s/%s: %w", res.Resource, name, err)
		}
	}

	// Poll ReadDesire status and update resourceStatuses.
	statusPrefix := dynamo.StatusPrefix(mc)
	if hasWatched {
		r.updateResourceStatuses(ctx, &hfm, statusPrefix, scopedTaskKey)
	}

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var latest hyperfleetv1alpha1.HyperFleetManifest
		if err := r.Get(ctx, client.ObjectKeyFromObject(&hfm), &latest); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type:               "DesiresWritten",
			Status:             metav1.ConditionTrue,
			Reason:             "ApplyDesiresCreated",
			Message:            fmt.Sprintf("Wrote %d ApplyDesires to DynamoDB", len(hfm.Spec.Resources)),
			ObservedGeneration: latest.Generation,
		})
		latest.Status.Phase = hyperfleetv1alpha1.ManifestPhaseApplied
		latest.Status.AppliedResources = int32(len(hfm.Spec.Resources))
		latest.Status.ObservedGeneration = latest.Generation
		return r.Status().Update(ctx, &latest)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("update status: %w", err)
	}

	if hasWatched {
		return ctrl.Result{RequeueAfter: manifestStatusRefreshDelay}, nil
	}
	return ctrl.Result{}, nil
}

func (r *HyperFleetManifestReconciler) reconcileDelete(ctx context.Context, hfm *hyperfleetv1alpha1.HyperFleetManifest) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(hfm, manifestFinalizer) {
		return ctrl.Result{}, nil
	}

	log.Info("HyperFleetManifest deleting", "manifest", hfm.Name)

	mc := hfm.Spec.ManagementCluster
	specsPrefix := dynamo.SpecsPrefix(mc)
	statusPrefix := dynamo.StatusPrefix(mc)
	scopedTaskKey := manifestScopedTaskKey(hfm) + "-delete"

	type deleteEntry struct {
		resource, name string
		docID          string
	}
	var entries []deleteEntry

	for _, res := range hfm.Spec.Resources {
		group, version, name, namespace, err := extractResourceMeta(res.Content.Raw)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("extract metadata from resource %s: %w", res.Resource, err)
		}

		docID := dynamo.NewDocumentID(scopedTaskKey, group, version, res.Resource, namespace, name)

		deleteDesire := &dynamo.DeleteDesire{
			DynamoDBMetadata: dynamo.DynamoDBMetadata{DocumentID: docID},
			Spec: dynamo.DeleteDesireSpec{
				ManagementCluster: mc,
				ClusterID:         hfm.Name,
				TargetItem: dynamo.ResourceReference{
					Group:     group,
					Version:   version,
					Resource:  res.Resource,
					Namespace: namespace,
					Name:      name,
				},
			},
		}
		if err := r.Dynamo.PutDeleteDesire(ctx, specsPrefix, deleteDesire); err != nil {
			return ctrl.Result{}, fmt.Errorf("put delete desire %s/%s: %w", res.Resource, name, err)
		}
		entries = append(entries, deleteEntry{resource: res.Resource, name: name, docID: docID})
	}

	for _, e := range entries {
		if _, err := r.Dynamo.GetDeleteDesireStatus(ctx, statusPrefix, e.docID); err != nil {
			log.Info("Waiting for DeleteDesire confirmations", "pendingResource", e.name)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}

	// Clean up ReadDesire specs from DynamoDB for watched resources.
	readTaskKey := manifestScopedTaskKey(hfm) + "-read"
	for _, res := range hfm.Spec.Resources {
		if !res.Watch {
			continue
		}
		group, version, name, namespace, _ := extractResourceMeta(res.Content.Raw)
		readDocID := dynamo.NewDocumentID(readTaskKey, group, version, res.Resource, namespace, name)
		if err := r.Dynamo.DeleteDesireSpec(ctx, specsPrefix, "-readdesires", readDocID); err != nil {
			log.Error(err, "failed to clean up ReadDesire spec", "resource", res.Resource, "name", name)
		}
	}

	// Remove finalizer with RetryOnConflict to handle stale resourceVersion.
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var latest hyperfleetv1alpha1.HyperFleetManifest
		if err := r.Get(ctx, client.ObjectKeyFromObject(hfm), &latest); err != nil {
			return err
		}
		if !controllerutil.ContainsFinalizer(&latest, manifestFinalizer) {
			return nil
		}
		controllerutil.RemoveFinalizer(&latest, manifestFinalizer)
		return r.Update(ctx, &latest)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
	}

	return ctrl.Result{}, nil
}

// cleanupOrphanedDesires removes ApplyDesire specs that exist in DynamoDB but are no
// longer in the current spec. This handles the case where resources are removed from
// spec.resources between generations — without cleanup, the old desires would persist
// indefinitely and the removed resources would remain on the management cluster.
func (r *HyperFleetManifestReconciler) cleanupOrphanedDesires(ctx context.Context, hfm *hyperfleetv1alpha1.HyperFleetManifest, specsPrefix, scopedTaskKey string, currentDocIDs map[string]struct{}) {
	log := logf.FromContext(ctx)

	// Only check for orphans when the spec has actually changed.
	if hfm.Generation == hfm.Status.ObservedGeneration {
		return
	}

	// Check each previously-applied resource against current spec.
	// ResourceStatuses tracks what was previously applied — any entry whose
	// docID is absent from currentDocIDs is an orphan.
	for _, rs := range hfm.Status.ResourceStatuses {
		docID := dynamo.NewDocumentID(scopedTaskKey, "", "", rs.Resource, rs.Namespace, rs.Name)
		if _, ok := currentDocIDs[docID]; ok {
			continue
		}
		if err := r.Dynamo.DeleteDesireSpec(ctx, specsPrefix, "-applydesires", docID); err != nil {
			if !errors.Is(err, dynamo.ErrNotFound) {
				log.Error(err, "failed to clean up orphaned ApplyDesire", "resource", rs.Resource, "name", rs.Name)
			}
		} else {
			log.Info("Cleaned up orphaned ApplyDesire", "resource", rs.Resource, "name", rs.Name)
		}
	}
}

func (r *HyperFleetManifestReconciler) updateResourceStatuses(ctx context.Context, hfm *hyperfleetv1alpha1.HyperFleetManifest, statusPrefix, scopedTaskKey string) {
	log := logf.FromContext(ctx)
	var statuses []hyperfleetv1alpha1.ResourceStatus

	for _, res := range hfm.Spec.Resources {
		if !res.Watch {
			continue
		}
		group, version, name, namespace, _ := extractResourceMeta(res.Content.Raw)
		readDocID := dynamo.NewDocumentID(scopedTaskKey+"-read", group, version, res.Resource, namespace, name)
		readStatus, err := r.Dynamo.GetReadDesireStatus(ctx, statusPrefix, readDocID)
		if err != nil {
			log.V(1).Info("ReadDesire status not yet available", "resource", res.Resource, "name", name)
			continue
		}
		if readStatus.KubeContent == nil {
			continue
		}
		var obj struct {
			Status json.RawMessage `json:"status"`
		}
		if err := json.Unmarshal(readStatus.KubeContent, &obj); err != nil {
			log.Error(err, "failed to unmarshal KubeContent", "resource", res.Resource, "name", name)
			continue
		}
		if len(obj.Status) == 0 {
			continue
		}
		statuses = append(statuses, hyperfleetv1alpha1.ResourceStatus{
			Resource:  res.Resource,
			Name:      name,
			Namespace: namespace,
			Status:    runtime.RawExtension{Raw: obj.Status},
		})
	}

	if len(statuses) == 0 {
		return
	}

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var latest hyperfleetv1alpha1.HyperFleetManifest
		if err := r.Get(ctx, client.ObjectKeyFromObject(hfm), &latest); err != nil {
			return err
		}
		latest.Status.ResourceStatuses = statuses
		return r.Status().Update(ctx, &latest)
	}); err != nil {
		log.Error(err, "failed to update resourceStatuses")
	}
}

func (r *HyperFleetManifestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&hyperfleetv1alpha1.HyperFleetManifest{}).
		Named("manifest").
		Complete(r)
}

// manifestScopedTaskKey returns a taskKey scoped to the CR's identity,
// preventing two HyperFleetManifest CRs deploying the same resource
// from producing colliding DynamoDB document IDs.
func manifestScopedTaskKey(hfm *hyperfleetv1alpha1.HyperFleetManifest) string {
	return fmt.Sprintf("%s/%s/%s", manifestTaskKey, hfm.Namespace, hfm.Name)
}

// extractResourceMeta parses apiVersion, metadata.name, and metadata.namespace
// from a raw Kubernetes resource manifest.
func extractResourceMeta(raw []byte) (group, version, name, namespace string, err error) {
	var m struct {
		APIVersion string `json:"apiVersion"`
		Metadata   struct {
			Name      string `json:"name"`
			Namespace string `json:"namespace"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return "", "", "", "", fmt.Errorf("unmarshal content: %w", err)
	}
	if m.Metadata.Name == "" {
		return "", "", "", "", fmt.Errorf("content missing metadata.name")
	}
	if m.APIVersion == "" {
		return "", "", "", "", fmt.Errorf("content missing apiVersion")
	}
	parts := strings.SplitN(m.APIVersion, "/", 2)
	if len(parts) == 1 {
		return "", parts[0], m.Metadata.Name, m.Metadata.Namespace, nil
	}
	return parts[0], parts[1], m.Metadata.Name, m.Metadata.Namespace, nil
}
