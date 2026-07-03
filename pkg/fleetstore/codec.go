package fleetstore

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	v1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ResourceRow maps 1:1 to the resources table columns.
type ResourceRow struct {
	Kind      string
	Namespace string
	Name      string
	UID       types.UID
	Generation int64

	Labels      json.RawMessage
	Annotations json.RawMessage
	OwnerRefs   json.RawMessage
	Finalizers  []string
	Spec        json.RawMessage
	Status      json.RawMessage
	CreatedAt   time.Time
	DeletionTimestamp *time.Time

	// Store metadata — stripped on decode, managed by trigger/store.
	Seq            int64
	AWSAccountID   *string
	UpdatedAt      time.Time
	DeletedAt      *time.Time
}

// Encode converts a client.Object into a ResourceRow.
// The caller must set AWSAccountID on the returned row for non-Cluster namespaced kinds.
func Encode(obj client.Object) (*ResourceRow, error) {
	kind, err := KindFor(obj)
	if err != nil {
		return nil, err
	}

	spec, status, err := marshalSpecStatus(kind, obj)
	if err != nil {
		return nil, err
	}

	labels, err := json.Marshal(obj.GetLabels())
	if err != nil {
		return nil, fmt.Errorf("marshal labels: %w", err)
	}
	if string(labels) == "null" {
		labels = []byte("{}")
	}

	annotations, err := json.Marshal(obj.GetAnnotations())
	if err != nil {
		return nil, fmt.Errorf("marshal annotations: %w", err)
	}
	if string(annotations) == "null" {
		annotations = []byte("{}")
	}

	ownerRefs, err := json.Marshal(obj.GetOwnerReferences())
	if err != nil {
		return nil, fmt.Errorf("marshal ownerRefs: %w", err)
	}
	if string(ownerRefs) == "null" {
		ownerRefs = []byte("[]")
	}

	ns := obj.GetNamespace()
	if IsGlobal(kind) {
		ns = GlobalNamespace
	}

	row := &ResourceRow{
		Kind:       kind,
		Namespace:  ns,
		Name:       obj.GetName(),
		UID:        obj.GetUID(),
		Generation: obj.GetGeneration(),
		Labels:     labels,
		Annotations: annotations,
		OwnerRefs:  ownerRefs,
		Finalizers: obj.GetFinalizers(),
		Spec:       spec,
		Status:     status,
	}

	if row.Finalizers == nil {
		row.Finalizers = []string{}
	}

	// Extract aws_account_id for Cluster kind.
	if kind == "Cluster" {
		if c, ok := obj.(*v1alpha1.Cluster); ok {
			row.AWSAccountID = &c.Spec.AccountID
		}
	}

	return row, nil
}

// Decode converts a ResourceRow back into the typed Go object.
// Store-only metadata (seq, aws_account_id, updated_at, deleted_at) is stripped;
// seq is mapped to metadata.resourceVersion.
func Decode(row *ResourceRow) (client.Object, error) {
	obj, err := NewObject(row.Kind)
	if err != nil {
		return nil, err
	}

	if err := unmarshalSpecStatus(row.Kind, obj, row.Spec, row.Status); err != nil {
		return nil, err
	}

	ns := row.Namespace
	if IsGlobal(row.Kind) {
		ns = ""
	}
	obj.SetNamespace(ns)
	obj.SetName(row.Name)
	obj.SetUID(row.UID)
	obj.SetGeneration(row.Generation)
	obj.SetResourceVersion(strconv.FormatInt(row.Seq, 10))
	obj.SetCreationTimestamp(metav1.NewTime(row.CreatedAt))

	if row.DeletionTimestamp != nil {
		t := metav1.NewTime(*row.DeletionTimestamp)
		obj.SetDeletionTimestamp(&t)
	}

	var labels map[string]string
	if len(row.Labels) > 0 {
		if err := json.Unmarshal(row.Labels, &labels); err != nil {
			return nil, fmt.Errorf("unmarshal labels: %w", err)
		}
	}
	obj.SetLabels(labels)

	var annotations map[string]string
	if len(row.Annotations) > 0 {
		if err := json.Unmarshal(row.Annotations, &annotations); err != nil {
			return nil, fmt.Errorf("unmarshal annotations: %w", err)
		}
	}
	obj.SetAnnotations(annotations)

	var ownerRefs []metav1.OwnerReference
	if len(row.OwnerRefs) > 0 {
		if err := json.Unmarshal(row.OwnerRefs, &ownerRefs); err != nil {
			return nil, fmt.Errorf("unmarshal ownerRefs: %w", err)
		}
	}
	obj.SetOwnerReferences(ownerRefs)

	obj.SetFinalizers(row.Finalizers)

	setTypeMeta(row.Kind, obj)

	return obj, nil
}

// SeqFromResourceVersion parses the seq int64 from a resourceVersion string.
func SeqFromResourceVersion(rv string) (int64, error) {
	if rv == "" {
		return 0, fmt.Errorf("empty resourceVersion")
	}
	return strconv.ParseInt(rv, 10, 64)
}

func marshalSpecStatus(kind string, obj client.Object) (spec, status json.RawMessage, err error) {
	switch kind {
	case "Cluster":
		o := obj.(*v1alpha1.Cluster)
		spec, err = json.Marshal(o.Spec)
		if err != nil {
			return nil, nil, fmt.Errorf("marshal Cluster spec: %w", err)
		}
		status, err = json.Marshal(o.Status)
	case "NodePool":
		o := obj.(*v1alpha1.NodePool)
		spec, err = json.Marshal(o.Spec)
		if err != nil {
			return nil, nil, fmt.Errorf("marshal NodePool spec: %w", err)
		}
		status, err = json.Marshal(o.Status)
	case "Placement":
		o := obj.(*v1alpha1.Placement)
		spec, err = json.Marshal(o.Spec)
		if err != nil {
			return nil, nil, fmt.Errorf("marshal Placement spec: %w", err)
		}
		status, err = json.Marshal(o.Status)
	case "Manifest":
		o := obj.(*v1alpha1.Manifest)
		spec, err = json.Marshal(o.Spec)
		if err != nil {
			return nil, nil, fmt.Errorf("marshal Manifest spec: %w", err)
		}
		status, err = json.Marshal(o.Status)
	case "ManagementCluster":
		o := obj.(*v1alpha1.ManagementCluster)
		spec, err = json.Marshal(o.Spec)
		if err != nil {
			return nil, nil, fmt.Errorf("marshal ManagementCluster spec: %w", err)
		}
		status, err = json.Marshal(o.Status)
	default:
		return nil, nil, fmt.Errorf("fleetstore codec: unknown kind %q", kind)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("marshal %s status: %w", kind, err)
	}
	return spec, status, nil
}

func unmarshalSpecStatus(kind string, obj client.Object, spec, status json.RawMessage) error {
	switch kind {
	case "Cluster":
		o := obj.(*v1alpha1.Cluster)
		if err := json.Unmarshal(spec, &o.Spec); err != nil {
			return fmt.Errorf("unmarshal Cluster spec: %w", err)
		}
		if len(status) > 0 {
			if err := json.Unmarshal(status, &o.Status); err != nil {
				return fmt.Errorf("unmarshal Cluster status: %w", err)
			}
		}
	case "NodePool":
		o := obj.(*v1alpha1.NodePool)
		if err := json.Unmarshal(spec, &o.Spec); err != nil {
			return fmt.Errorf("unmarshal NodePool spec: %w", err)
		}
		if len(status) > 0 {
			if err := json.Unmarshal(status, &o.Status); err != nil {
				return fmt.Errorf("unmarshal NodePool status: %w", err)
			}
		}
	case "Placement":
		o := obj.(*v1alpha1.Placement)
		if err := json.Unmarshal(spec, &o.Spec); err != nil {
			return fmt.Errorf("unmarshal Placement spec: %w", err)
		}
		if len(status) > 0 {
			if err := json.Unmarshal(status, &o.Status); err != nil {
				return fmt.Errorf("unmarshal Placement status: %w", err)
			}
		}
	case "Manifest":
		o := obj.(*v1alpha1.Manifest)
		if err := json.Unmarshal(spec, &o.Spec); err != nil {
			return fmt.Errorf("unmarshal Manifest spec: %w", err)
		}
		if len(status) > 0 {
			if err := json.Unmarshal(status, &o.Status); err != nil {
				return fmt.Errorf("unmarshal Manifest status: %w", err)
			}
		}
	case "ManagementCluster":
		o := obj.(*v1alpha1.ManagementCluster)
		if err := json.Unmarshal(spec, &o.Spec); err != nil {
			return fmt.Errorf("unmarshal ManagementCluster spec: %w", err)
		}
		if len(status) > 0 {
			if err := json.Unmarshal(status, &o.Status); err != nil {
				return fmt.Errorf("unmarshal ManagementCluster status: %w", err)
			}
		}
	default:
		return fmt.Errorf("fleetstore codec: unknown kind %q", kind)
	}
	return nil
}

func setTypeMeta(kind string, obj client.Object) {
	switch kind {
	case "Cluster":
		obj.(*v1alpha1.Cluster).TypeMeta = metav1.TypeMeta{Kind: "Cluster", APIVersion: v1alpha1.SchemeGroupVersion.String()}
	case "NodePool":
		obj.(*v1alpha1.NodePool).TypeMeta = metav1.TypeMeta{Kind: "NodePool", APIVersion: v1alpha1.SchemeGroupVersion.String()}
	case "Placement":
		obj.(*v1alpha1.Placement).TypeMeta = metav1.TypeMeta{Kind: "Placement", APIVersion: v1alpha1.SchemeGroupVersion.String()}
	case "Manifest":
		obj.(*v1alpha1.Manifest).TypeMeta = metav1.TypeMeta{Kind: "Manifest", APIVersion: v1alpha1.SchemeGroupVersion.String()}
	case "ManagementCluster":
		obj.(*v1alpha1.ManagementCluster).TypeMeta = metav1.TypeMeta{Kind: "ManagementCluster", APIVersion: v1alpha1.SchemeGroupVersion.String()}
	}
}
