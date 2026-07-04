package fleetstore

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ResourceRow maps 1:1 to the resources table columns.
type ResourceRow struct {
	Kind       string
	Namespace  string
	Name       string
	UID        types.UID
	Generation int64

	Labels            json.RawMessage
	Annotations       json.RawMessage
	OwnerRefs         json.RawMessage
	Finalizers        []string
	Spec              json.RawMessage
	Status            json.RawMessage
	CreatedAt         time.Time
	DeletionTimestamp *time.Time

	// Store metadata — stripped on decode, managed by trigger/store.
	Seq          int64
	AWSAccountID *string
	UpdatedAt    time.Time
	DeletedAt    *time.Time
}

// Encode converts a client.Object into a ResourceRow.
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
		Kind:        kind,
		Namespace:   ns,
		Name:        obj.GetName(),
		UID:         obj.GetUID(),
		Generation:  obj.GetGeneration(),
		Labels:      labels,
		Annotations: annotations,
		OwnerRefs:   ownerRefs,
		Finalizers:  obj.GetFinalizers(),
		Spec:        spec,
		Status:      status,
	}

	if row.Finalizers == nil {
		row.Finalizers = []string{}
	}

	if aid := ExtractAccountID(kind, obj); aid != nil {
		row.AWSAccountID = aid
	} else if !IsGlobal(kind) && ns != "" {
		row.AWSAccountID = &ns
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
	v := reflect.ValueOf(obj).Elem()

	spec, err = json.Marshal(v.FieldByName("Spec").Interface())
	if err != nil {
		return nil, nil, fmt.Errorf("marshal %s spec: %w", kind, err)
	}

	status, err = json.Marshal(v.FieldByName("Status").Interface())
	if err != nil {
		return nil, nil, fmt.Errorf("marshal %s status: %w", kind, err)
	}

	return spec, status, nil
}

func unmarshalSpecStatus(kind string, obj client.Object, spec, status json.RawMessage) error {
	v := reflect.ValueOf(obj).Elem()

	if err := json.Unmarshal(spec, v.FieldByName("Spec").Addr().Interface()); err != nil {
		return fmt.Errorf("unmarshal %s spec: %w", kind, err)
	}

	if len(status) > 0 {
		if err := json.Unmarshal(status, v.FieldByName("Status").Addr().Interface()); err != nil {
			return fmt.Errorf("unmarshal %s status: %w", kind, err)
		}
	}

	return nil
}

func setTypeMeta(kind string, obj client.Object) {
	gvk, _ := GVKFor(kind)
	obj.GetObjectKind().SetGroupVersionKind(gvk)
}
