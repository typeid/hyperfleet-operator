package fleetstore

import (
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var gvr = schema.GroupVersionResource{Group: "hyperfleet.io", Version: "v1alpha1"}

func notFound(kind, name string) *apierrors.StatusError {
	return apierrors.NewNotFound(gvr.GroupResource(), name)
}

func alreadyExists(kind, name string) *apierrors.StatusError {
	return apierrors.NewAlreadyExists(gvr.GroupResource(), name)
}

func conflict(kind, name string) *apierrors.StatusError {
	return apierrors.NewConflict(gvr.GroupResource(), name, fmt.Errorf("the object has been modified; please apply your changes to the latest version and try again"))
}

// ErrUnsupported is returned for client.Client operations that FleetStore does not support.
var ErrUnsupported = &apierrors.StatusError{ErrStatus: metav1.Status{
	Status:  metav1.StatusFailure,
	Code:    405,
	Reason:  metav1.StatusReasonMethodNotAllowed,
	Message: "this operation is not supported by FleetStore",
}}
