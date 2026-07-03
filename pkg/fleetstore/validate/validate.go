package validate

import (
	"fmt"
	"regexp"

	v1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var rfc1123RE = regexp.MustCompile(`^[a-z0-9]([a-z0-9\-]*[a-z0-9])?$`)

// Create validates an object for the Create path.
func Create(obj client.Object, accountID string) error {
	if err := validateName(obj.GetName()); err != nil {
		return err
	}

	kind, err := kindOf(obj)
	if err != nil {
		return err
	}

	if !isGlobal(kind) {
		if accountID == "" {
			return fmt.Errorf("aws_account_id is required for namespaced kind %s", kind)
		}
	}

	if err := validateClusterRef(kind, obj); err != nil {
		return err
	}

	return nil
}

// Update validates mutations between old and new objects.
func Update(old, new client.Object) error {
	if old.GetNamespace() != new.GetNamespace() {
		return fmt.Errorf("namespace is immutable")
	}
	if old.GetName() != new.GetName() {
		return fmt.Errorf("name is immutable")
	}
	if old.GetUID() != new.GetUID() {
		return fmt.Errorf("uid is immutable")
	}

	oldKind, err := kindOf(old)
	if err != nil {
		return err
	}

	if err := validateImmutableAccountID(oldKind, old, new); err != nil {
		return err
	}

	if old.GetDeletionTimestamp() != nil && new.GetDeletionTimestamp() == nil {
		return fmt.Errorf("deletion_timestamp cannot be unset")
	}
	if old.GetDeletionTimestamp() == nil && new.GetDeletionTimestamp() != nil {
		return fmt.Errorf("deletion_timestamp cannot be set via Update; use Delete")
	}

	if err := validateClusterRef(oldKind, new); err != nil {
		return err
	}

	return nil
}

func validateName(name string) error {
	if name == "" {
		return fmt.Errorf("name is required")
	}
	if len(name) > 253 {
		return fmt.Errorf("name must be at most 253 characters")
	}
	if !rfc1123RE.MatchString(name) {
		return fmt.Errorf("name %q must be a valid RFC1123 subdomain", name)
	}
	return nil
}

func validateClusterRef(kind string, obj client.Object) error {
	ns := obj.GetNamespace()
	switch kind {
	case "NodePool":
		if np, ok := obj.(*v1alpha1.NodePool); ok && np.Spec.ClusterRef != ns {
			return fmt.Errorf("spec.clusterRef %q must equal namespace %q", np.Spec.ClusterRef, ns)
		}
	case "Placement":
		if p, ok := obj.(*v1alpha1.Placement); ok && p.Spec.ClusterRef != ns {
			return fmt.Errorf("spec.clusterRef %q must equal namespace %q", p.Spec.ClusterRef, ns)
		}
	}
	return nil
}

func validateImmutableAccountID(kind string, old, new client.Object) error {
	switch kind {
	case "Cluster":
		oldC, newC := old.(*v1alpha1.Cluster), new.(*v1alpha1.Cluster)
		if oldC.Spec.AccountID != newC.Spec.AccountID {
			return fmt.Errorf("spec.accountId is immutable")
		}
	}
	return nil
}

func kindOf(obj client.Object) (string, error) {
	switch obj.(type) {
	case *v1alpha1.Cluster:
		return "Cluster", nil
	case *v1alpha1.NodePool:
		return "NodePool", nil
	case *v1alpha1.Placement:
		return "Placement", nil
	case *v1alpha1.Manifest:
		return "Manifest", nil
	case *v1alpha1.ManagementCluster:
		return "ManagementCluster", nil
	default:
		return "", fmt.Errorf("unknown type %T", obj)
	}
}

func isGlobal(kind string) bool {
	return kind == "ManagementCluster"
}
