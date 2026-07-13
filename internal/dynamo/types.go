package dynamo

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DesireConditionSuccessful is the condition type that kube-applier-aws sets on
// desire statuses. "True" with reason "NoErrors" means the operation completed
// (resource applied, deleted, or read). "False" with reasons like
// "WaitingForDeletion" or "KubeAPIError" indicates in-progress or failure.
const DesireConditionSuccessful = "Successful"

// ResourceReference identifies a Kubernetes resource without needing a RESTMapper.
// Resource is the plural form (e.g. "nodepools") because kube-applier-aws uses it
// to build Kubernetes REST paths. This could be eliminated if kube-applier-aws
// derived the plural via discovery against the MC API server instead.
type ResourceReference struct {
	Group     string `json:"group"               dynamodbav:"group"`
	Version   string `json:"version"             dynamodbav:"version"`
	Resource  string `json:"resource"            dynamodbav:"resource"`
	Namespace string `json:"namespace,omitempty" dynamodbav:"namespace,omitempty"`
	Name      string `json:"name"                dynamodbav:"name"`
}

// DynamoDBMetadata holds per-item metadata fields that every desire type carries.
type DynamoDBMetadata struct {
	DocumentID string    `json:"documentID"           dynamodbav:"-"`
	Version    int64     `json:"version"              dynamodbav:"version"`
	UpdateTime time.Time `json:"updateTime"           dynamodbav:"updateTime"`
	CreateTime time.Time `json:"createTime,omitempty" dynamodbav:"createTime,omitempty"`
}

// ApplyDesireType discriminates the operation an ApplyDesire performs.
// Matches the type values used by kube-applier-aws.
type ApplyDesireType string

const (
	// ApplyDesireTypeServerSideApply indicates a server-side-apply of
	// Spec.ServerSideApply.KubeContent to the management cluster.
	ApplyDesireTypeServerSideApply ApplyDesireType = "ServerSideApply"

	// ApplyDesireTypeDelete indicates a deletion of Spec.TargetItem from
	// the management cluster. Spec.ServerSideApply must be nil.
	ApplyDesireTypeDelete ApplyDesireType = "Delete"
)

// ApplyDesire holds a single intent to either server-side-apply a Kubernetes
// object or delete one from the management cluster's apiserver.
type ApplyDesire struct {
	DynamoDBMetadata `json:"dynamodbMetadata" dynamodbav:",omitempty"`
	Spec             ApplyDesireSpec   `json:"spec"   dynamodbav:"spec"`
	Status           ApplyDesireStatus `json:"status" dynamodbav:"status"`
}

// ApplyDesireSpec is discriminated by Type:
//   - Type=ServerSideApply: ServerSideApply must be non-nil and contains the
//     manifest to apply.
//   - Type=Delete: ServerSideApply must be nil; TargetItem identifies the
//     object to delete.
type ApplyDesireSpec struct {
	Type              ApplyDesireType      `json:"type,omitempty"           dynamodbav:"type,omitempty"`
	ManagementCluster string               `json:"managementCluster"        dynamodbav:"managementCluster"`
	ClusterID         string               `json:"clusterID"                dynamodbav:"clusterID"`
	NodePoolName      string               `json:"nodePoolName,omitempty"   dynamodbav:"nodePoolName,omitempty"`
	TargetItem        ResourceReference    `json:"targetItem"               dynamodbav:"targetItem"`
	ServerSideApply   *ServerSideApplyConfig `json:"serverSideApply,omitempty" dynamodbav:"serverSideApply,omitempty"`
}

// ServerSideApplyConfig holds the manifest for a ServerSideApply desire.
type ServerSideApplyConfig struct {
	// KubeContent is the raw JSON of the Kubernetes object to apply.
	KubeContent []byte `json:"kubeContent,omitempty" dynamodbav:"-"`
}

type ApplyDesireStatus struct {
	Conditions                []metav1.Condition `json:"conditions,omitempty"                dynamodbav:"conditions,omitempty"`
	ObservedDesireUpdateTime  time.Time          `json:"observedDesireUpdateTime,omitempty"   dynamodbav:"observedDesireUpdateTime,omitempty"`
	AppliedResourceGeneration int64              `json:"appliedResourceGeneration,omitempty"  dynamodbav:"appliedResourceGeneration,omitempty"`
}

// ReadDesire requests kube-applier to watch a resource and mirror it back into status.
type ReadDesire struct {
	DynamoDBMetadata `json:"dynamodbMetadata" dynamodbav:",omitempty"`
	Spec             ReadDesireSpec   `json:"spec"   dynamodbav:"spec"`
	Status           ReadDesireStatus `json:"status" dynamodbav:"status"`
}

type ReadDesireSpec struct {
	ManagementCluster string            `json:"managementCluster"      dynamodbav:"managementCluster"`
	ClusterID         string            `json:"clusterID"              dynamodbav:"clusterID"`
	NodePoolName      string            `json:"nodePoolName,omitempty" dynamodbav:"nodePoolName,omitempty"`
	TargetItem        ResourceReference `json:"targetItem,omitempty"   dynamodbav:"targetItem"`
}

type ReadDesireStatus struct {
	Conditions               []metav1.Condition `json:"conditions,omitempty"               dynamodbav:"conditions,omitempty"`
	ObservedDesireUpdateTime time.Time          `json:"observedDesireUpdateTime,omitempty"  dynamodbav:"observedDesireUpdateTime,omitempty"`
	KubeContent              []byte             `json:"kubeContent,omitempty"               dynamodbav:"kubeContent,omitempty"`
}
