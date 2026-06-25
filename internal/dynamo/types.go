package dynamo

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

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

// ApplyDesire holds a single Kubernetes object to be server-side-applied.
type ApplyDesire struct {
	DynamoDBMetadata `json:"dynamodbMetadata" dynamodbav:",omitempty"`
	Spec             ApplyDesireSpec   `json:"spec"   dynamodbav:"spec"`
	Status           ApplyDesireStatus `json:"status" dynamodbav:"status"`
}

type ApplyDesireSpec struct {
	ManagementCluster string            `json:"managementCluster"       dynamodbav:"managementCluster"`
	ClusterID         string            `json:"clusterID"               dynamodbav:"clusterID"`
	NodePoolName      string            `json:"nodePoolName,omitempty"  dynamodbav:"nodePoolName,omitempty"`
	TargetItem        ResourceReference `json:"targetItem"              dynamodbav:"targetItem"`
	KubeContent       []byte            `json:"kubeContent,omitempty"   dynamodbav:"kubeContent,omitempty"`
}

type ApplyDesireStatus struct {
	Conditions                []metav1.Condition `json:"conditions,omitempty"                dynamodbav:"conditions,omitempty"`
	ObservedDesireUpdateTime  time.Time          `json:"observedDesireUpdateTime,omitempty"   dynamodbav:"observedDesireUpdateTime,omitempty"`
	AppliedResourceGeneration int64              `json:"appliedResourceGeneration,omitempty"  dynamodbav:"appliedResourceGeneration,omitempty"`
}

// DeleteDesire targets a single Kubernetes object for deletion.
type DeleteDesire struct {
	DynamoDBMetadata `json:"dynamodbMetadata" dynamodbav:",omitempty"`
	Spec             DeleteDesireSpec   `json:"spec"   dynamodbav:"spec"`
	Status           DeleteDesireStatus `json:"status" dynamodbav:"status"`
}

type DeleteDesireSpec struct {
	ManagementCluster string            `json:"managementCluster"      dynamodbav:"managementCluster"`
	ClusterID         string            `json:"clusterID"              dynamodbav:"clusterID"`
	NodePoolName      string            `json:"nodePoolName,omitempty" dynamodbav:"nodePoolName,omitempty"`
	TargetItem        ResourceReference `json:"targetItem,omitempty"   dynamodbav:"targetItem"`
}

type DeleteDesireStatus struct {
	Conditions               []metav1.Condition `json:"conditions,omitempty"               dynamodbav:"conditions,omitempty"`
	ObservedDesireUpdateTime time.Time          `json:"observedDesireUpdateTime,omitempty"  dynamodbav:"observedDesireUpdateTime,omitempty"`
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
