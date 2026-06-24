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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// NodePoolSpec defines the desired state of a NodePool.
type NodePoolSpec struct {
	// ClusterRef is the name of the Cluster CR this node pool belongs to.
	// +kubebuilder:validation:MinLength=1
	ClusterRef string `json:"clusterRef"`

	// Replicas is the desired number of worker nodes.
	// +kubebuilder:validation:Minimum=0
	Replicas int32 `json:"replicas"`

	// Management configures node lifecycle management.
	Management NodePoolManagementSpec `json:"management"`

	// Release specifies the OpenShift release for the worker nodes.
	Release ReleaseSpec `json:"release"`

	// Platform contains cloud-provider-specific node pool configuration.
	Platform NodePoolPlatformSpec `json:"platform"`
}

// NodePoolManagementSpec configures node lifecycle behaviour.
type NodePoolManagementSpec struct {
	// AutoRepair enables automatic replacement of unhealthy nodes.
	// +optional
	AutoRepair bool `json:"autoRepair,omitempty"`

	// UpgradeType controls how nodes are upgraded (Replace or InPlace).
	// +kubebuilder:validation:Enum=Replace;InPlace
	// +optional
	UpgradeType string `json:"upgradeType,omitempty"`
}

// NodePoolPlatformSpec contains cloud-provider-specific node pool configuration.
type NodePoolPlatformSpec struct {
	// AWS contains AWS-specific node pool configuration.
	AWS AWSNodePoolSpec `json:"aws"`
}

// AWSNodePoolSpec configures AWS-specific node pool settings.
type AWSNodePoolSpec struct {
	// InstanceType is the EC2 instance type (e.g. m6a.xlarge).
	// +kubebuilder:validation:MinLength=1
	InstanceType string `json:"instanceType"`

	// RootVolume configures the root EBS volume.
	RootVolume RootVolumeSpec `json:"rootVolume"`

	// SubnetId is the subnet where nodes are launched.
	// +kubebuilder:validation:Pattern=`^subnet-[a-z0-9]+$`
	SubnetId string `json:"subnetId"`

	// InstanceProfile is the IAM instance profile for worker nodes.
	// +kubebuilder:validation:MinLength=1
	InstanceProfile string `json:"instanceProfile"`

	// SecurityGroups is the list of security group IDs for worker nodes.
	// +kubebuilder:validation:MinItems=1
	SecurityGroups []string `json:"securityGroups"`
}

// RootVolumeSpec configures the root EBS volume for worker nodes.
type RootVolumeSpec struct {
	// Size is the volume size in GiB.
	// +kubebuilder:validation:Minimum=1
	Size int32 `json:"size"`

	// Type is the EBS volume type (e.g. gp3).
	// +kubebuilder:validation:Enum=gp2;gp3;io1;io2;st1;sc1
	Type string `json:"type"`
}

// NodePoolStatus defines the observed state of a NodePool.
type NodePoolStatus struct {
	// Conditions represent the latest observations of the node pool's state.
	// Known condition types: Ready.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Phase summarizes the node pool's lifecycle state.
	// +optional
	Phase string `json:"phase,omitempty"`

	// ObservedGeneration is the most recent generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=".spec.clusterRef"
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=".spec.replicas"
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// NodePool is the Schema for the nodepools API.
// It represents a set of worker nodes for a Cluster.
type NodePool struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec NodePoolSpec `json:"spec"`

	// +optional
	Status NodePoolStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// NodePoolList contains a list of NodePool.
type NodePoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []NodePool `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &NodePool{}, &NodePoolList{})
		return nil
	})
}
