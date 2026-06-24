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

// ClusterSpec defines the desired state of a ROSA HCP cluster.
type ClusterSpec struct {
	// Name is the human-readable display name for the cluster (3-53 characters).
	// +kubebuilder:validation:MinLength=3
	// +kubebuilder:validation:MaxLength=53
	Name string `json:"name"`

	// AccountID is the AWS account ID that owns this cluster.
	// +kubebuilder:validation:Pattern=`^\d{12}$`
	AccountID string `json:"accountId"`

	// Region is the AWS region (e.g. us-east-1).
	// +kubebuilder:validation:MinLength=1
	Region string `json:"region"`

	// Zone is the AWS availability zone (e.g. us-east-1a).
	// Workaround for CCM region detection on EKS (OCPBUGS-84562).
	// +kubebuilder:validation:MinLength=1
	Zone string `json:"zone"`

	// BaseDomain is the DNS base domain for the cluster.
	// +kubebuilder:validation:MinLength=1
	BaseDomain string `json:"baseDomain"`

	// VpcID is the AWS VPC ID for the cluster's worker nodes.
	// +kubebuilder:validation:Pattern=`^vpc-[a-z0-9]+$`
	VpcID string `json:"vpcId"`

	// PrivateSubnetIds is a comma-separated list of private subnet IDs.
	// +kubebuilder:validation:MinLength=1
	PrivateSubnetIds string `json:"privateSubnetIds"`

	// WorkerInstanceProfileName is the IAM instance profile for worker nodes.
	// +kubebuilder:validation:MinLength=1
	WorkerInstanceProfileName string `json:"workerInstanceProfileName"`

	// WorkerSecurityGroupId is the security group ID for worker nodes.
	// +kubebuilder:validation:Pattern=`^sg-[a-z0-9]+$`
	WorkerSecurityGroupId string `json:"workerSecurityGroupId"`

	// Release specifies the OpenShift release to use for the control plane.
	Release ReleaseSpec `json:"release"`

	// Networking configures cluster networking CIDRs.
	Networking NetworkingSpec `json:"networking"`

	// Platform contains cloud-provider-specific configuration.
	Platform PlatformSpec `json:"platform"`

	// OIDCIssuerURL is the OIDC issuer URL for this cluster's service account
	// token verification (e.g. a CloudFront or S3 endpoint).
	// +kubebuilder:validation:Pattern=`^https://`
	OIDCIssuerURL string `json:"oidcIssuerURL"`

	// CreatorARN is the IAM ARN of the user who created this cluster.
	// Used to bootstrap the initial cluster-admin RBAC mapping.
	// +optional
	// +kubebuilder:validation:Pattern=`^arn:aws:`
	CreatorARN string `json:"creatorARN,omitempty"`
}

// ReleaseSpec identifies an OpenShift release payload.
type ReleaseSpec struct {
	// Image is the release payload image reference.
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`
}

// NetworkingSpec configures cluster network CIDRs.
type NetworkingSpec struct {
	// ClusterNetwork is the CIDR block(s) for pod IPs.
	ClusterNetwork []NetworkEntry `json:"clusterNetwork"`

	// ServiceNetwork is the CIDR block(s) for service ClusterIPs.
	ServiceNetwork []NetworkEntry `json:"serviceNetwork"`

	// MachineNetwork is the CIDR block(s) for node IPs.
	MachineNetwork []NetworkEntry `json:"machineNetwork"`
}

// NetworkEntry is a single CIDR block.
type NetworkEntry struct {
	CIDR string `json:"cidr"`
}

// PlatformSpec contains cloud-provider-specific configuration.
type PlatformSpec struct {
	// AWS contains AWS-specific platform configuration.
	AWS AWSPlatformSpec `json:"aws"`
}

// AWSPlatformSpec configures AWS-specific cluster settings.
type AWSPlatformSpec struct {
	// Roles contains the IAM role ARNs for HyperShift components.
	Roles AWSRolesSpec `json:"roles"`
}

// AWSRolesSpec contains IAM role ARNs for each HyperShift component.
type AWSRolesSpec struct {
	// +kubebuilder:validation:Pattern=`^arn:aws:iam::`
	ControlPlaneOperatorARN string `json:"controlPlaneOperatorARN"`
	// +kubebuilder:validation:Pattern=`^arn:aws:iam::`
	IngressARN string `json:"ingressARN"`
	// +kubebuilder:validation:Pattern=`^arn:aws:iam::`
	ImageRegistryARN string `json:"imageRegistryARN"`
	// +kubebuilder:validation:Pattern=`^arn:aws:iam::`
	KubeCloudControllerARN string `json:"kubeCloudControllerARN"`
	// +kubebuilder:validation:Pattern=`^arn:aws:iam::`
	NodePoolManagementARN string `json:"nodePoolManagementARN"`
	// +kubebuilder:validation:Pattern=`^arn:aws:iam::`
	NetworkARN string `json:"networkARN"`
	// +kubebuilder:validation:Pattern=`^arn:aws:iam::`
	StorageARN string `json:"storageARN"`
}

// ClusterStatus defines the observed state of a Cluster.
type ClusterStatus struct {
	// Conditions represent the latest observations of the cluster's state.
	// Known condition types: Available, Degraded, DesiresWritten.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Phase summarizes the cluster's lifecycle state.
	// +optional
	Phase string `json:"phase,omitempty"`

	// ControlPlaneEndpoint is the API server endpoint for the hosted cluster.
	// +optional
	ControlPlaneEndpoint string `json:"controlPlaneEndpoint,omitempty"`

	// Version is the running OpenShift version.
	// +optional
	Version string `json:"version,omitempty"`

	// PlacementRef references the Placement that assigned this cluster to a management cluster.
	// +optional
	PlacementRef *PlacementReference `json:"placementRef,omitempty"`

	// ObservedGeneration is the most recent generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// PlacementReference identifies the management cluster assignment.
type PlacementReference struct {
	// Name is the Placement CR name.
	Name string `json:"name"`

	// ManagementCluster is the target management cluster ID (e.g. mc01).
	ManagementCluster string `json:"managementCluster"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:printcolumn:name="Display Name",type=string,JSONPath=".spec.name"
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="MC",type=string,JSONPath=".status.placementRef.managementCluster"
// +kubebuilder:printcolumn:name="Endpoint",type=string,JSONPath=".status.controlPlaneEndpoint",priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// Cluster is the Schema for the clusters API.
// It represents a ROSA HCP cluster whose lifecycle is managed by the hyperfleet-operator.
type Cluster struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec ClusterSpec `json:"spec"`

	// +optional
	Status ClusterStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ClusterList contains a list of Cluster.
type ClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Cluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &Cluster{}, &ClusterList{})
		return nil
	})
}
