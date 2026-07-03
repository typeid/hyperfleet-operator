package fleetstore

import (
	hypershiftv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	v1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func newTestCluster(ns, name, accountID string) *v1alpha1.Cluster {
	return &v1alpha1.Cluster{
		TypeMeta: metav1.TypeMeta{Kind: "Cluster", APIVersion: v1alpha1.SchemeGroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      name,
		},
		Spec: v1alpha1.ClusterSpec{
			Name:                      name,
			AccountID:                 accountID,
			Region:                    "us-east-1",
			VpcID:                     "vpc-abc123",
			PrivateSubnetIDs:          []string{"subnet-aaa"},
			WorkerInstanceProfileName: "profile",
			WorkerSecurityGroupID:     "sg-aaa",
			Release:                   hypershiftv1beta1.Release{Image: "quay.io/openshift-release-dev/ocp-release:4.17.0-multi"},
			Networking: v1alpha1.NetworkingSpec{
				ClusterNetwork: []v1alpha1.NetworkEntry{{CIDR: "10.128.0.0/14"}},
				ServiceNetwork: []v1alpha1.NetworkEntry{{CIDR: "172.30.0.0/16"}},
			},
			Platform:      v1alpha1.PlatformSpec{},
			OIDCIssuerURL: "https://oidc.example.com",
		},
	}
}

func newTestManagementCluster(name string) *v1alpha1.ManagementCluster {
	return &v1alpha1.ManagementCluster{
		TypeMeta: metav1.TypeMeta{Kind: "ManagementCluster", APIVersion: v1alpha1.SchemeGroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1alpha1.ManagementClusterSpec{
			Region:    "us-east-1",
			AccountID: "123456789012",
		},
	}
}
