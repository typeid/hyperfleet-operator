package render

import (
	"fmt"
	"strings"

	configv1 "github.com/openshift/api/config/v1"
	hypershiftv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	"github.com/openshift/hypershift/api/util/ipnet"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	hyperfleetv1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
)

// ClusterResources generates the 7 Kubernetes resources for a cluster on the MC.
func ClusterResources(cluster *hyperfleetv1alpha1.Cluster, rcfg RegionalConfig) ([]Resource, error) {
	clusterID := cluster.Name
	clusterName := cluster.Spec.Name
	ns := clusterNamespace(clusterID)
	h4 := hash4(clusterID)

	hc, err := hostedCluster(cluster, h4, rcfg)
	if err != nil {
		return nil, err
	}

	return []Resource{
		namespace(clusterID, ns),
		clusterConfig(clusterID, clusterName, ns),
		awsIAMAuthConfig(clusterID, clusterName, ns, cluster.Spec.CreatorARN),
		pullSecret(clusterID, ns),
		apiServingCert(clusterID, clusterName, h4, rcfg.BaseDomain, ns),
		hc,
		sshKey(clusterID, ns),
	}, nil
}

func namespace(clusterID, ns string) Resource {
	return Resource{
		Group: "", Version: "v1", Resource: "namespaces",
		Name: ns, Namespace: "",
		Object: &corev1.Namespace{
			TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
			ObjectMeta: metav1.ObjectMeta{
				Name: ns,
				Labels: map[string]string{
					"hyperfleet.io/cluster-id":    clusterID,
					"hyperfleet.io/managed-by":    "hyperfleet-operator",
					"hyperfleet.io/resource-type": "namespace",
				},
			},
		},
	}
}

func clusterConfig(clusterID, clusterName, ns string) Resource {
	return Resource{
		Group: "", Version: "v1", Resource: "configmaps",
		Name: "cluster-config", Namespace: ns,
		Object: &corev1.ConfigMap{
			TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
			ObjectMeta: metav1.ObjectMeta{
				Name:      "cluster-config",
				Namespace: ns,
				Labels: map[string]string{
					"hyperfleet.io/cluster-id": clusterID,
				},
			},
			Data: map[string]string{
				"cluster_id":   clusterID,
				"cluster_name": clusterName,
			},
		},
	}
}

func awsIAMAuthConfig(clusterID, clusterName, ns, creatorARN string) Resource {
	mapUsers := "      mapUsers: []\n"
	if creatorARN != "" {
		mapUsers = fmt.Sprintf(`      mapUsers:
        - userARN: %s
          username: cluster-creator
          groups:
            - system:masters
`, creatorARN)
	}

	configYAML := fmt.Sprintf("clusterID: %s\nserver:\n%s", clusterID, mapUsers)

	return Resource{
		Group: "", Version: "v1", Resource: "configmaps",
		Name: "aws-iam-auth-config", Namespace: ns,
		Object: &corev1.ConfigMap{
			TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
			ObjectMeta: metav1.ObjectMeta{
				Name:      "aws-iam-auth-config",
				Namespace: ns,
				Labels: map[string]string{
					"hyperfleet.io/cluster-id": clusterID,
				},
				Annotations: map[string]string{
					"hypershift.openshift.io/cluster": fmt.Sprintf("%s/%s", ns, clusterName),
				},
			},
			Data: map[string]string{
				"config.yaml": configYAML,
			},
		},
	}
}

func pullSecret(clusterID, ns string) Resource {
	return Resource{
		Group: "external-secrets.io", Version: "v1", Resource: "externalsecrets",
		Name: "pull-secret", Namespace: ns,
		Object: &ExternalSecret{
			TypeMeta: metav1.TypeMeta{APIVersion: "external-secrets.io/v1", Kind: "ExternalSecret"},
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pull-secret",
				Namespace: ns,
				Labels: map[string]string{
					"hyperfleet.io/cluster-id":    clusterID,
					"hyperfleet.io/resource-type": "pull-secret",
				},
			},
			Spec: ExternalSecretSpec{
				RefreshInterval: "1h",
				SecretStoreRef: SecretStoreRef{
					Name: "aws-parameter-store",
					Kind: "ClusterSecretStore",
				},
				Target: ExternalSecretTarget{
					Name:           "pull-secret",
					CreationPolicy: "Orphan",
					Template: ExternalSecretTargetTemplate{
						Type: "kubernetes.io/dockerconfigjson",
					},
				},
				Data: []ExternalSecretDataEntry{
					{
						SecretKey: ".dockerconfigjson",
						RemoteRef: ExternalRemoteRef{Key: "/infra/pull-secret"},
					},
				},
			},
		},
	}
}

func apiServingCert(clusterID, clusterName, h4, baseDomain, ns string) Resource {
	return Resource{
		Group: "cert-manager.io", Version: "v1", Resource: "certificates",
		Name: "api-serving-cert", Namespace: ns,
		Object: &Certificate{
			TypeMeta: metav1.TypeMeta{APIVersion: "cert-manager.io/v1", Kind: "Certificate"},
			ObjectMeta: metav1.ObjectMeta{
				Name:      "api-serving-cert",
				Namespace: ns,
				Labels: map[string]string{
					"hyperfleet.io/cluster-id": clusterID,
				},
			},
			Spec: CertificateSpec{
				SecretName: "api-serving-cert",
				IssuerRef: CertificateIssuerRef{
					Name: "letsencrypt-dns01",
					Kind: "ClusterIssuer",
				},
				DNSNames: []string{
					fmt.Sprintf("*.%s.%s.%s", clusterName, h4, baseDomain),
				},
			},
		},
	}
}

func hostedCluster(cluster *hyperfleetv1alpha1.Cluster, h4 string, rcfg RegionalConfig) (Resource, error) {
	clusterID := cluster.Name
	clusterName := cluster.Spec.Name
	ns := clusterNamespace(clusterID)
	baseDomain := rcfg.BaseDomain
	zone := rcfg.AWSRegion + "a"
	roles := cluster.Spec.Platform.AWS.Roles

	clusterNetwork := make([]hypershiftv1beta1.ClusterNetworkEntry, 0, len(cluster.Spec.Networking.ClusterNetwork))
	for _, n := range cluster.Spec.Networking.ClusterNetwork {
		parsed, err := ipnet.ParseCIDR(n.CIDR)
		if err != nil {
			return Resource{}, fmt.Errorf("parse cluster network CIDR %q: %w", n.CIDR, err)
		}
		clusterNetwork = append(clusterNetwork, hypershiftv1beta1.ClusterNetworkEntry{CIDR: *parsed})
	}
	serviceNetwork := make([]hypershiftv1beta1.ServiceNetworkEntry, 0, len(cluster.Spec.Networking.ServiceNetwork))
	for _, n := range cluster.Spec.Networking.ServiceNetwork {
		parsed, err := ipnet.ParseCIDR(n.CIDR)
		if err != nil {
			return Resource{}, fmt.Errorf("parse service network CIDR %q: %w", n.CIDR, err)
		}
		serviceNetwork = append(serviceNetwork, hypershiftv1beta1.ServiceNetworkEntry{CIDR: *parsed})
	}
	machineNetwork := make([]hypershiftv1beta1.MachineNetworkEntry, 0, len(cluster.Spec.Networking.MachineNetwork))
	for _, n := range cluster.Spec.Networking.MachineNetwork {
		parsed, err := ipnet.ParseCIDR(n.CIDR)
		if err != nil {
			return Resource{}, fmt.Errorf("parse machine network CIDR %q: %w", n.CIDR, err)
		}
		machineNetwork = append(machineNetwork, hypershiftv1beta1.MachineNetworkEntry{CIDR: *parsed})
	}

	apiHost := fmt.Sprintf("api.%s.%s.%s", clusterName, h4, baseDomain)

	return Resource{
		Group: "hypershift.openshift.io", Version: "v1beta1", Resource: "hostedclusters",
		Name: clusterName, Namespace: ns,
		Object: &hypershiftv1beta1.HostedCluster{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "hypershift.openshift.io/v1beta1",
				Kind:       "HostedCluster",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterName,
				Namespace: ns,
				Labels: map[string]string{
					"hyperfleet.io/cluster-id": clusterID,
				},
				Annotations: map[string]string{
					hypershiftv1beta1.PodSecurityAdmissionLabelOverrideAnnotation: "privileged",
					hypershiftv1beta1.ControlPlaneOperatorImageAnnotation:         "quay.io/cbusse_openshift/control-plane-operator:4.23-iam-auth",
					"hypershift.openshift.io/aws-iam-authenticator":               "true",
				},
			},
			Spec: hypershiftv1beta1.HostedClusterSpec{
				InfraID: clusterID,
				DNS: hypershiftv1beta1.DNSSpec{
					BaseDomain: fmt.Sprintf("%s.%s", h4, baseDomain),
				},
				Etcd: hypershiftv1beta1.EtcdSpec{
					ManagementType: hypershiftv1beta1.Managed,
					Managed: &hypershiftv1beta1.ManagedEtcdSpec{
						Storage: hypershiftv1beta1.ManagedEtcdStorageSpec{
							Type: hypershiftv1beta1.PersistentVolumeEtcdStorage,
							PersistentVolume: &hypershiftv1beta1.PersistentVolumeEtcdStorageSpec{
								Size:             ptr.To(resource.MustParse("32Gi")),
								StorageClassName: ptr.To("gp3"),
							},
						},
					},
				},
				FIPS:       true,
				Release:    cluster.Spec.Release,
				PullSecret: corev1.LocalObjectReference{Name: "pull-secret"},
				SSHKey:     corev1.LocalObjectReference{Name: "ssh-key"},
				Networking: hypershiftv1beta1.ClusterNetworking{
					ClusterNetwork: clusterNetwork,
					ServiceNetwork: serviceNetwork,
					MachineNetwork: machineNetwork,
					NetworkType:    hypershiftv1beta1.OVNKubernetes,
				},
				Platform: hypershiftv1beta1.PlatformSpec{
					Type: hypershiftv1beta1.AWSPlatform,
					AWS: &hypershiftv1beta1.AWSPlatformSpec{
						Region: cluster.Spec.Region,
						CloudProviderConfig: &hypershiftv1beta1.AWSCloudProviderConfig{
							VPC:  cluster.Spec.VpcID,
							Zone: zone,
							Subnet: &hypershiftv1beta1.AWSResourceReference{
								ID: ptr.To(strings.Join(cluster.Spec.PrivateSubnetIDs, ",")),
							},
						},
						EndpointAccess: hypershiftv1beta1.PublicAndPrivate,
						RolesRef:       roles,
						ResourceTags: []hypershiftv1beta1.AWSResourceTag{
							{Key: fmt.Sprintf("kubernetes.io/cluster/%s", clusterID), Value: "owned"},
							{Key: "red-hat-managed", Value: "true"},
						},
					},
				},
				KubeAPIServerDNSName: apiHost,
				Configuration: &hypershiftv1beta1.ClusterConfiguration{
					APIServer: &configv1.APIServerSpec{
						ServingCerts: configv1.APIServerServingCerts{
							NamedCertificates: []configv1.APIServerNamedServingCert{
								{
									ServingCertificate: configv1.SecretNameReference{
										Name: "api-serving-cert",
									},
								},
							},
						},
					},
				},
				IssuerURL: cluster.Spec.OIDCIssuerURL,
				Services: []hypershiftv1beta1.ServicePublishingStrategyMapping{
					{
						Service: hypershiftv1beta1.APIServer,
						ServicePublishingStrategy: hypershiftv1beta1.ServicePublishingStrategy{
							Type:  hypershiftv1beta1.Route,
							Route: &hypershiftv1beta1.RoutePublishingStrategy{Hostname: apiHost},
						},
					},
					{
						Service: hypershiftv1beta1.OAuthServer,
						ServicePublishingStrategy: hypershiftv1beta1.ServicePublishingStrategy{
							Type:  hypershiftv1beta1.Route,
							Route: &hypershiftv1beta1.RoutePublishingStrategy{Hostname: fmt.Sprintf("oauth.%s.%s.%s", clusterName, h4, baseDomain)},
						},
					},
					{
						Service: hypershiftv1beta1.Konnectivity,
						ServicePublishingStrategy: hypershiftv1beta1.ServicePublishingStrategy{
							Type: hypershiftv1beta1.Route,
						},
					},
					{
						Service: hypershiftv1beta1.Ignition,
						ServicePublishingStrategy: hypershiftv1beta1.ServicePublishingStrategy{
							Type: hypershiftv1beta1.Route,
						},
					},
				},
				InfrastructureAvailabilityPolicy: hypershiftv1beta1.HighlyAvailable,
				ControllerAvailabilityPolicy:     hypershiftv1beta1.HighlyAvailable,
			},
		},
	}, nil
}

func sshKey(clusterID, ns string) Resource {
	return Resource{
		Group: "", Version: "v1", Resource: "secrets",
		Name: "ssh-key", Namespace: ns,
		Object: &corev1.Secret{
			TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
			ObjectMeta: metav1.ObjectMeta{
				Name:      "ssh-key",
				Namespace: ns,
				Labels: map[string]string{
					"hyperfleet.io/cluster-id": clusterID,
				},
			},
			Type: corev1.SecretTypeOpaque,
			Data: map[string][]byte{
				"id_rsa.pub": {},
			},
		},
	}
}
