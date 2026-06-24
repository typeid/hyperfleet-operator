package manifest

import (
	"fmt"

	hyperfleetv1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
)

const taskKey = "hyperfleet-operator"

// TODO: Replace map[string]any manifest construction with typed structs from upstream packages:
//   - github.com/openshift/hypershift/api/hypershift/v1beta1 (HostedCluster, NodePool)
//   - github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1 (Certificate)
//   - github.com/external-secrets/external-secrets/apis/externalsecrets/v1 (ExternalSecret)
// This gives compile-time type safety and automatic schema tracking when upstream types change.

func hash4(clusterID string) string {
	if len(clusterID) < 4 {
		return clusterID
	}
	return clusterID[:4]
}

func clusterNamespace(clusterID string) string {
	return fmt.Sprintf("clusters-%s", clusterID)
}

// Manifest is a generated Kubernetes resource with its GVR for desire creation.
type Manifest struct {
	Group    string
	Version  string
	Resource string
	Name     string
	Object   map[string]any
}

// ClusterManifests generates the 7 Kubernetes resources for a cluster on the MC.
// This matches the current manifestwork.yaml exactly (minus the NodePool, which
// is created by the NodePool controller).
func ClusterManifests(cluster *hyperfleetv1alpha1.Cluster) []Manifest {
	clusterID := cluster.Name
	clusterName := cluster.Spec.Name
	ns := clusterNamespace(clusterID)
	h4 := hash4(clusterID)
	baseDomain := cluster.Spec.BaseDomain

	return []Manifest{
		namespace(clusterID, ns),
		clusterConfig(clusterID, clusterName, ns),
		awsIAMAuthConfig(clusterID, clusterName, ns, cluster.Spec.CreatorARN),
		pullSecret(clusterID, ns),
		apiServingCert(clusterID, clusterName, h4, baseDomain, ns),
		hostedCluster(cluster, h4),
		sshKey(clusterID, ns),
	}
}

func namespace(clusterID, ns string) Manifest {
	return Manifest{
		Group: "", Version: "v1", Resource: "namespaces", Name: ns,
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Namespace",
			"metadata": map[string]any{
				"name": ns,
				"labels": map[string]string{
					"hyperfleet.io/cluster-id":    clusterID,
					"hyperfleet.io/managed-by":    "hyperfleet-operator",
					"hyperfleet.io/resource-type": "namespace",
				},
			},
		},
	}
}

func clusterConfig(clusterID, clusterName, ns string) Manifest {
	return Manifest{
		Group: "", Version: "v1", Resource: "configmaps", Name: "cluster-config",
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":      "cluster-config",
				"namespace": ns,
				"labels": map[string]string{
					"hyperfleet.io/cluster-id": clusterID,
				},
			},
			"data": map[string]string{
				"cluster_id":   clusterID,
				"cluster_name": clusterName,
			},
		},
	}
}

func awsIAMAuthConfig(clusterID, clusterName, ns, creatorARN string) Manifest {
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

	return Manifest{
		Group: "", Version: "v1", Resource: "configmaps", Name: "aws-iam-auth-config",
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":      "aws-iam-auth-config",
				"namespace": ns,
				"labels": map[string]string{
					"hyperfleet.io/cluster-id": clusterID,
				},
				"annotations": map[string]string{
					"hypershift.openshift.io/cluster": fmt.Sprintf("%s/%s", ns, clusterName),
				},
			},
			"data": map[string]string{
				"config.yaml": configYAML,
			},
		},
	}
}

func pullSecret(clusterID, ns string) Manifest {
	return Manifest{
		Group: "external-secrets.io", Version: "v1", Resource: "externalsecrets", Name: "pull-secret",
		Object: map[string]any{
			"apiVersion": "external-secrets.io/v1",
			"kind":       "ExternalSecret",
			"metadata": map[string]any{
				"name":      "pull-secret",
				"namespace": ns,
				"labels": map[string]string{
					"hyperfleet.io/cluster-id":    clusterID,
					"hyperfleet.io/resource-type": "pull-secret",
				},
			},
			"spec": map[string]any{
				"refreshInterval": "1h",
				"secretStoreRef": map[string]string{
					"name": "aws-parameter-store",
					"kind": "ClusterSecretStore",
				},
				"target": map[string]any{
					"name":           "pull-secret",
					"creationPolicy": "Orphan",
					"template": map[string]string{
						"type": "kubernetes.io/dockerconfigjson",
					},
				},
				"data": []map[string]any{
					{
						"secretKey": ".dockerconfigjson",
						"remoteRef": map[string]string{
							"key": "/infra/pull-secret",
						},
					},
				},
			},
		},
	}
}

func apiServingCert(clusterID, clusterName, h4, baseDomain, ns string) Manifest {
	return Manifest{
		Group: "cert-manager.io", Version: "v1", Resource: "certificates", Name: "api-serving-cert",
		Object: map[string]any{
			"apiVersion": "cert-manager.io/v1",
			"kind":       "Certificate",
			"metadata": map[string]any{
				"name":      "api-serving-cert",
				"namespace": ns,
				"labels": map[string]string{
					"hyperfleet.io/cluster-id": clusterID,
				},
			},
			"spec": map[string]any{
				"secretName": "api-serving-cert",
				"issuerRef": map[string]string{
					"name": "letsencrypt-dns01",
					"kind": "ClusterIssuer",
				},
				"dnsNames": []string{
					fmt.Sprintf("*.%s.%s.%s", clusterName, h4, baseDomain),
				},
			},
		},
	}
}

func hostedCluster(cluster *hyperfleetv1alpha1.Cluster, h4 string) Manifest {
	clusterID := cluster.Name
	clusterName := cluster.Spec.Name
	ns := clusterNamespace(clusterID)
	baseDomain := cluster.Spec.BaseDomain

	clusterNetwork := make([]map[string]string, 0, len(cluster.Spec.Networking.ClusterNetwork))
	for _, n := range cluster.Spec.Networking.ClusterNetwork {
		clusterNetwork = append(clusterNetwork, map[string]string{"cidr": n.CIDR})
	}
	serviceNetwork := make([]map[string]string, 0, len(cluster.Spec.Networking.ServiceNetwork))
	for _, n := range cluster.Spec.Networking.ServiceNetwork {
		serviceNetwork = append(serviceNetwork, map[string]string{"cidr": n.CIDR})
	}
	machineNetwork := make([]map[string]string, 0, len(cluster.Spec.Networking.MachineNetwork))
	for _, n := range cluster.Spec.Networking.MachineNetwork {
		machineNetwork = append(machineNetwork, map[string]string{"cidr": n.CIDR})
	}

	roles := cluster.Spec.Platform.AWS.Roles

	return Manifest{
		Group: "hypershift.openshift.io", Version: "v1beta1", Resource: "hostedclusters", Name: clusterName,
		Object: map[string]any{
			"apiVersion": "hypershift.openshift.io/v1beta1",
			"kind":       "HostedCluster",
			"metadata": map[string]any{
				"name":      clusterName,
				"namespace": ns,
				"labels": map[string]string{
					"hyperfleet.io/cluster-id": clusterID,
				},
				"annotations": map[string]string{
					"hypershift.openshift.io/pod-security-admission-label-override": "privileged",
					"hypershift.openshift.io/control-plane-operator-image":          "quay.io/cbusse_openshift/control-plane-operator:4.23-iam-auth",
					"hypershift.openshift.io/aws-iam-authenticator":                 "true",
				},
			},
			"spec": map[string]any{
				"infraID": clusterID,
				"dns": map[string]string{
					"baseDomain": fmt.Sprintf("%s.%s", h4, baseDomain),
				},
				"etcd": map[string]any{
					"managed": map[string]any{
						"storage": map[string]any{
							"persistentVolume": map[string]any{
								"size":             "32Gi",
								"storageClassName": "gp3",
							},
							"type": "PersistentVolume",
						},
					},
					"managementType": "Managed",
				},
				"fips": true,
				"release": map[string]string{
					"image": cluster.Spec.Release.Image,
				},
				"pullSecret": map[string]string{"name": "pull-secret"},
				"sshKey":     map[string]string{"name": "ssh-key"},
				"networking": map[string]any{
					"clusterNetwork": clusterNetwork,
					"serviceNetwork": serviceNetwork,
					"machineNetwork": machineNetwork,
					"networkType":    "OVNKubernetes",
				},
				"platform": map[string]any{
					"type": "AWS",
					"aws": map[string]any{
						"region": cluster.Spec.Region,
						"cloudProviderConfig": map[string]any{
							"vpc":  cluster.Spec.VpcID,
							"zone": cluster.Spec.Zone,
							"subnet": map[string]string{
								"id": cluster.Spec.PrivateSubnetIds,
							},
						},
						"endpointAccess": "PublicAndPrivate",
						"rolesRef": map[string]string{
							"controlPlaneOperatorARN": roles.ControlPlaneOperatorARN,
							"ingressARN":              roles.IngressARN,
							"imageRegistryARN":        roles.ImageRegistryARN,
							"kubeCloudControllerARN":  roles.KubeCloudControllerARN,
							"nodePoolManagementARN":   roles.NodePoolManagementARN,
							"networkARN":              roles.NetworkARN,
							"storageARN":              roles.StorageARN,
						},
						"resourceTags": []map[string]string{
							{"key": fmt.Sprintf("kubernetes.io/cluster/%s", clusterID), "value": "owned"},
							{"key": "red-hat-managed", "value": "true"},
						},
					},
				},
				"kubeAPIServerDNSName": fmt.Sprintf("api.%s.%s.%s", clusterName, h4, baseDomain),
				"configuration": map[string]any{
					"apiServer": map[string]any{
						"servingCerts": map[string]any{
							"namedCertificates": []map[string]any{
								{
									"servingCertificate": map[string]string{
										"name": "api-serving-cert",
									},
								},
							},
						},
					},
				},
				"issuerURL": cluster.Spec.OIDCIssuerURL,
				"services": []map[string]any{
					{
						"service": "APIServer",
						"servicePublishingStrategy": map[string]any{
							"type": "Route",
							"route": map[string]string{
								"hostname": fmt.Sprintf("api.%s.%s.%s", clusterName, h4, baseDomain),
							},
						},
					},
					{
						"service": "OAuthServer",
						"servicePublishingStrategy": map[string]any{
							"type": "Route",
							"route": map[string]string{
								"hostname": fmt.Sprintf("oauth.%s.%s.%s", clusterName, h4, baseDomain),
							},
						},
					},
					{
						"service":                   "Konnectivity",
						"servicePublishingStrategy": map[string]any{"type": "Route"},
					},
					{
						"service":                   "Ignition",
						"servicePublishingStrategy": map[string]any{"type": "Route"},
					},
				},
				"infrastructureAvailabilityPolicy": "HighlyAvailable",
				"controllerAvailabilityPolicy":     "HighlyAvailable",
			},
		},
	}
}

func sshKey(clusterID, ns string) Manifest {
	return Manifest{
		Group: "", Version: "v1", Resource: "secrets", Name: "ssh-key",
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Secret",
			"metadata": map[string]any{
				"name":      "ssh-key",
				"namespace": ns,
				"labels": map[string]string{
					"hyperfleet.io/cluster-id": clusterID,
				},
			},
			"type": "Opaque",
			"data": map[string]string{
				"id_rsa.pub": "",
			},
		},
	}
}
