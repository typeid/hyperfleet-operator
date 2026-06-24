# Quickstart

Deploy the hyperfleet-operator on a Regional Cluster (EKS) and connect it to fleet-db and DynamoDB.

## Prerequisites

- An EKS cluster to run the operator (the Regional Cluster)
- A fleet-db EKS cluster (workerless, kube-apiserver only)
- DynamoDB tables for each management cluster
- An IAM role for the operator with permissions described below

## 1. IAM Role

Create an IAM role that the operator's ServiceAccount will assume via [EKS Pod Identity](https://docs.aws.amazon.com/eks/latest/userguide/pod-identities.html) or IRSA. The role needs two sets of permissions:

### EKS (fleet-db access)

The operator calls `DescribeCluster` to discover fleet-db's API endpoint and CA, then authenticates using presigned STS tokens.

```json
{
  "Effect": "Allow",
  "Action": ["eks:DescribeCluster"],
  "Resource": "arn:aws:eks:<region>:<account>:cluster/<fleet-db-cluster-name>"
}
```

Register this role as an [EKS access entry](https://docs.aws.amazon.com/eks/latest/userguide/access-entries.html) on the fleet-db cluster with Kubernetes RBAC permissions for the `hyperfleet.io` API group (the CRDs the operator watches and manages).

### DynamoDB

Per management cluster, there are 6 tables. The operator writes to specs tables and reads from status tables:

| Action            | Tables                                                                                           |
| ----------------- | ------------------------------------------------------------------------------------------------ |
| `dynamodb:PutItem` | `{mc}-specs-applydesires`, `{mc}-specs-deletedesires`, `{mc}-specs-readdesires`                  |
| `dynamodb:GetItem` | `{mc}-status-applydesires`, `{mc}-status-deletedesires`, `{mc}-status-readdesires`               |

The role should **not** have write access to status tables or read access to specs tables — that is kube-applier-aws's domain.

### STS (token generation)

The operator presigns `sts:GetCallerIdentity` requests as bearer tokens for EKS authentication. The IAM role must be allowed to call STS (this is typically already permitted by default).

## 2. DynamoDB Tables

Create 6 DynamoDB tables per management cluster in the **regional account**, each with a single string partition key `documentID`:

```
{mc}-specs-applydesires
{mc}-specs-deletedesires
{mc}-specs-readdesires
{mc}-status-applydesires
{mc}-status-deletedesires
{mc}-status-readdesires
```

Use on-demand billing (`PAY_PER_REQUEST`).

Tables live in the regional account so the operator accesses DynamoDB locally. Each kube-applier-aws instance on a management cluster assumes a cross-account role to read specs and write statuses back.

## 3. fleet-db EKS Access Entry

The operator authenticates to fleet-db using IAM. Register the operator's IAM role as an access entry on the fleet-db cluster:

```bash
aws eks create-access-entry \
  --cluster-name <fleet-db-cluster-name> \
  --principal-arn <operator-iam-role-arn> \
  --type STANDARD

aws eks associate-access-policy \
  --cluster-name <fleet-db-cluster-name> \
  --principal-arn <operator-iam-role-arn> \
  --policy-arn arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy \
  --access-scope type=namespace,namespaces=default
```

For production, scope the access policy to only the `hyperfleet.io` API group using a custom Kubernetes RBAC ClusterRole and binding instead of the cluster admin policy.

## 4. Install the Helm Chart

```bash
helm install hyperfleet-operator charts/hyperfleet-operator \
  --namespace hyperfleet-system --create-namespace \
  --set awsRegion=us-east-1 \
  --set fleetDBClusterName=fleet-db-prod \
  --set serviceAccount.annotations."eks\.amazonaws\.com/role-arn"=arn:aws:iam::<account>:role/<operator-role>
```

### Required Values

| Value              | Description                                         |
| ------------------ | --------------------------------------------------- |
| `awsRegion`        | AWS region for DynamoDB and EKS DescribeCluster      |
| `fleetDBClusterName` | EKS cluster name for fleet-db                      |

### Optional Values

| Value                        | Default                                          | Description                          |
| ---------------------------- | ------------------------------------------------ | ------------------------------------ |
| `image.repository`           | `quay.io/cbusse_openshift/hyperfleet-operator`   | Container image                      |
| `image.tag`                  | `latest`                                         | Image tag                            |
| `leaderElection.enabled`     | `true`                                           | Enable leader election               |
| `serviceAccount.annotations` | `{}`                                             | SA annotations (set IAM role ARN)    |
| `replicaCount`               | `1`                                              | Number of replicas                   |

## 5. Install CRDs

The operator's CRDs must be installed on fleet-db (not on the Regional Cluster where the operator runs):

```bash
kubectl apply --kubeconfig <fleet-db-kubeconfig> -f charts/hyperfleet-operator/crds/
```

Or deploy them via ArgoCD targeting the fleet-db cluster.

## Verify

Check the operator logs:

```bash
kubectl logs -n hyperfleet-system deploy/hyperfleet-operator -f
```

Create a test Cluster CR on fleet-db and verify the operator creates a Placement and writes ApplyDesires to DynamoDB:

```bash
kubectl get clusters.hyperfleet.io --kubeconfig <fleet-db-kubeconfig>
kubectl get placements.hyperfleet.io --kubeconfig <fleet-db-kubeconfig>
```
