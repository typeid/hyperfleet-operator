//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	hyperfleetv1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
	crdbases "github.com/typeid/hyperfleet-operator/config/crd/bases"
	"github.com/typeid/hyperfleet-operator/internal/controller"
	"github.com/typeid/hyperfleet-operator/internal/crdinstall"
	dynamo "github.com/typeid/hyperfleet-operator/internal/dynamo"
	"github.com/typeid/hyperfleet-operator/internal/mcconfig"
	"github.com/typeid/hyperfleet-operator/internal/render"
)

const (
	containerName = "hyperfleet-e2e-dynamodb"
	mc            = "mc01"
)

var (
	ctx         context.Context
	cancel      context.CancelFunc
	testEnv     *envtest.Environment
	cfg         *rest.Config
	k8sClient   client.Client
	dynamoDBCli *dynamodb.Client
	dynamoCli   *dynamo.Client
	ddbPort     string
)

func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	SetDefaultEventuallyTimeout(30 * time.Second)
	SetDefaultEventuallyPollingInterval(500 * time.Millisecond)
	RunSpecs(t, "E2E Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))
	ctx, cancel = context.WithCancel(context.TODO())

	By("finding a free port for DynamoDB Local")
	ddbPort = freePort()

	By("starting DynamoDB Local container")
	containerTool := os.Getenv("CONTAINER_TOOL")
	if containerTool == "" {
		containerTool = "podman"
	}
	cmd := exec.Command(containerTool, "run", "-d", "--rm",
		"--name", containerName,
		"-p", fmt.Sprintf("%s:8000", ddbPort),
		"amazon/dynamodb-local",
	)
	out, err := cmd.CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), "start DynamoDB Local: %s", string(out))

	Eventually(func() error {
		conn, err := net.DialTimeout("tcp", "127.0.0.1:"+ddbPort, time.Second)
		if err != nil {
			return err
		}
		conn.Close()
		return nil
	}, 30*time.Second, 500*time.Millisecond).Should(Succeed(), "DynamoDB Local did not become ready")

	By("creating DynamoDB tables")
	dynamoDBCli = dynamodb.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", "test"),
		BaseEndpoint: aws.String(fmt.Sprintf("http://127.0.0.1:%s", ddbPort)),
	})
	createTables(dynamoDBCli)
	dynamoCli = dynamo.NewClient(dynamoDBCli)

	By("bootstrapping envtest")
	Expect(hyperfleetv1alpha1.AddToScheme(scheme.Scheme)).To(Succeed())

	testEnv = &envtest.Environment{}
	if dir := firstEnvTestBinDir(); dir != "" {
		testEnv.BinaryAssetsDirectory = dir
	}

	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	By("installing CRDs via crdinstall (same path as production)")
	Expect(crdinstall.Install(ctx, cfg, "hyperfleet-system", crdbases.YAMLs)).To(Succeed())

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())

	By("creating test namespace")
	Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "111222333444"}})).To(Succeed())

	By("verifying DynamoDB connectivity")
	_, err = dynamoDBCli.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(mc + "-specs-applydesires"),
		Item: map[string]dynamodbtypes.AttributeValue{
			"documentID": &dynamodbtypes.AttributeValueMemberS{Value: "connectivity-check"},
		},
	})
	Expect(err).NotTo(HaveOccurred(), "DynamoDB connectivity check failed")
	_, err = dynamoDBCli.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(mc + "-specs-applydesires"),
		Key: map[string]dynamodbtypes.AttributeValue{
			"documentID": &dynamodbtypes.AttributeValueMemberS{Value: "connectivity-check"},
		},
	})
	Expect(err).NotTo(HaveOccurred())

	By("creating MC config ConfigMap")
	Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: mcconfig.ConfigMapNamespace}})).To(Succeed())
	Expect(k8sClient.Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mcconfig.ConfigMapName,
			Namespace: mcconfig.ConfigMapNamespace,
		},
		Data: map[string]string{
			mcconfig.ConfigMapKey: "- id: mc01\n  region: us-east-1\n  accountId: \"111222333444\"\n",
		},
	})).To(Succeed())

	mcLoader := mcconfig.NewLoader(k8sClient)
	Expect(mcLoader.Reload(ctx)).To(Succeed())

	By("starting controller manager")
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme.Scheme,
	})
	Expect(err).NotTo(HaveOccurred())

	Expect((&controller.PlacementReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		MCConfig: mcLoader,
	}).SetupWithManager(mgr)).To(Succeed())

	Expect((&controller.ClusterReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Dynamo: dynamoCli,
		RegionalConfig: render.RegionalConfig{
			BaseDomain: "e2e.example.com",
			AWSRegion:  "us-east-1",
		},
	}).SetupWithManager(mgr)).To(Succeed())

	Expect((&controller.NodePoolReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Dynamo: dynamoCli,
	}).SetupWithManager(mgr)).To(Succeed())

	Expect((&controller.ManifestReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Dynamo: dynamoCli,
	}).SetupWithManager(mgr)).To(Succeed())

	go func() {
		defer GinkgoRecover()
		Expect(mgr.Start(ctx)).To(Succeed())
	}()

	// Simulate kube-applier-aws: poll specs-applydesires and write status
	// entries with Successful=True so controllers see apply confirmations.
	go func() {
		defer GinkgoRecover()
		specsTable := mc + "-specs-applydesires"
		statusTable := mc + "-status-applydesires"

		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				out, err := dynamoDBCli.Scan(ctx, &dynamodb.ScanInput{
					TableName: aws.String(specsTable),
				})
				if err != nil {
					continue
				}
				for _, item := range out.Items {
					docID, ok := item["documentID"]
					if !ok {
						continue
					}
					// Mirror real kube-applier: copy the spec's updateTime
					// into ObservedDesireUpdateTime so the wrong-generation
					// check sees the status as current.
					var observedTime time.Time
					if ut, ok := item["updateTime"]; ok {
						if sv, ok := ut.(*dynamodbtypes.AttributeValueMemberS); ok {
							observedTime, _ = time.Parse(time.RFC3339, sv.Value)
						}
					}
					completeStatus := dynamo.ApplyDesireStatus{
						ObservedDesireUpdateTime: observedTime,
						Conditions: []metav1.Condition{{
							Type:               dynamo.DesireConditionSuccessful,
							Status:             metav1.ConditionTrue,
							Reason:             "NoErrors",
							LastTransitionTime: metav1.Now(),
						}},
					}
					statusAttrs, err := attributevalue.MarshalMap(completeStatus)
					if err != nil {
						continue
					}
					statusItem := map[string]dynamodbtypes.AttributeValue{
						"documentID": docID,
						"status":     &dynamodbtypes.AttributeValueMemberM{Value: statusAttrs},
					}
					_, _ = dynamoDBCli.PutItem(ctx, &dynamodb.PutItemInput{
						TableName:           aws.String(statusTable),
						Item:                statusItem,
						ConditionExpression: aws.String("attribute_not_exists(documentID)"),
					})
				}
			}
		}
	}()

	// Simulate kube-applier-aws: poll specs-deletedesires and write status
	// entries with Successful=True so controllers see deletion confirmations.
	go func() {
		defer GinkgoRecover()
		specsTable := mc + "-specs-deletedesires"
		statusTable := mc + "-status-deletedesires"

		completeStatus := dynamo.DeleteDesireStatus{
			Conditions: []metav1.Condition{{
				Type:               dynamo.DesireConditionSuccessful,
				Status:             metav1.ConditionTrue,
				Reason:             "NoErrors",
				LastTransitionTime: metav1.Now(),
			}},
		}
		statusAttrs, err := attributevalue.MarshalMap(completeStatus)
		Expect(err).NotTo(HaveOccurred())

		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				out, err := dynamoDBCli.Scan(ctx, &dynamodb.ScanInput{
					TableName: aws.String(specsTable),
				})
				if err != nil {
					continue
				}
				for _, item := range out.Items {
					docID, ok := item["documentID"]
					if !ok {
						continue
					}
					statusItem := map[string]dynamodbtypes.AttributeValue{
						"documentID": docID,
						"status":     &dynamodbtypes.AttributeValueMemberM{Value: statusAttrs},
					}
					_, _ = dynamoDBCli.PutItem(ctx, &dynamodb.PutItemInput{
						TableName:           aws.String(statusTable),
						Item:                statusItem,
						ConditionExpression: aws.String("attribute_not_exists(documentID)"),
					})
				}
			}
		}
	}()

	// Simulate kube-applier-aws: poll specs-readdesires and write status
	// entries with fabricated KubeContent (a completed Job).
	go func() {
		defer GinkgoRecover()
		specsTable := mc + "-specs-readdesires"
		statusTable := mc + "-status-readdesires"
		completedJob := []byte(`{"apiVersion":"batch/v1","kind":"Job","metadata":{"name":"e2e-job-abc123","namespace":"e2e-actions"},"status":{"succeeded":1,"startTime":"2026-06-25T10:00:00Z","completionTime":"2026-06-25T10:00:05Z"}}`)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				out, err := dynamoDBCli.Scan(ctx, &dynamodb.ScanInput{
					TableName: aws.String(specsTable),
				})
				if err != nil {
					continue
				}
				for _, item := range out.Items {
					docID, ok := item["documentID"]
					if !ok {
						continue
					}
					_, _ = dynamoDBCli.PutItem(ctx, &dynamodb.PutItemInput{
						TableName: aws.String(statusTable),
						Item: map[string]dynamodbtypes.AttributeValue{
							"documentID":  docID,
							"kubeContent": &dynamodbtypes.AttributeValueMemberB{Value: completedJob},
						},
						ConditionExpression: aws.String("attribute_not_exists(documentID)"),
					})
				}
			}
		}
	}()
})

var _ = AfterSuite(func() {
	By("stopping controller manager")
	cancel()

	By("stopping envtest")
	if testEnv != nil {
		Eventually(func() error {
			return testEnv.Stop()
		}, time.Minute, time.Second).Should(Succeed())
	}

	By("stopping DynamoDB Local container")
	containerTool := os.Getenv("CONTAINER_TOOL")
	if containerTool == "" {
		containerTool = "podman"
	}
	_ = exec.Command(containerTool, "rm", "-f", containerName).Run()
})

func freePort() string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return fmt.Sprintf("%d", port)
}

func createTables(db *dynamodb.Client) {
	suffixes := []string{"-applydesires", "-deletedesires", "-readdesires"}
	prefixes := []string{mc + "-specs", mc + "-status"}

	for _, prefix := range prefixes {
		for _, suffix := range suffixes {
			tableName := prefix + suffix
			_, err := db.CreateTable(context.Background(), &dynamodb.CreateTableInput{
				TableName: aws.String(tableName),
				AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
					{
						AttributeName: aws.String("documentID"),
						AttributeType: dynamodbtypes.ScalarAttributeTypeS,
					},
				},
				KeySchema: []dynamodbtypes.KeySchemaElement{
					{
						AttributeName: aws.String("documentID"),
						KeyType:       dynamodbtypes.KeyTypeHash,
					},
				},
				BillingMode: dynamodbtypes.BillingModePayPerRequest,
			})
			Expect(err).NotTo(HaveOccurred(), "create table %s", tableName)
		}
	}
}

func firstEnvTestBinDir() string {
	basePath := filepath.Join("..", "..", "bin", "k8s")
	entries, err := os.ReadDir(basePath)
	if err != nil {
		return ""
	}
	for _, entry := range entries {
		if entry.IsDir() {
			return filepath.Join(basePath, entry.Name())
		}
	}
	return ""
}
