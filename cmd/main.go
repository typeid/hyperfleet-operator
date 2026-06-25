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

package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	_ "k8s.io/client-go/plugin/pkg/client/auth"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	hyperfleetv1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
	crdbases "github.com/typeid/hyperfleet-operator/config/crd/bases"
	"github.com/typeid/hyperfleet-operator/internal/controller"
	"github.com/typeid/hyperfleet-operator/internal/crdinstall"
	"github.com/typeid/hyperfleet-operator/internal/dynamo"
	"github.com/typeid/hyperfleet-operator/internal/eksauth"
	"github.com/typeid/hyperfleet-operator/internal/mcconfig"
	"github.com/typeid/hyperfleet-operator/internal/render"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(hyperfleetv1alpha1.AddToScheme(scheme))
}

func main() {
	var metricsAddr string
	var probeAddr string
	var enableLeaderElection bool
	var awsRegion string
	var fleetDBClusterName string
	var baseDomain string

	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", true,
		"Enable leader election for controller manager.")
	flag.StringVar(&awsRegion, "aws-region", "", "AWS region for DynamoDB and EKS (required).")
	flag.StringVar(&fleetDBClusterName, "fleet-db-cluster-name", "", "EKS cluster name for fleet-db (required).")
	flag.StringVar(&baseDomain, "base-domain", "", "DNS base domain for hosted clusters (required).")

	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	if awsRegion == "" {
		setupLog.Error(nil, "--aws-region is required")
		os.Exit(1)
	}
	if fleetDBClusterName == "" {
		setupLog.Error(nil, "--fleet-db-cluster-name is required")
		os.Exit(1)
	}
	if baseDomain == "" {
		setupLog.Error(nil, "--base-domain is required")
		os.Exit(1)
	}

	ctx := context.Background()

	// Load AWS config — credentials come from Pod Identity / IRSA automatically.
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(awsRegion))
	if err != nil {
		setupLog.Error(err, "Failed to load AWS config")
		os.Exit(1)
	}

	// Build REST config for fleet-db using IAM authentication.
	fleetDBConfig, err := eksauth.NewRESTConfig(ctx, awsCfg, fleetDBClusterName)
	if err != nil {
		setupLog.Error(err, "Failed to build fleet-db REST config")
		os.Exit(1)
	}

	// Ensure namespaces and CRDs exist on fleet-db before starting informers.
	if err := crdinstall.Install(ctx, fleetDBConfig, "hyperfleet-system", crdbases.YAMLs); err != nil {
		setupLog.Error(err, "Failed to install CRDs on fleet-db")
		os.Exit(1)
	}
	if err := crdinstall.EnsureNamespace(ctx, fleetDBConfig, "zoa-jobs"); err != nil {
		setupLog.Error(err, "Failed to ensure zoa-jobs namespace on fleet-db")
		os.Exit(1)
	}

	dynamoClient := dynamo.NewClient(dynamodb.NewFromConfig(awsCfg))

	mgr, err := ctrl.NewManager(fleetDBConfig, ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: metricsAddr,
		},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "c9f76021.hyperfleet.io",
	})
	if err != nil {
		setupLog.Error(err, "Failed to start manager")
		os.Exit(1)
	}

	// The mcconfig loader reads from the local RC cluster (in-cluster config),
	// not fleet-db, because the management-clusters ConfigMap lives on the RC.
	localClient, err := client.NewWithWatch(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		setupLog.Error(err, "Failed to create local cluster client")
		os.Exit(1)
	}
	mcLoader := mcconfig.NewLoader(localClient)

	rcfg := render.RegionalConfig{
		BaseDomain: baseDomain,
		AWSRegion:  awsRegion,
	}

	if err := (&controller.ClusterReconciler{
		Client:        mgr.GetClient(),
		Scheme:        mgr.GetScheme(),
		Dynamo:        dynamoClient,
		RegionalConfig: rcfg,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "cluster")
		os.Exit(1)
	}
	if err := (&controller.NodePoolReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Dynamo: dynamoClient,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "nodepool")
		os.Exit(1)
	}
	if err := (&controller.PlacementReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		MCConfig: mcLoader,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "placement")
		os.Exit(1)
	}

	if err := (&controller.HyperFleetManifestReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Dynamo: dynamoClient,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "manifest")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up ready check")
		os.Exit(1)
	}

	watchCtx, watchCancel := context.WithCancel(context.Background())
	defer watchCancel()
	go mcLoader.Watch(watchCtx, 5*time.Second, slog.Default())

	setupLog.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Failed to run manager")
		os.Exit(1)
	}
}
