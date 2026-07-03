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

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/typeid/hyperfleet-operator/internal/controller"
	"github.com/typeid/hyperfleet-operator/internal/dynamo"
	"github.com/typeid/hyperfleet-operator/internal/dynamo/statusstream"
	"github.com/typeid/hyperfleet-operator/internal/mcconfig"
	"github.com/typeid/hyperfleet-operator/internal/render"
	"github.com/typeid/hyperfleet-operator/pkg/fleetstore"
)

var setupLog = ctrl.Log.WithName("setup")

func main() {
	var metricsAddr string
	var probeAddr string
	var awsRegion string
	var baseDomain string
	var maxConcurrentReconciles int

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metrics endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.StringVar(&awsRegion, "aws-region", "", "AWS region for DynamoDB and EKS (required).")
	flag.StringVar(&baseDomain, "base-domain", "", "DNS base domain for hosted clusters (required).")
	flag.IntVar(&maxConcurrentReconciles, "max-concurrent-reconciles", 10, "Maximum number of concurrent reconciles per controller.")

	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	if awsRegion == "" {
		setupLog.Error(nil, "--aws-region is required")
		os.Exit(1)
	}
	if baseDomain == "" {
		setupLog.Error(nil, "--base-domain is required")
		os.Exit(1)
	}

	dsn := os.Getenv("FLEETSTORE_DSN")
	if dsn == "" {
		setupLog.Error(nil, "FLEETSTORE_DSN environment variable is required")
		os.Exit(1)
	}

	ctx := context.Background()

	// Load AWS config — credentials come from Pod Identity / IRSA automatically.
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(awsRegion))
	if err != nil {
		setupLog.Error(err, "Failed to load AWS config")
		os.Exit(1)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	// Create FleetManager (replaces fleet-db EKS).
	fm, err := fleetstore.NewFleetManager(ctx, fleetstore.Options{
		DSN:                    dsn,
		Logger:                 logger,
		MetricsBindAddress:     metricsAddr,
		HealthProbeBindAddress: probeAddr,
	})
	if err != nil {
		setupLog.Error(err, "Failed to create FleetManager")
		os.Exit(1)
	}
	defer fm.Close()

	// Leader election via Postgres lease.
	le := fleetstore.NewLeaderElector(fm.Pool, fleetstore.DefaultLeaderConfig(), logger)
	if err := le.Acquire(ctx); err != nil {
		setupLog.Error(err, "Failed to acquire leader lease")
		os.Exit(1)
	}
	go le.Run(ctx)

	// Auditor for rolling consistency checks and tombstone cleanup.
	auditor := fleetstore.NewAuditor(fm.Pool, fm.Watcher, fm.Stores, fleetstore.DefaultAuditConfig(), logger)
	auditor.Run(ctx)

	mgr := fm.Manager

	dynamoDBClient := dynamodb.NewFromConfig(awsCfg)
	dynamoClient := dynamo.NewClient(dynamoDBClient)
	streamsClient := dynamodbstreams.NewFromConfig(awsCfg)

	// MC config loader reads ManagementCluster CRs from the FleetStore cache.
	mcLoader := mcconfig.NewStoreLoader(fm.Cache)

	rcfg := render.RegionalConfig{
		BaseDomain: baseDomain,
		AWSRegion:  awsRegion,
	}

	eventRouter := controller.NewEventRouter()
	clusterStatusEvents := make(chan event.GenericEvent, 256)
	nodePoolStatusEvents := make(chan event.GenericEvent, 256)
	manifestStatusEvents := make(chan event.GenericEvent, 256)

	if err := (&controller.ClusterReconciler{
		Client:                  mgr.GetClient(),
		Scheme:                  mgr.GetScheme(),
		Dynamo:                  dynamoClient,
		RegionalConfig:          rcfg,
		StatusEvents:            clusterStatusEvents,
		EventRouter:             eventRouter,
		MaxConcurrentReconciles: maxConcurrentReconciles,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "cluster")
		os.Exit(1)
	}
	if err := (&controller.NodePoolReconciler{
		Client:                  mgr.GetClient(),
		Scheme:                  mgr.GetScheme(),
		Dynamo:                  dynamoClient,
		StatusEvents:            nodePoolStatusEvents,
		EventRouter:             eventRouter,
		MaxConcurrentReconciles: maxConcurrentReconciles,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "nodepool")
		os.Exit(1)
	}
	if err := (&controller.PlacementReconciler{
		Client:                  mgr.GetClient(),
		Scheme:                  mgr.GetScheme(),
		MCConfig:                mcLoader,
		MaxConcurrentReconciles: maxConcurrentReconciles,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "placement")
		os.Exit(1)
	}
	if err := (&controller.ManifestReconciler{
		Client:                  mgr.GetClient(),
		Scheme:                  mgr.GetScheme(),
		Dynamo:                  dynamoClient,
		StatusEvents:            manifestStatusEvents,
		EventRouter:             eventRouter,
		MaxConcurrentReconciles: maxConcurrentReconciles,
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

	streamMgr := statusstream.NewManager(
		dynamoDBClient,
		streamsClient,
		mcLoader,
		[]string{dynamo.TableSuffixStatusApplyDesires, dynamo.TableSuffixStatusReadDesires},
		func(documentID string) { eventRouter.Dispatch(documentID) },
		slog.Default().With("component", "statusstream"),
	)
	watchCtx, watchCancel := context.WithCancel(context.Background())
	defer watchCancel()
	go streamMgr.Run(watchCtx, 5*time.Second)

	setupLog.Info("Starting FleetStore manager")
	if err := fm.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Failed to run manager")
		os.Exit(1)
	}
}
