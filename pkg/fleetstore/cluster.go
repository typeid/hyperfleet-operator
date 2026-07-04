package fleetstore

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	v1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
)

// Options configures a FleetManager.
type Options struct {
	DSN    string
	Logger *slog.Logger

	MetricsBindAddress     string
	HealthProbeBindAddress string

	WatchConfig WatchConfig
}

// FleetManager holds all FleetStore components and the controller-runtime manager.
type FleetManager struct {
	Pool    *pgxpool.Pool
	Client  *Client
	Cache   *Cache
	Watcher *Watcher
	Stores  map[string]*InformerStore
	Manager manager.Manager
	Logger  *slog.Logger
}

// NewFleetManager creates the FleetStore infrastructure and a controller-runtime
// manager wired to use it.
func NewFleetManager(ctx context.Context, opts Options) (*FleetManager, error) {
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(os.Stderr, nil))
	}

	pool, err := pgxpool.New(ctx, opts.DSN)
	if err != nil {
		return nil, fmt.Errorf("create connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	if err := Migrate(ctx, pool, logger); err != nil {
		pool.Close()
		return nil, fmt.Errorf("run schema migration: %w", err)
	}

	watchCfg := opts.WatchConfig
	if watchCfg.PollIdle == 0 {
		watchCfg = DefaultWatchConfig()
	}

	stores := make(map[string]*InformerStore)
	for _, kind := range RegisteredKinds() {
		stores[kind] = NewInformerStore(kind, logger)
	}

	watcher := NewWatcher(pool, watchCfg, logger)
	for kind, store := range stores {
		watcher.RegisterStore(kind, store)
	}

	fleetCache := NewCache(stores)
	fleetClient := NewClient(pool, fleetCache, logger)

	scheme, err := buildScheme()
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("build scheme: %w", err)
	}

	mgr, err := manager.New(&rest.Config{}, manager.Options{
		Scheme:         scheme,
		LeaderElection: false,
		MapperProvider: func(c *rest.Config, httpClient *http.Client) (meta.RESTMapper, error) {
			return newStaticMapper(), nil
		},
		NewCache: func(config *rest.Config, opts cache.Options) (cache.Cache, error) {
			return fleetCache, nil
		},
		NewClient: func(config *rest.Config, opts client.Options) (client.Client, error) {
			return fleetClient, nil
		},
		Metrics: metricsserver.Options{
			BindAddress: opts.MetricsBindAddress,
		},
		HealthProbeBindAddress: opts.HealthProbeBindAddress,
	})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("create manager: %w", err)
	}

	return &FleetManager{
		Pool:    pool,
		Client:  fleetClient,
		Cache:   fleetCache,
		Watcher: watcher,
		Stores:  stores,
		Manager: mgr,
		Logger:  logger,
	}, nil
}

// Start runs the watcher (LIST-then-WATCH) and then starts the manager.
func (fm *FleetManager) Start(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- fm.Watcher.Run(ctx)
	}()

	// Wait for cache sync, but also check if the watcher failed (e.g. initialList error).
	for {
		select {
		case err := <-errCh:
			return fmt.Errorf("watcher failed: %w", err)
		default:
		}

		allSynced := true
		for _, store := range fm.Stores {
			if !store.HasSynced() {
				allSynced = false
				break
			}
		}
		if allSynced {
			fm.Cache.synced.Store(true)
			break
		}

		select {
		case err := <-errCh:
			return fmt.Errorf("watcher failed: %w", err)
		case <-ctx.Done():
			return fmt.Errorf("cache sync cancelled: %w", ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}

	fm.Logger.Info("FleetStore cache synced, starting manager")
	return fm.Manager.Start(ctx)
}

// Close cleans up resources.
func (fm *FleetManager) Close() {
	fm.Pool.Close()
}

func buildScheme() (*runtime.Scheme, error) {
	s := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(s); err != nil {
		return nil, err
	}
	return s, nil
}

func newStaticMapper() meta.RESTMapper {
	gvs := map[schema.GroupVersion]bool{}
	for _, kind := range RegisteredKinds() {
		gvk, _ := GVKFor(kind)
		gvs[gvk.GroupVersion()] = true
	}
	gvList := make([]schema.GroupVersion, 0, len(gvs))
	for gv := range gvs {
		gvList = append(gvList, gv)
	}
	mapper := meta.NewDefaultRESTMapper(gvList)

	for _, kind := range RegisteredKinds() {
		gvk, _ := GVKFor(kind)
		scope := meta.RESTScopeNamespace
		if IsGlobal(kind) {
			scope = meta.RESTScopeRoot
		}
		mapper.Add(gvk, scope)
	}

	return mapper
}
