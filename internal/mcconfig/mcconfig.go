package mcconfig

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	hyperfleetv1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
)

const (
	ConfigMapName      = "management-clusters"
	ConfigMapNamespace = "platform-api"
	ConfigMapKey       = "clusters.yaml"
)

type ManagementCluster struct {
	ID        string `json:"id" yaml:"id"`
	Region    string `json:"region" yaml:"region"`
	AccountID string `json:"accountId" yaml:"accountId"`
}

// MCLister is satisfied by both Loader (ConfigMap-based) and StoreLoader (FleetStore-based).
type MCLister interface {
	List() []ManagementCluster
}

type Loader struct {
	reader client.Reader

	mu       sync.RWMutex
	clusters []ManagementCluster
}

func NewLoader(reader client.Reader) *Loader {
	return &Loader{reader: reader}
}

// NewLoaderFromFile creates a Loader pre-populated from a YAML file.
// Intended for tests that don't need a Kubernetes client.
func NewLoaderFromFile(path string) (*Loader, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read mc config %s: %w", path, err)
	}
	clusters, err := parseClusters(data)
	if err != nil {
		return nil, err
	}
	return &Loader{clusters: clusters}, nil
}

func (l *Loader) Reload(ctx context.Context) error {
	var cm corev1.ConfigMap
	if err := l.reader.Get(ctx, types.NamespacedName{
		Name:      ConfigMapName,
		Namespace: ConfigMapNamespace,
	}, &cm); err != nil {
		return fmt.Errorf("get ConfigMap %s/%s: %w", ConfigMapNamespace, ConfigMapName, err)
	}
	data, ok := cm.Data[ConfigMapKey]
	if !ok {
		return fmt.Errorf("ConfigMap %s/%s missing key %q", ConfigMapNamespace, ConfigMapName, ConfigMapKey)
	}
	clusters, err := parseClusters([]byte(data))
	if err != nil {
		return err
	}
	l.mu.Lock()
	l.clusters = clusters
	l.mu.Unlock()
	return nil
}

func (l *Loader) List() []ManagementCluster {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make([]ManagementCluster, len(l.clusters))
	copy(out, l.clusters)
	return out
}

// Watch polls the ConfigMap for changes and reloads when the content changes.
// Blocks until ctx is cancelled. The ConfigMap is a temporary data source, so
// we poll rather than building a full controller with informer watches.
func (l *Loader) Watch(ctx context.Context, interval time.Duration, logger *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := l.Reload(ctx); err != nil {
				logger.Warn("failed to reload mc config", "error", err)
				continue
			}
		}
	}
}

func parseClusters(data []byte) ([]ManagementCluster, error) {
	var clusters []ManagementCluster
	if err := yaml.Unmarshal(data, &clusters); err != nil {
		return nil, fmt.Errorf("parse mc config: %w", err)
	}
	if len(clusters) == 0 {
		return nil, fmt.Errorf("mc config: no management clusters defined")
	}
	return clusters, nil
}

// StoreLoader reads ManagementCluster CRs from the FleetStore cache,
// replacing ConfigMap-based polling.
type StoreLoader struct {
	reader client.Reader
}

// NewStoreLoader creates a StoreLoader backed by a FleetStore cache.
func NewStoreLoader(reader client.Reader) *StoreLoader {
	return &StoreLoader{reader: reader}
}

// List returns all ManagementCluster CRs as mcconfig.ManagementCluster structs.
func (s *StoreLoader) List() []ManagementCluster {
	var list hyperfleetv1alpha1.ManagementClusterList
	if err := s.reader.List(context.Background(), &list); err != nil {
		return nil
	}
	out := make([]ManagementCluster, 0, len(list.Items))
	for _, mc := range list.Items {
		out = append(out, ManagementCluster{
			ID:        mc.Name,
			Region:    mc.Spec.Region,
			AccountID: mc.Spec.AccountID,
		})
	}
	return out
}
