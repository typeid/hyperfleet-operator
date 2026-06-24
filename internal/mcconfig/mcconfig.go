package mcconfig

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

type ManagementCluster struct {
	ID        string `yaml:"id"`
	Region    string `yaml:"region"`
	AccountID string `yaml:"accountId"`
}

type Loader struct {
	path string

	mu       sync.RWMutex
	clusters []ManagementCluster
}

func NewLoader(path string) (*Loader, error) {
	l := &Loader{path: path}
	if err := l.Reload(); err != nil {
		return nil, err
	}
	return l, nil
}

// NewLoaderLazy creates a Loader that does not require the config file to exist
// at startup. Use Watch to begin polling for it.
func NewLoaderLazy(path string) *Loader {
	return &Loader{path: path}
}

func (l *Loader) Reload() error {
	data, err := os.ReadFile(l.path)
	if err != nil {
		return fmt.Errorf("read mc config %s: %w", l.path, err)
	}
	var clusters []ManagementCluster
	if err := yaml.Unmarshal(data, &clusters); err != nil {
		return fmt.Errorf("parse mc config %s: %w", l.path, err)
	}
	if len(clusters) == 0 {
		return fmt.Errorf("mc config %s: no management clusters defined", l.path)
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

// Watch polls the config file for changes and reloads when the content changes.
// Kubernetes ConfigMap volume mounts update via symlink swap, which makes
// fsnotify unreliable — polling is the robust approach. Blocks until ctx is
// cancelled. This is a temporary mechanism; the long-term plan is a regional
// DynamoDB table for MC registration.
func (l *Loader) Watch(ctx context.Context, interval time.Duration, logger *slog.Logger) {
	var lastMod time.Time
	var lastSize int64

	if info, err := os.Stat(l.path); err == nil {
		lastMod = info.ModTime()
		lastSize = info.Size()
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			info, err := os.Stat(l.path)
			if err != nil {
				continue
			}
			if info.ModTime().Equal(lastMod) && info.Size() == lastSize {
				continue
			}
			if err := l.Reload(); err != nil {
				logger.Warn("failed to reload mc config", "error", err)
				continue
			}
			lastMod = info.ModTime()
			lastSize = info.Size()
			logger.Info("reloaded mc config", "path", l.path, "clusters", len(l.List()))
		}
	}
}
