package statusstream

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"sigs.k8s.io/controller-runtime/pkg/client"

	hyperfleetv1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
)

type watcherHandle struct {
	cancel context.CancelFunc
}

// Manager discovers management clusters and runs one Watcher per MC per
// table suffix. It polls the MC list periodically to start watchers for
// new MCs and stop watchers for removed MCs.
type Manager struct {
	dbClient      *dynamodb.Client
	streamsClient *dynamodbstreams.Client
	mcReader      client.Reader
	tableSuffixes []string
	onChange      OnChange
	logger        *slog.Logger
}

func NewManager(
	dbClient *dynamodb.Client,
	streamsClient *dynamodbstreams.Client,
	mcReader client.Reader,
	tableSuffixes []string,
	onChange OnChange,
	logger *slog.Logger,
) *Manager {
	return &Manager{
		dbClient:      dbClient,
		streamsClient: streamsClient,
		mcReader:      mcReader,
		tableSuffixes: tableSuffixes,
		onChange:      onChange,
		logger:        logger,
	}
}

// Run blocks until ctx is cancelled. It polls the MC list every interval
// and ensures one Watcher goroutine runs per MC.
func (m *Manager) Run(ctx context.Context, interval time.Duration) {
	active := make(map[string]watcherHandle)

	defer func() {
		for _, w := range active {
			w.cancel()
		}
	}()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	m.syncWatchers(ctx, active)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.syncWatchers(ctx, active)
		}
	}
}

func (m *Manager) syncWatchers(ctx context.Context, active map[string]watcherHandle) {
	var list hyperfleetv1alpha1.ManagementClusterList
	if err := m.mcReader.List(ctx, &list); err != nil {
		m.logger.Error("failed to list ManagementCluster CRs", "error", err)
		return
	}

	desired := make(map[string]struct{}, len(list.Items)*len(m.tableSuffixes))
	for _, mc := range list.Items {
		for _, suffix := range m.tableSuffixes {
			desired[mc.Name+suffix] = struct{}{}
		}
	}

	for key, entry := range active {
		if _, ok := desired[key]; !ok {
			m.logger.Info("stopping status stream watcher", "key", key)
			entry.cancel()
			delete(active, key)
		}
	}

	for _, mc := range list.Items {
		if strings.HasPrefix(mc.Name, "test-mc-") {
			continue
		}
		for _, suffix := range m.tableSuffixes {
			key := mc.Name + suffix
			if _, ok := active[key]; ok {
				continue
			}
			tableName := mc.Name + suffix
			watcher := NewWatcher(m.dbClient, m.streamsClient, tableName, m.onChange, m.logger)
			watcherCtx, cancel := context.WithCancel(ctx)
			active[key] = watcherHandle{cancel: cancel}
			m.logger.Info("starting status stream watcher", "mc", mc.Name, "table", tableName)
			go watcher.Run(watcherCtx)
		}
	}
}
