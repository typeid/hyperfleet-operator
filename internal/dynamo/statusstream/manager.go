package statusstream

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"

	"github.com/typeid/hyperfleet-operator/internal/mcconfig"
)

const tableSuffixStatusReadDesires = "-status-readdesires"

type watcherHandle struct {
	cancel context.CancelFunc
}

// Manager discovers management clusters and runs one Watcher per MC's
// status-readdesires table. It polls the MC list periodically to start
// watchers for new MCs and stop watchers for removed MCs.
type Manager struct {
	dbClient      *dynamodb.Client
	streamsClient *dynamodbstreams.Client
	mcLoader      *mcconfig.Loader
	onChange      OnChange
	logger        *slog.Logger
}

func NewManager(
	dbClient *dynamodb.Client,
	streamsClient *dynamodbstreams.Client,
	mcLoader *mcconfig.Loader,
	onChange OnChange,
	logger *slog.Logger,
) *Manager {
	return &Manager{
		dbClient:      dbClient,
		streamsClient: streamsClient,
		mcLoader:      mcLoader,
		onChange:       onChange,
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
	mcs := m.mcLoader.List()
	desired := make(map[string]struct{}, len(mcs))
	for _, mc := range mcs {
		desired[mc.ID] = struct{}{}
	}

	for id, entry := range active {
		if _, ok := desired[id]; !ok {
			m.logger.Info("stopping status stream watcher for removed MC", "mc", id)
			entry.cancel()
			delete(active, id)
		}
	}

	for _, mc := range mcs {
		if _, ok := active[mc.ID]; ok {
			continue
		}
		tableName := fmt.Sprintf("%s%s", mc.ID, tableSuffixStatusReadDesires)
		watcher := NewWatcher(m.dbClient, m.streamsClient, tableName, m.onChange, m.logger)
		watcherCtx, cancel := context.WithCancel(ctx)
		active[mc.ID] = watcherHandle{cancel: cancel}
		m.logger.Info("starting status stream watcher", "mc", mc.ID, "table", tableName)
		go watcher.Run(watcherCtx)
	}
}
