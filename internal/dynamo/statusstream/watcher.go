package statusstream

import (
	"context"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

const (
	streamPollInterval = 2 * time.Second
	batchPause         = 200 * time.Millisecond
)

// OnChange is called when a status item is inserted or modified.
// documentID is the partition key of the changed item.
type OnChange func(documentID string)

// Watcher tails a DynamoDB Stream on a single status-readdesires table,
// calling onChange for every INSERT or MODIFY event.
type Watcher struct {
	dbClient      *dynamodb.Client
	streamsClient *dynamodbstreams.Client
	tableName     string
	onChange      OnChange
	logger        *slog.Logger
}

func NewWatcher(
	dbClient *dynamodb.Client,
	streamsClient *dynamodbstreams.Client,
	tableName string,
	onChange OnChange,
	logger *slog.Logger,
) *Watcher {
	return &Watcher{
		dbClient:      dbClient,
		streamsClient: streamsClient,
		tableName:     tableName,
		onChange:       onChange,
		logger:        logger.With("table", tableName),
	}
}

// Run blocks until ctx is cancelled, polling the stream for changes.
func (w *Watcher) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		w.watchStream(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(streamPollInterval):
		}
	}
}

func (w *Watcher) watchStream(ctx context.Context) {
	streamARN, err := w.getStreamARN(ctx)
	if err != nil {
		w.logger.Warn("failed to get stream ARN", "error", err)
		return
	}
	if streamARN == "" {
		w.logger.Warn("streams not enabled on table")
		return
	}

	shardIters, err := w.getShardIterators(ctx, streamARN)
	if err != nil {
		w.logger.Warn("failed to get shard iterators", "error", err)
		return
	}

	for {
		if ctx.Err() != nil {
			return
		}
		if len(shardIters) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(streamPollInterval):
			}
			shardIters, err = w.getShardIterators(ctx, streamARN)
			if err != nil {
				w.logger.Warn("failed to re-discover shards", "error", err)
				return
			}
			continue
		}

		var nextIters []string
		for _, iter := range shardIters {
			records, nextIter, err := w.getRecords(ctx, iter)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				continue
			}
			for _, rec := range records {
				if rec.Dynamodb == nil {
					continue
				}
				if rec.EventName == streamtypes.OperationTypeRemove {
					continue
				}
				docID := extractDocumentID(rec.Dynamodb.NewImage)
				if docID != "" {
					w.onChange(docID)
				}
			}
			if nextIter != "" {
				nextIters = append(nextIters, nextIter)
			}
		}
		shardIters = nextIters

		pause := streamPollInterval
		if len(shardIters) > 0 {
			pause = batchPause
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(pause):
		}
	}
}

func (w *Watcher) getStreamARN(ctx context.Context) (string, error) {
	out, err := w.dbClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(w.tableName),
	})
	if err != nil {
		return "", err
	}
	if out.Table.LatestStreamArn == nil {
		return "", nil
	}
	return *out.Table.LatestStreamArn, nil
}

func (w *Watcher) getShardIterators(ctx context.Context, streamARN string) ([]string, error) {
	var iters []string
	var lastShardID *string
	for {
		out, err := w.streamsClient.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
			StreamArn:             aws.String(streamARN),
			ExclusiveStartShardId: lastShardID,
		})
		if err != nil {
			return nil, err
		}
		for _, shard := range out.StreamDescription.Shards {
			if shard.ShardId == nil {
				continue
			}
			iterOut, err := w.streamsClient.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
				StreamArn:         aws.String(streamARN),
				ShardId:           shard.ShardId,
				ShardIteratorType: streamtypes.ShardIteratorTypeTrimHorizon,
			})
			if err != nil {
				continue
			}
			if iterOut.ShardIterator != nil {
				iters = append(iters, *iterOut.ShardIterator)
			}
		}
		if out.StreamDescription.LastEvaluatedShardId == nil {
			break
		}
		lastShardID = out.StreamDescription.LastEvaluatedShardId
	}
	return iters, nil
}

func (w *Watcher) getRecords(ctx context.Context, shardIterator string) ([]streamtypes.Record, string, error) {
	out, err := w.streamsClient.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
		ShardIterator: aws.String(shardIterator),
	})
	if err != nil {
		return nil, "", err
	}
	nextIter := ""
	if out.NextShardIterator != nil {
		nextIter = *out.NextShardIterator
	}
	return out.Records, nextIter, nil
}

// extractDocumentID pulls the documentID partition key from a stream image.
func extractDocumentID(image map[string]streamtypes.AttributeValue) string {
	av, ok := image["documentID"]
	if !ok {
		return ""
	}
	s, ok := av.(*streamtypes.AttributeValueMemberS)
	if !ok {
		return ""
	}
	return s.Value
}
