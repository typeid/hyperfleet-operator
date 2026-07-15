package statusstream

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

const (
	pollInterval     = 1 * time.Second
	discoverInterval = 30 * time.Second
	streamRetryDelay = 5 * time.Second
)

// OnChange is called when a status item is inserted or modified.
// documentID is the partition key of the changed item.
type OnChange func(documentID string)

// shardState tracks the reading state of a single stream shard.
type shardState struct {
	shardID       string
	parentShardID string
	iterator      string
	iteratorType  streamtypes.ShardIteratorType
	lastSeqNum    string
	closed        bool
}

// Watcher tails a DynamoDB Stream on a single status table,
// calling onChange for every INSERT or MODIFY event.
//
// It tracks shards by ID rather than opaque iterator strings,
// so it can detect shard rotation (parent closes, child created)
// and immediately adopt the child without waiting for all shards
// to close.
type Watcher struct {
	dbClient      *dynamodb.Client
	streamsClient *dynamodbstreams.Client
	tableName     string
	onChange      OnChange
	logger        *slog.Logger

	streamARN string
	shards    map[string]*shardState
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
		shards:        make(map[string]*shardState),
	}
}

// Run blocks until ctx is cancelled, polling the stream for changes.
func (w *Watcher) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		arn, err := w.getStreamARN(ctx)
		if err != nil || arn == "" {
			w.logger.Warn("failed to get stream ARN, retrying", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(streamRetryDelay):
				continue
			}
		}
		w.streamARN = arn
		break
	}

	w.discoverShards(ctx, true)

	discoverTicker := time.NewTicker(discoverInterval)
	defer discoverTicker.Stop()
	pollTicker := time.NewTicker(pollInterval)
	defer pollTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-discoverTicker.C:
			w.discoverShards(ctx, false)
		case <-pollTicker.C:
			if w.pollAllShards(ctx) {
				w.discoverShards(ctx, false)
			}
		}
	}
}

// discoverShards enumerates all shards via DescribeStream and adopts
// new ones into the tracked set.
//
// On initial discovery, only open shards are adopted (with LATEST)
// to avoid replaying history on startup.
//
// On subsequent discoveries, only children of tracked parents are
// adopted (with TRIM_HORIZON) so the watcher picks up exactly the
// records written after the parent closed.
func (w *Watcher) discoverShards(ctx context.Context, isInitial bool) {
	allShards, err := w.listAllShards(ctx)
	if err != nil {
		if isResourceNotFoundError(err) {
			w.refreshStreamARN(ctx)
		} else {
			w.logger.Warn("failed to list shards", "error", err)
		}
		return
	}
	w.discoverShardsFrom(allShards, isInitial)
}

func (w *Watcher) refreshStreamARN(ctx context.Context) {
	arn, err := w.getStreamARN(ctx)
	if err != nil || arn == "" {
		w.logger.Warn("failed to refresh stream ARN", "error", err)
		return
	}
	if arn != w.streamARN {
		w.logger.Info("stream ARN changed, resetting shard state", "old", w.streamARN, "new", arn)
		w.streamARN = arn
		w.shards = make(map[string]*shardState)
	}
}

// discoverShardsFrom processes a list of shards and adopts new ones.
// Separated from discoverShards for testability.
func (w *Watcher) discoverShardsFrom(allShards []streamtypes.Shard, isInitial bool) {
	for _, shard := range allShards {
		if shard.ShardId == nil {
			continue
		}
		sid := *shard.ShardId
		if _, tracked := w.shards[sid]; tracked {
			continue
		}

		isClosed := shard.SequenceNumberRange != nil &&
			shard.SequenceNumberRange.EndingSequenceNumber != nil

		if isInitial {
			if isClosed {
				continue
			}
			w.shards[sid] = &shardState{
				shardID:      sid,
				iteratorType: streamtypes.ShardIteratorTypeLatest,
			}
			w.logger.Info("initial shard adopted", "shardID", sid)
			continue
		}

		parentID := ""
		if shard.ParentShardId != nil {
			parentID = *shard.ParentShardId
		}
		if parentID != "" {
			if _, parentTracked := w.shards[parentID]; parentTracked {
				w.shards[sid] = &shardState{
					shardID:       sid,
					parentShardID: parentID,
					iteratorType:  streamtypes.ShardIteratorTypeTrimHorizon,
				}
				w.logger.Info("child shard adopted", "shardID", sid, "parentShardID", parentID)
			}
		} else if !isClosed {
			w.shards[sid] = &shardState{
				shardID:      sid,
				iteratorType: streamtypes.ShardIteratorTypeLatest,
			}
			w.logger.Info("orphan open shard adopted", "shardID", sid)
		}
	}

	w.pruneClosedShards()
}

// pruneClosedShards removes closed shards from the tracked set once
// their children have been adopted.
func (w *Watcher) pruneClosedShards() {
	parentsWithChildren := make(map[string]struct{})
	for _, s := range w.shards {
		if s.parentShardID != "" {
			parentsWithChildren[s.parentShardID] = struct{}{}
		}
	}
	for sid, s := range w.shards {
		if s.closed {
			if _, hasChild := parentsWithChildren[sid]; hasChild {
				delete(w.shards, sid)
			}
		}
	}
}

// pollAllShards reads records from every non-closed shard.
// Returns true if any shard closed during this poll cycle.
func (w *Watcher) pollAllShards(ctx context.Context) bool {
	anyClosed := false
	for _, shard := range w.shards {
		if shard.closed {
			continue
		}
		if ctx.Err() != nil {
			return anyClosed
		}

		if shard.iterator == "" {
			iter, err := w.getShardIterator(ctx, shard)
			if err != nil {
				if isResourceNotFoundError(err) {
					w.logger.Warn("shard not found, marking closed", "shardID", shard.shardID, "error", err)
					shard.closed = true
					anyClosed = true
					continue
				}
				w.logger.Warn("failed to get shard iterator", "shardID", shard.shardID, "error", err)
				continue
			}
			if iter == "" {
				continue
			}
			shard.iterator = iter
		}

		records, nextIter, err := w.getRecords(ctx, shard.iterator)
		if err != nil {
			if ctx.Err() != nil {
				return anyClosed
			}
			switch {
			case isExpiredIteratorError(err):
				w.logger.Info("iterator expired, refreshing", "shardID", shard.shardID)
				shard.iterator = ""
			case isResourceNotFoundError(err):
				w.logger.Warn("shard resource not found, marking closed", "shardID", shard.shardID, "error", err)
				shard.closed = true
				shard.iterator = ""
				anyClosed = true
			case isTrimmedDataError(err):
				w.logger.Warn("data trimmed past position, resetting to latest", "shardID", shard.shardID, "error", err)
				shard.iterator = ""
				shard.lastSeqNum = ""
				shard.iteratorType = streamtypes.ShardIteratorTypeLatest
			default:
				w.logger.Warn("getRecords failed, clearing iterator for retry", "shardID", shard.shardID, "error", err)
				shard.iterator = ""
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
			if rec.Dynamodb.SequenceNumber != nil {
				shard.lastSeqNum = *rec.Dynamodb.SequenceNumber
			}
		}

		if nextIter == "" {
			shard.closed = true
			shard.iterator = ""
			w.logger.Info("shard closed", "shardID", shard.shardID)
			anyClosed = true
		} else {
			shard.iterator = nextIter
		}
	}
	return anyClosed
}

func (w *Watcher) getShardIterator(ctx context.Context, shard *shardState) (string, error) {
	input := &dynamodbstreams.GetShardIteratorInput{
		StreamArn: aws.String(w.streamARN),
		ShardId:   aws.String(shard.shardID),
	}
	if shard.lastSeqNum != "" {
		input.ShardIteratorType = streamtypes.ShardIteratorTypeAfterSequenceNumber
		input.SequenceNumber = aws.String(shard.lastSeqNum)
	} else {
		input.ShardIteratorType = shard.iteratorType
	}

	out, err := w.streamsClient.GetShardIterator(ctx, input)
	if err != nil {
		return "", err
	}
	if out.ShardIterator == nil {
		return "", nil
	}
	return *out.ShardIterator, nil
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

func (w *Watcher) listAllShards(ctx context.Context) ([]streamtypes.Shard, error) {
	var shards []streamtypes.Shard
	var lastShardID *string
	for {
		out, err := w.streamsClient.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
			StreamArn:             aws.String(w.streamARN),
			ExclusiveStartShardId: lastShardID,
		})
		if err != nil {
			return nil, err
		}
		shards = append(shards, out.StreamDescription.Shards...)
		if out.StreamDescription.LastEvaluatedShardId == nil {
			break
		}
		lastShardID = out.StreamDescription.LastEvaluatedShardId
	}
	return shards, nil
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

func isExpiredIteratorError(err error) bool {
	var e *streamtypes.ExpiredIteratorException
	return errors.As(err, &e)
}

func isResourceNotFoundError(err error) bool {
	var e *streamtypes.ResourceNotFoundException
	return errors.As(err, &e)
}

func isTrimmedDataError(err error) bool {
	var e *streamtypes.TrimmedDataAccessException
	return errors.As(err, &e)
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
