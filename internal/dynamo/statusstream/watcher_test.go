package statusstream

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

func testWatcher(shards map[string]*shardState) *Watcher {
	if shards == nil {
		shards = make(map[string]*shardState)
	}
	return &Watcher{
		shards: shards,
		logger: slog.Default(),
	}
}

func TestExtractDocumentID(t *testing.T) {
	tests := []struct {
		name  string
		image map[string]streamtypes.AttributeValue
		want  string
	}{
		{
			name: "valid documentID",
			image: map[string]streamtypes.AttributeValue{
				"documentID": &streamtypes.AttributeValueMemberS{Value: "abc-123"},
				"version":    &streamtypes.AttributeValueMemberN{Value: "1"},
			},
			want: "abc-123",
		},
		{
			name:  "nil image",
			image: nil,
			want:  "",
		},
		{
			name: "missing documentID",
			image: map[string]streamtypes.AttributeValue{
				"version": &streamtypes.AttributeValueMemberN{Value: "1"},
			},
			want: "",
		},
		{
			name: "wrong type for documentID",
			image: map[string]streamtypes.AttributeValue{
				"documentID": &streamtypes.AttributeValueMemberN{Value: "123"},
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractDocumentID(tt.image)
			if got != tt.want {
				t.Errorf("extractDocumentID() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDiscoverShards_Initial(t *testing.T) {
	w := testWatcher(nil)

	allShards := []streamtypes.Shard{
		{
			ShardId: aws.String("open-1"),
			SequenceNumberRange: &streamtypes.SequenceNumberRange{
				StartingSequenceNumber: aws.String("100"),
			},
		},
		{
			ShardId: aws.String("open-2"),
			SequenceNumberRange: &streamtypes.SequenceNumberRange{
				StartingSequenceNumber: aws.String("200"),
			},
		},
		{
			ShardId: aws.String("closed-1"),
			SequenceNumberRange: &streamtypes.SequenceNumberRange{
				StartingSequenceNumber: aws.String("50"),
				EndingSequenceNumber:   aws.String("99"),
			},
		},
		{
			ShardId: aws.String("closed-2"),
			ParentShardId: aws.String("closed-0"),
			SequenceNumberRange: &streamtypes.SequenceNumberRange{
				StartingSequenceNumber: aws.String("10"),
				EndingSequenceNumber:   aws.String("49"),
			},
		},
	}

	w.discoverShardsFrom(allShards, true)

	if len(w.shards) != 2 {
		t.Fatalf("expected 2 shards, got %d", len(w.shards))
	}
	for _, id := range []string{"open-1", "open-2"} {
		s, ok := w.shards[id]
		if !ok {
			t.Errorf("expected shard %s to be tracked", id)
			continue
		}
		if s.iteratorType != streamtypes.ShardIteratorTypeTrimHorizon {
			t.Errorf("shard %s: expected TRIM_HORIZON, got %s", id, s.iteratorType)
		}
	}
	for _, id := range []string{"closed-1", "closed-2"} {
		if _, ok := w.shards[id]; ok {
			t.Errorf("closed shard %s should not be tracked on initial discovery", id)
		}
	}
}

func TestDiscoverShards_ChildAdoption(t *testing.T) {
	w := testWatcher(map[string]*shardState{
		"parent-1": {shardID: "parent-1", closed: true},
	})

	allShards := []streamtypes.Shard{
		{
			ShardId:       aws.String("parent-1"),
			SequenceNumberRange: &streamtypes.SequenceNumberRange{
				StartingSequenceNumber: aws.String("100"),
				EndingSequenceNumber:   aws.String("200"),
			},
		},
		{
			ShardId:       aws.String("child-1"),
			ParentShardId: aws.String("parent-1"),
			SequenceNumberRange: &streamtypes.SequenceNumberRange{
				StartingSequenceNumber: aws.String("201"),
			},
		},
	}

	w.discoverShardsFrom(allShards, false)

	child, ok := w.shards["child-1"]
	if !ok {
		t.Fatal("expected child-1 to be adopted")
	}
	if child.iteratorType != streamtypes.ShardIteratorTypeTrimHorizon {
		t.Errorf("child shard should use TRIM_HORIZON, got %s", child.iteratorType)
	}
	if child.parentShardID != "parent-1" {
		t.Errorf("child parentShardID = %q, want %q", child.parentShardID, "parent-1")
	}

	if _, ok := w.shards["parent-1"]; ok {
		t.Error("closed parent should be pruned after child adoption")
	}
}

func TestDiscoverShards_DebrisSkipped(t *testing.T) {
	w := testWatcher(nil)

	allShards := []streamtypes.Shard{
		{
			ShardId:       aws.String("debris-child"),
			ParentShardId: aws.String("untracked-parent"),
			SequenceNumberRange: &streamtypes.SequenceNumberRange{
				StartingSequenceNumber: aws.String("100"),
				EndingSequenceNumber:   aws.String("200"),
			},
		},
	}

	w.discoverShardsFrom(allShards, false)

	if len(w.shards) != 0 {
		t.Errorf("expected 0 shards (debris should be skipped), got %d", len(w.shards))
	}
}

func TestDiscoverShards_OrphanOpenShard(t *testing.T) {
	w := testWatcher(nil)

	allShards := []streamtypes.Shard{
		{
			ShardId: aws.String("orphan-open"),
			SequenceNumberRange: &streamtypes.SequenceNumberRange{
				StartingSequenceNumber: aws.String("100"),
			},
		},
	}

	w.discoverShardsFrom(allShards, false)

	s, ok := w.shards["orphan-open"]
	if !ok {
		t.Fatal("expected orphan open shard to be adopted")
	}
	if s.iteratorType != streamtypes.ShardIteratorTypeTrimHorizon {
		t.Errorf("orphan open shard should use TRIM_HORIZON, got %s", s.iteratorType)
	}
}

func TestDiscoverShards_AlreadyTracked(t *testing.T) {
	existing := &shardState{
		shardID:      "shard-1",
		iterator:     "existing-iter",
		iteratorType: streamtypes.ShardIteratorTypeLatest,
		lastSeqNum:   "500",
	}
	w := testWatcher(map[string]*shardState{"shard-1": existing})

	allShards := []streamtypes.Shard{
		{
			ShardId: aws.String("shard-1"),
			SequenceNumberRange: &streamtypes.SequenceNumberRange{
				StartingSequenceNumber: aws.String("100"),
			},
		},
	}

	w.discoverShardsFrom(allShards, false)

	if w.shards["shard-1"] != existing {
		t.Error("already-tracked shard should not be replaced")
	}
	if w.shards["shard-1"].iterator != "existing-iter" {
		t.Error("existing iterator should be preserved")
	}
}

func TestPruneClosedShards(t *testing.T) {
	w := testWatcher(map[string]*shardState{
		"parent": {shardID: "parent", closed: true},
		"child":  {shardID: "child", parentShardID: "parent"},
		"orphan": {shardID: "orphan", closed: true},
	})

	w.pruneClosedShards()

	if _, ok := w.shards["parent"]; ok {
		t.Error("parent with tracked child should be pruned")
	}
	if _, ok := w.shards["child"]; !ok {
		t.Error("child should still be tracked")
	}
	if _, ok := w.shards["orphan"]; !ok {
		t.Error("closed shard without tracked child should NOT be pruned (waiting for child discovery)")
	}
}

func TestDiscoverShards_FullRotation(t *testing.T) {
	w := testWatcher(map[string]*shardState{
		"A": {shardID: "A", iteratorType: streamtypes.ShardIteratorTypeLatest},
	})

	// Shard A closes
	w.shards["A"].closed = true

	// Child A' appears
	w.discoverShardsFrom([]streamtypes.Shard{
		{
			ShardId: aws.String("A"),
			SequenceNumberRange: &streamtypes.SequenceNumberRange{
				StartingSequenceNumber: aws.String("100"),
				EndingSequenceNumber:   aws.String("200"),
			},
		},
		{
			ShardId:       aws.String("A-prime"),
			ParentShardId: aws.String("A"),
			SequenceNumberRange: &streamtypes.SequenceNumberRange{
				StartingSequenceNumber: aws.String("201"),
			},
		},
	}, false)

	if _, ok := w.shards["A"]; ok {
		t.Error("A should be pruned after child adoption")
	}
	aPrime, ok := w.shards["A-prime"]
	if !ok {
		t.Fatal("A-prime should be adopted")
	}
	if aPrime.iteratorType != streamtypes.ShardIteratorTypeTrimHorizon {
		t.Error("A-prime should use TRIM_HORIZON")
	}

	// A' closes, grandchild A'' appears
	w.shards["A-prime"].closed = true

	w.discoverShardsFrom([]streamtypes.Shard{
		{
			ShardId: aws.String("A-prime"),
			SequenceNumberRange: &streamtypes.SequenceNumberRange{
				StartingSequenceNumber: aws.String("201"),
				EndingSequenceNumber:   aws.String("300"),
			},
		},
		{
			ShardId:       aws.String("A-double-prime"),
			ParentShardId: aws.String("A-prime"),
			SequenceNumberRange: &streamtypes.SequenceNumberRange{
				StartingSequenceNumber: aws.String("301"),
			},
		},
	}, false)

	if _, ok := w.shards["A-prime"]; ok {
		t.Error("A-prime should be pruned after grandchild adoption")
	}
	aDoublePrime, ok := w.shards["A-double-prime"]
	if !ok {
		t.Fatal("A-double-prime should be adopted")
	}
	if aDoublePrime.iteratorType != streamtypes.ShardIteratorTypeTrimHorizon {
		t.Error("A-double-prime should use TRIM_HORIZON")
	}
}

func TestIsExpiredIteratorError(t *testing.T) {
	expired := &streamtypes.ExpiredIteratorException{Message: aws.String("iterator has expired")}
	notFound := &streamtypes.ResourceNotFoundException{Message: aws.String("not found")}

	if !isExpiredIteratorError(expired) {
		t.Error("expected true for ExpiredIteratorException")
	}
	if isExpiredIteratorError(notFound) {
		t.Error("expected false for ResourceNotFoundException")
	}
	if isExpiredIteratorError(fmt.Errorf("some other error")) {
		t.Error("expected false for generic error")
	}
	if isExpiredIteratorError(fmt.Errorf("wrapped: %w", expired)) {
		// errors.As unwraps, so this should match
	} else {
		t.Error("expected true for wrapped ExpiredIteratorException")
	}
}

func TestIsResourceNotFoundError(t *testing.T) {
	notFound := &streamtypes.ResourceNotFoundException{Message: aws.String("not found")}
	expired := &streamtypes.ExpiredIteratorException{Message: aws.String("expired")}

	if !isResourceNotFoundError(notFound) {
		t.Error("expected true for ResourceNotFoundException")
	}
	if isResourceNotFoundError(expired) {
		t.Error("expected false for ExpiredIteratorException")
	}
	if !isResourceNotFoundError(fmt.Errorf("wrapped: %w", notFound)) {
		t.Error("expected true for wrapped ResourceNotFoundException")
	}
}

func TestIsTrimmedDataError(t *testing.T) {
	trimmed := &streamtypes.TrimmedDataAccessException{Message: aws.String("trimmed")}
	expired := &streamtypes.ExpiredIteratorException{Message: aws.String("expired")}

	if !isTrimmedDataError(trimmed) {
		t.Error("expected true for TrimmedDataAccessException")
	}
	if isTrimmedDataError(expired) {
		t.Error("expected false for ExpiredIteratorException")
	}
	if !isTrimmedDataError(fmt.Errorf("wrapped: %w", trimmed)) {
		t.Error("expected true for wrapped TrimmedDataAccessException")
	}
}
