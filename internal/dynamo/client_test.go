package dynamo

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// spyDB implements dynamoAPI in-memory for unit tests.
type spyDB struct {
	getCount int
	putCount int
	delCount int
	items    map[string]map[string]dynamodbtypes.AttributeValue // table/docID → item
}

func newSpyDB() *spyDB {
	return &spyDB{items: make(map[string]map[string]dynamodbtypes.AttributeValue)}
}

func (s *spyDB) key(table, docID string) string { return table + "/" + docID }

func (s *spyDB) GetItem(_ context.Context, input *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	s.getCount++
	docID := input.Key[attributeDocumentID].(*dynamodbtypes.AttributeValueMemberS).Value
	item, ok := s.items[s.key(*input.TableName, docID)]
	if !ok {
		return &dynamodb.GetItemOutput{}, nil
	}
	return &dynamodb.GetItemOutput{Item: item}, nil
}

func (s *spyDB) PutItem(_ context.Context, input *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	s.putCount++
	docID := input.Item[attributeDocumentID].(*dynamodbtypes.AttributeValueMemberS).Value
	s.items[s.key(*input.TableName, docID)] = input.Item
	return &dynamodb.PutItemOutput{}, nil
}

func (s *spyDB) DeleteItem(_ context.Context, input *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	s.delCount++
	docID := input.Key[attributeDocumentID].(*dynamodbtypes.AttributeValueMemberS).Value
	delete(s.items, s.key(*input.TableName, docID))
	return &dynamodb.DeleteItemOutput{}, nil
}

func newTestClient() (*Client, *spyDB) {
	spy := newSpyDB()
	c := NewClient(spy)
	return c, spy
}

func TestUpsertCacheHit(t *testing.T) {
	c, spy := newTestClient()
	ctx := context.Background()
	table := "test-applydesires"
	docID := "doc-1"
	spec := map[string]string{"key": "value"}

	// First upsert: cache miss → GetItem + PutItem.
	res, err := c.upsertDesire(ctx, table, docID, spec)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Changed {
		t.Fatal("expected Changed=true on first upsert")
	}
	if spy.getCount != 1 || spy.putCount != 1 {
		t.Fatalf("first upsert: got get=%d put=%d, want get=1 put=1", spy.getCount, spy.putCount)
	}

	// Second upsert with same spec: cache hit → no DynamoDB calls.
	res, err = c.upsertDesire(ctx, table, docID, spec)
	if err != nil {
		t.Fatal(err)
	}
	if res.Changed {
		t.Fatal("expected Changed=false on cache hit")
	}
	if spy.getCount != 1 || spy.putCount != 1 {
		t.Fatalf("cache hit: got get=%d put=%d, want get=1 put=1", spy.getCount, spy.putCount)
	}
}

func TestUpsertCacheChangedSpec(t *testing.T) {
	c, spy := newTestClient()
	ctx := context.Background()
	table := "test-applydesires"
	docID := "doc-1"

	// First upsert.
	if _, err := c.upsertDesire(ctx, table, docID, map[string]string{"v": "1"}); err != nil {
		t.Fatal(err)
	}

	// Second upsert with different spec: cache miss on hash → GetItem + PutItem.
	res, err := c.upsertDesire(ctx, table, docID, map[string]string{"v": "2"})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Changed {
		t.Fatal("expected Changed=true when spec changes")
	}
	if spy.getCount != 2 || spy.putCount != 2 {
		t.Fatalf("changed spec: got get=%d put=%d, want get=2 put=2", spy.getCount, spy.putCount)
	}

	// Third upsert with same v2 spec: cache hit.
	res, err = c.upsertDesire(ctx, table, docID, map[string]string{"v": "2"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Changed {
		t.Fatal("expected Changed=false after cache updated with new spec")
	}
	if spy.getCount != 2 || spy.putCount != 2 {
		t.Fatalf("after update cache hit: got get=%d put=%d, want get=2 put=2", spy.getCount, spy.putCount)
	}
}

func TestUpsertCacheColdStart(t *testing.T) {
	c, spy := newTestClient()
	ctx := context.Background()
	table := "test-applydesires"
	docID := "doc-1"
	spec := map[string]string{"key": "value"}

	// Pre-populate the spy with an existing item (simulates restart — item exists in DynamoDB but not in cache).
	hash, _ := computeSpecHash(spec)
	existingTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	spy.items[spy.key(table, docID)] = map[string]dynamodbtypes.AttributeValue{
		attributeDocumentID: &dynamodbtypes.AttributeValueMemberS{Value: docID},
		"specHash":          &dynamodbtypes.AttributeValueMemberS{Value: hash},
		"updateTime":        &dynamodbtypes.AttributeValueMemberS{Value: existingTime.Format(time.RFC3339)},
	}

	// First upsert after restart: cache miss → GetItem (hash matches) → no PutItem, cache populated.
	res, err := c.upsertDesire(ctx, table, docID, spec)
	if err != nil {
		t.Fatal(err)
	}
	if res.Changed {
		t.Fatal("expected Changed=false when DynamoDB hash matches")
	}
	if spy.getCount != 1 || spy.putCount != 0 {
		t.Fatalf("cold start: got get=%d put=%d, want get=1 put=0", spy.getCount, spy.putCount)
	}

	// Second upsert: cache now warm → no DynamoDB calls.
	res, err = c.upsertDesire(ctx, table, docID, spec)
	if err != nil {
		t.Fatal(err)
	}
	if spy.getCount != 1 || spy.putCount != 0 {
		t.Fatalf("warm cache: got get=%d put=%d, want get=1 put=0", spy.getCount, spy.putCount)
	}
}

func TestDeleteClearsCache(t *testing.T) {
	c, spy := newTestClient()
	ctx := context.Background()
	table := "test-applydesires"
	docID := "doc-1"
	spec := map[string]string{"key": "value"}

	// Populate cache via upsert.
	if _, err := c.upsertDesire(ctx, table, docID, spec); err != nil {
		t.Fatal(err)
	}

	// Verify cache is populated.
	ck := c.cacheKey(table, docID)
	if _, ok := c.cache.Load(ck); !ok {
		t.Fatal("expected cache entry after upsert")
	}

	// Delete clears cache.
	if err := c.DeleteDesireSpec(ctx, table, "", docID); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.cache.Load(ck); ok {
		t.Fatal("expected cache entry cleared after delete")
	}

	// Next upsert must go to DynamoDB again.
	spy.getCount = 0
	spy.putCount = 0
	res, err := c.upsertDesire(ctx, table, docID, spec)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Changed {
		t.Fatal("expected Changed=true after cache cleared")
	}
	if spy.getCount != 1 || spy.putCount != 1 {
		t.Fatalf("after delete: got get=%d put=%d, want get=1 put=1", spy.getCount, spy.putCount)
	}
}

func TestComputeSpecHash(t *testing.T) {
	h1, err := computeSpecHash(map[string]string{"a": "1"})
	if err != nil {
		t.Fatal(err)
	}
	h2, err := computeSpecHash(map[string]string{"a": "1"})
	if err != nil {
		t.Fatal(err)
	}
	h3, err := computeSpecHash(map[string]string{"a": "2"})
	if err != nil {
		t.Fatal(err)
	}
	if h1 != h2 {
		t.Errorf("same input produced different hashes: %s != %s", h1, h2)
	}
	if h1 == h3 {
		t.Errorf("different input produced same hash: %s", h1)
	}
	if len(h1) != 64 {
		t.Errorf("expected 64-char hex hash, got %d chars", len(h1))
	}
}
