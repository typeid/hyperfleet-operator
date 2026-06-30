package dynamo

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ErrNotFound is returned when a desire item does not exist in DynamoDB.
var ErrNotFound = errors.New("desire not found")

const (
	TableSuffixApplyDesires       = "-applydesires"
	TableSuffixDeleteDesires      = "-deletedesires"
	TableSuffixReadDesires        = "-readdesires"
	TableSuffixStatusApplyDesires  = "-status-applydesires"
	TableSuffixStatusDeleteDesires = "-status-deletedesires"
	TableSuffixStatusReadDesires   = "-status-readdesires"
	attributeDocumentID           = "documentID"
)

// UpsertResult reports whether an upsert changed the item and the updateTime
// that should be used for staleness tracking. When Changed is false, UpdateTime
// reflects the existing item's time so callers never need to fabricate one.
type UpsertResult struct {
	Changed    bool
	UpdateTime time.Time
}

// DesireClient is the interface used by controllers to interact with DynamoDB desires.
type DesireClient interface {
	UpsertApplyDesire(ctx context.Context, specsPrefix string, desire *ApplyDesire) (UpsertResult, error)
	PutDeleteDesire(ctx context.Context, specsPrefix string, desire *DeleteDesire) error
	UpsertReadDesire(ctx context.Context, specsPrefix string, desire *ReadDesire) (UpsertResult, error)
	GetApplyDesireStatus(ctx context.Context, statusPrefix, documentID string) (*ApplyDesireStatus, error)
	GetDeleteDesireStatus(ctx context.Context, statusPrefix, documentID string) (*DeleteDesireStatus, error)
	GetReadDesireStatus(ctx context.Context, statusPrefix, documentID string) (*ReadDesireStatus, error)
	DeleteDesireSpec(ctx context.Context, specsPrefix, suffix, documentID string) error
}

// Client writes desire specs and reads desire statuses from DynamoDB.
// It is the inverse of kube-applier-aws: the operator writes specs and reads
// statuses, while kube-applier reads specs and writes statuses.
type Client struct {
	db *dynamodb.Client
}

var _ DesireClient = (*Client)(nil)

func NewClient(db *dynamodb.Client) *Client {
	return &Client{db: db}
}

// UpsertApplyDesire writes an ApplyDesire spec only when content has changed.
func (c *Client) UpsertApplyDesire(ctx context.Context, specsPrefix string, desire *ApplyDesire) (UpsertResult, error) {
	return c.upsertDesire(ctx, specsPrefix+TableSuffixApplyDesires, desire.DocumentID, desire.Spec)
}

// PutDeleteDesire writes a DeleteDesire spec to the specs table.
func (c *Client) PutDeleteDesire(ctx context.Context, specsPrefix string, desire *DeleteDesire) error {
	return c.putDesire(ctx, specsPrefix+TableSuffixDeleteDesires, desire.DocumentID, desire.Spec)
}

// UpsertReadDesire writes a ReadDesire spec only when content has changed.
func (c *Client) UpsertReadDesire(ctx context.Context, specsPrefix string, desire *ReadDesire) (UpsertResult, error) {
	return c.upsertDesire(ctx, specsPrefix+TableSuffixReadDesires, desire.DocumentID, desire.Spec)
}

// GetApplyDesireStatus reads an ApplyDesire from the status table.
func (c *Client) GetApplyDesireStatus(ctx context.Context, statusPrefix, documentID string) (*ApplyDesireStatus, error) {
	var status ApplyDesireStatus
	if err := c.getDesireStatus(ctx, statusPrefix+TableSuffixApplyDesires, documentID, &status); err != nil {
		return nil, err
	}
	return &status, nil
}

// GetDeleteDesireStatus reads a DeleteDesire from the status table.
func (c *Client) GetDeleteDesireStatus(ctx context.Context, statusPrefix, documentID string) (*DeleteDesireStatus, error) {
	var status DeleteDesireStatus
	if err := c.getDesireStatus(ctx, statusPrefix+TableSuffixDeleteDesires, documentID, &status); err != nil {
		return nil, err
	}
	return &status, nil
}

// GetReadDesireStatus reads a ReadDesire from the status table.
func (c *Client) GetReadDesireStatus(ctx context.Context, statusPrefix, documentID string) (*ReadDesireStatus, error) {
	var status ReadDesireStatus
	if err := c.getDesireStatus(ctx, statusPrefix+TableSuffixReadDesires, documentID, &status); err != nil {
		return nil, err
	}
	return &status, nil
}

// DeleteDesireSpec removes a desire from the specs table.
func (c *Client) DeleteDesireSpec(ctx context.Context, specsPrefix, suffix, documentID string) error {
	_, err := c.db.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(specsPrefix + suffix),
		Key: map[string]dynamodbtypes.AttributeValue{
			attributeDocumentID: &dynamodbtypes.AttributeValueMemberS{Value: documentID},
		},
	})
	if err != nil {
		return fmt.Errorf("dynamodb delete %s/%s: %w", specsPrefix+suffix, documentID, err)
	}
	return nil
}

func computeSpecHash(spec any) (string, error) {
	data, err := json.Marshal(spec)
	if err != nil {
		return "", fmt.Errorf("marshal spec for hash: %w", err)
	}
	h := sha256.Sum256(data)
	return fmt.Sprintf("%x", h), nil
}

func (c *Client) upsertDesire(ctx context.Context, table, documentID string, spec any) (UpsertResult, error) {
	newHash, err := computeSpecHash(spec)
	if err != nil {
		return UpsertResult{}, err
	}

	existing, err := c.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:            aws.String(table),
		ProjectionExpression: aws.String("specHash, updateTime"),
		Key: map[string]dynamodbtypes.AttributeValue{
			attributeDocumentID: &dynamodbtypes.AttributeValueMemberS{Value: documentID},
		},
	})
	if err != nil {
		return UpsertResult{}, fmt.Errorf("dynamodb get %s/%s for upsert: %w", table, documentID, err)
	}

	if len(existing.Item) > 0 {
		if hashAttr, ok := existing.Item["specHash"]; ok {
			if s, ok := hashAttr.(*dynamodbtypes.AttributeValueMemberS); ok && s.Value == newHash {
				var existingTime time.Time
				if utAttr, ok := existing.Item["updateTime"]; ok {
					if ts, ok := utAttr.(*dynamodbtypes.AttributeValueMemberS); ok {
						existingTime, _ = time.Parse(time.RFC3339, ts.Value)
					}
				}
				return UpsertResult{Changed: false, UpdateTime: existingTime}, nil
			}
		}
	}

	now := time.Now().UTC()
	if err := c.putDesireWithHash(ctx, table, documentID, spec, newHash, now); err != nil {
		return UpsertResult{}, err
	}
	return UpsertResult{Changed: true, UpdateTime: now}, nil
}

// putDesire writes a desire spec unconditionally (used for DeleteDesire).
func (c *Client) putDesire(ctx context.Context, table, documentID string, spec any) error {
	return c.putDesireWithHash(ctx, table, documentID, spec, "", time.Now().UTC())
}

func (c *Client) putDesireWithHash(ctx context.Context, table, documentID string, spec any, specHash string, updateTime time.Time) error {
	specAttrs, err := attributevalue.MarshalMap(spec)
	if err != nil {
		return fmt.Errorf("marshal spec: %w", err)
	}

	item := map[string]dynamodbtypes.AttributeValue{
		attributeDocumentID: &dynamodbtypes.AttributeValueMemberS{Value: documentID},
		"version":           &dynamodbtypes.AttributeValueMemberN{Value: "1"},
		"updateTime":        &dynamodbtypes.AttributeValueMemberS{Value: updateTime.Format(time.RFC3339)},
		"spec":              &dynamodbtypes.AttributeValueMemberM{Value: specAttrs},
	}

	if specHash != "" {
		item["specHash"] = &dynamodbtypes.AttributeValueMemberS{Value: specHash}
	}

	if specMap, ok := spec.(ApplyDesireSpec); ok && specMap.KubeContent != nil {
		item["spec_kubeContent"] = &dynamodbtypes.AttributeValueMemberS{Value: string(specMap.KubeContent)}
		delete(specAttrs, "kubeContent")
	}

	_, err = c.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("dynamodb put %s/%s: %w", table, documentID, err)
	}
	return nil
}

func (c *Client) getDesireStatus(ctx context.Context, table, documentID string, out any) error {
	result, err := c.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(table),
		ConsistentRead: aws.Bool(true),
		Key: map[string]dynamodbtypes.AttributeValue{
			attributeDocumentID: &dynamodbtypes.AttributeValueMemberS{Value: documentID},
		},
	})
	if err != nil {
		return fmt.Errorf("dynamodb get %s/%s: %w", table, documentID, err)
	}
	if len(result.Item) == 0 {
		return fmt.Errorf("%w: %s/%s", ErrNotFound, table, documentID)
	}

	// kube-applier-aws writes the status fields nested under a "status" key.
	// Extract that map for unmarshalling into the status struct.
	statusAttrs := result.Item
	if statusM, ok := result.Item["status"]; ok {
		if m, ok := statusM.(*dynamodbtypes.AttributeValueMemberM); ok {
			statusAttrs = m.Value
		}
	}

	// Inject kubeContent from the top-level string attribute into the status map.
	if av, ok := result.Item["status_kubeContent"]; ok {
		if sv, ok := av.(*dynamodbtypes.AttributeValueMemberS); ok {
			statusAttrs["kubeContent"] = &dynamodbtypes.AttributeValueMemberB{Value: []byte(sv.Value)}
		}
	}

	if err := attributevalue.UnmarshalMap(statusAttrs, out); err != nil {
		return fmt.Errorf("unmarshal %s/%s: %w", table, documentID, err)
	}
	return nil
}

// TablePrefix returns the specs or status table prefix for a management cluster.
func SpecsPrefix(mc string) string {
	return fmt.Sprintf("%s-specs", mc)
}

func StatusPrefix(mc string) string {
	return fmt.Sprintf("%s-status", mc)
}

// MarshalManifest serializes a Kubernetes resource struct to JSON for KubeContent.
func MarshalManifest(obj any) ([]byte, error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("marshal manifest: %w", err)
	}
	return data, nil
}
