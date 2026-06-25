package dynamo

import (
	"context"
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
	tableSuffixApplyDesires  = "-applydesires"
	tableSuffixDeleteDesires = "-deletedesires"
	tableSuffixReadDesires   = "-readdesires"
	attributeDocumentID      = "documentID"
)

// DesireClient is the interface used by controllers to interact with DynamoDB desires.
type DesireClient interface {
	PutApplyDesire(ctx context.Context, specsPrefix string, desire *ApplyDesire) error
	PutDeleteDesire(ctx context.Context, specsPrefix string, desire *DeleteDesire) error
	PutReadDesire(ctx context.Context, specsPrefix string, desire *ReadDesire) error
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

// PutApplyDesire writes an ApplyDesire spec to the specs table.
func (c *Client) PutApplyDesire(ctx context.Context, specsPrefix string, desire *ApplyDesire) error {
	return c.putDesire(ctx, specsPrefix+tableSuffixApplyDesires, desire.DocumentID, desire.Spec)
}

// PutDeleteDesire writes a DeleteDesire spec to the specs table.
func (c *Client) PutDeleteDesire(ctx context.Context, specsPrefix string, desire *DeleteDesire) error {
	return c.putDesire(ctx, specsPrefix+tableSuffixDeleteDesires, desire.DocumentID, desire.Spec)
}

// PutReadDesire writes a ReadDesire spec to the specs table.
func (c *Client) PutReadDesire(ctx context.Context, specsPrefix string, desire *ReadDesire) error {
	return c.putDesire(ctx, specsPrefix+tableSuffixReadDesires, desire.DocumentID, desire.Spec)
}

// GetApplyDesireStatus reads an ApplyDesire from the status table.
func (c *Client) GetApplyDesireStatus(ctx context.Context, statusPrefix, documentID string) (*ApplyDesireStatus, error) {
	var status ApplyDesireStatus
	if err := c.getDesireStatus(ctx, statusPrefix+tableSuffixApplyDesires, documentID, &status); err != nil {
		return nil, err
	}
	return &status, nil
}

// GetDeleteDesireStatus reads a DeleteDesire from the status table.
func (c *Client) GetDeleteDesireStatus(ctx context.Context, statusPrefix, documentID string) (*DeleteDesireStatus, error) {
	var status DeleteDesireStatus
	if err := c.getDesireStatus(ctx, statusPrefix+tableSuffixDeleteDesires, documentID, &status); err != nil {
		return nil, err
	}
	return &status, nil
}

// GetReadDesireStatus reads a ReadDesire from the status table.
func (c *Client) GetReadDesireStatus(ctx context.Context, statusPrefix, documentID string) (*ReadDesireStatus, error) {
	var status ReadDesireStatus
	if err := c.getDesireStatus(ctx, statusPrefix+tableSuffixReadDesires, documentID, &status); err != nil {
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

func (c *Client) putDesire(ctx context.Context, table, documentID string, spec any) error {
	item, err := attributevalue.MarshalMap(spec)
	if err != nil {
		return fmt.Errorf("marshal spec: %w", err)
	}
	item[attributeDocumentID] = &dynamodbtypes.AttributeValueMemberS{Value: documentID}
	item["version"] = &dynamodbtypes.AttributeValueMemberN{Value: "1"}

	now := time.Now().UTC().Format(time.RFC3339)
	item["updateTime"] = &dynamodbtypes.AttributeValueMemberS{Value: now}

	// For ApplyDesireSpec, kubeContent is []byte but needs to be stored as a
	// top-level string attribute for kube-applier-aws compatibility.
	if specMap, ok := spec.(ApplyDesireSpec); ok && specMap.KubeContent != nil {
		item["spec_kubeContent"] = &dynamodbtypes.AttributeValueMemberS{Value: string(specMap.KubeContent)}
		// Remove from nested spec to avoid double-write
		if nested, ok := item["spec"]; ok {
			if m, ok := nested.(*dynamodbtypes.AttributeValueMemberM); ok {
				delete(m.Value, "kubeContent")
			}
		}
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

	// Read kubeContent from the special top-level attribute if present.
	if av, ok := result.Item["status_kubeContent"]; ok {
		if sv, ok := av.(*dynamodbtypes.AttributeValueMemberS); ok {
			if statusM, ok := result.Item["status"]; ok {
				if m, ok := statusM.(*dynamodbtypes.AttributeValueMemberM); ok {
					m.Value["kubeContent"] = &dynamodbtypes.AttributeValueMemberB{Value: []byte(sv.Value)}
				}
			}
		}
	}

	if err := attributevalue.UnmarshalMap(result.Item, out); err != nil {
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
