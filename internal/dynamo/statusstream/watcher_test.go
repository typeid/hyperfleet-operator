package statusstream

import (
	"testing"

	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

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

func TestStreamAttrToDB(t *testing.T) {
	// Verify recursive conversion of a nested map with multiple types.
	input := map[string]streamtypes.AttributeValue{
		"str":  &streamtypes.AttributeValueMemberS{Value: "hello"},
		"num":  &streamtypes.AttributeValueMemberN{Value: "42"},
		"bool": &streamtypes.AttributeValueMemberBOOL{Value: true},
		"nested": &streamtypes.AttributeValueMemberM{Value: map[string]streamtypes.AttributeValue{
			"inner": &streamtypes.AttributeValueMemberS{Value: "world"},
		}},
		"list": &streamtypes.AttributeValueMemberL{Value: []streamtypes.AttributeValue{
			&streamtypes.AttributeValueMemberS{Value: "a"},
			&streamtypes.AttributeValueMemberN{Value: "1"},
		}},
	}

	result := streamImageToDynamoDBItem(input)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if len(result) != 5 {
		t.Fatalf("expected 5 keys, got %d", len(result))
	}
}

func TestStreamImageToDynamoDBItemNil(t *testing.T) {
	result := streamImageToDynamoDBItem(nil)
	if result != nil {
		t.Errorf("expected nil for nil input, got %v", result)
	}
}
