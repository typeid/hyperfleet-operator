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
