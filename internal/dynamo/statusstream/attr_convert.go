package statusstream

import (
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

// streamImageToDynamoDBItem converts a DynamoDB Streams attribute value map
// to the equivalent DynamoDB API attribute value map. The two SDK packages
// define separate but structurally identical types; this function bridges them.
func streamImageToDynamoDBItem(image map[string]streamtypes.AttributeValue) map[string]dbtypes.AttributeValue {
	if image == nil {
		return nil
	}
	out := make(map[string]dbtypes.AttributeValue, len(image))
	for k, v := range image {
		out[k] = streamAttrToDB(v)
	}
	return out
}

func streamAttrToDB(av streamtypes.AttributeValue) dbtypes.AttributeValue {
	switch v := av.(type) {
	case *streamtypes.AttributeValueMemberS:
		return &dbtypes.AttributeValueMemberS{Value: v.Value}
	case *streamtypes.AttributeValueMemberN:
		return &dbtypes.AttributeValueMemberN{Value: v.Value}
	case *streamtypes.AttributeValueMemberBOOL:
		return &dbtypes.AttributeValueMemberBOOL{Value: v.Value}
	case *streamtypes.AttributeValueMemberNULL:
		return &dbtypes.AttributeValueMemberNULL{Value: v.Value}
	case *streamtypes.AttributeValueMemberB:
		cp := make([]byte, len(v.Value))
		copy(cp, v.Value)
		return &dbtypes.AttributeValueMemberB{Value: cp}
	case *streamtypes.AttributeValueMemberSS:
		cp := make([]string, len(v.Value))
		copy(cp, v.Value)
		return &dbtypes.AttributeValueMemberSS{Value: cp}
	case *streamtypes.AttributeValueMemberNS:
		cp := make([]string, len(v.Value))
		copy(cp, v.Value)
		return &dbtypes.AttributeValueMemberNS{Value: cp}
	case *streamtypes.AttributeValueMemberBS:
		cp := make([][]byte, len(v.Value))
		for i, b := range v.Value {
			bcp := make([]byte, len(b))
			copy(bcp, b)
			cp[i] = bcp
		}
		return &dbtypes.AttributeValueMemberBS{Value: cp}
	case *streamtypes.AttributeValueMemberL:
		list := make([]dbtypes.AttributeValue, len(v.Value))
		for i, item := range v.Value {
			list[i] = streamAttrToDB(item)
		}
		return &dbtypes.AttributeValueMemberL{Value: list}
	case *streamtypes.AttributeValueMemberM:
		m := make(map[string]dbtypes.AttributeValue, len(v.Value))
		for mk, mv := range v.Value {
			m[mk] = streamAttrToDB(mv)
		}
		return &dbtypes.AttributeValueMemberM{Value: m}
	default:
		return &dbtypes.AttributeValueMemberNULL{Value: true}
	}
}
