package ddbrel

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func attrValue(t *testing.T, in map[string]types.AttributeValue, name string) string {
	t.Helper()

	v, ok := stringAttr(in[name])
	if !ok {
		t.Fatalf("expression value %q missing or not a string", name)
	}
	return v
}

func TestQueryInput(t *testing.T) {
	t.Parallel()

	s := New(nil, "edges")

	tests := []struct {
		name     string
		dir      string
		node     string
		opts     []QueryOption
		wantCond string
		wantErr  bool
		check    func(t *testing.T, in map[string]types.AttributeValue)
	}{
		{
			name:    "sort range without a label is not expressible",
			dir:     dirOut,
			node:    "user-1",
			opts:    []QueryOption{WithSortRange("a", "b")},
			wantErr: true,
		},
		{
			name:     "prefix query",
			dir:      dirOut,
			node:     "user-1",
			opts:     []QueryOption{WithLabel("PLACED")},
			wantCond: "#pk = :pk AND begins_with(#sk, :prefix)",
			check: func(t *testing.T, in map[string]types.AttributeValue) {
				if got := attrValue(t, in, ":prefix"); got != "OUT#PLACED#" {
					t.Errorf(":prefix = %q, want %q", got, "OUT#PLACED#")
				}
			},
		},
		{
			name:     "no label matches every label in the direction",
			dir:      dirIn,
			node:     "order-5",
			wantCond: "#pk = :pk AND begins_with(#sk, :prefix)",
			check: func(t *testing.T, in map[string]types.AttributeValue) {
				if got := attrValue(t, in, ":prefix"); got != "IN#" {
					t.Errorf(":prefix = %q, want %q", got, "IN#")
				}
			},
		},
		{
			name:     "explicit empty label is its own label",
			dir:      dirIn,
			node:     "order-5",
			opts:     []QueryOption{WithLabel("")},
			wantCond: "#pk = :pk AND begins_with(#sk, :prefix)",
			check: func(t *testing.T, in map[string]types.AttributeValue) {
				if got := attrValue(t, in, ":prefix"); got != "IN##" {
					t.Errorf(":prefix = %q, want %q", got, "IN##")
				}
			},
		},
		{
			name:     "bounded range",
			dir:      dirOut,
			node:     "user-1",
			opts:     []QueryOption{WithLabel("PLACED"), WithSortRange("20260101", "20260201")},
			wantCond: "#pk = :pk AND #sk BETWEEN :lo AND :hi",
			check: func(t *testing.T, in map[string]types.AttributeValue) {
				if got := attrValue(t, in, ":lo"); got != "OUT#PLACED#20260101" {
					t.Errorf(":lo = %q", got)
				}
				if got := attrValue(t, in, ":hi"); got != "OUT#PLACED#20260201$" {
					t.Errorf(":hi = %q", got)
				}
			},
		},
		{
			name:     "open upper bound",
			dir:      dirOut,
			node:     "user-1",
			opts:     []QueryOption{WithLabel("PLACED"), WithSortRange("20260101", "")},
			wantCond: "#pk = :pk AND #sk BETWEEN :lo AND :hi",
			check: func(t *testing.T, in map[string]types.AttributeValue) {
				if got := attrValue(t, in, ":hi"); got != "OUT#PLACED$" {
					t.Errorf(":hi = %q", got)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in, err := s.queryInput(tt.dir, tt.node, newQueryOptions(tt.opts))
			if tt.wantErr {
				if err == nil {
					t.Fatalf("queryInput = %v, want error", in)
				}
				return
			}
			if err != nil {
				t.Fatalf("queryInput: %v", err)
			}
			if aws.ToString(in.KeyConditionExpression) != tt.wantCond {
				t.Errorf("key condition = %q, want %q", aws.ToString(in.KeyConditionExpression), tt.wantCond)
			}
			if got := attrValue(t, in.ExpressionAttributeValues, ":pk"); got != tt.node {
				t.Errorf(":pk = %q, want %q", got, tt.node)
			}
			tt.check(t, in.ExpressionAttributeValues)
		})
	}
}

func TestQueryInputOptions(t *testing.T) {
	t.Parallel()

	s := New(nil, "edges")

	in, err := s.queryInput(dirOut, "user-1", newQueryOptions(nil))
	if err != nil {
		t.Fatalf("queryInput: %v", err)
	}
	if in.Limit != nil || in.ScanIndexForward != nil || in.ConsistentRead != nil || in.ExclusiveStartKey != nil {
		t.Fatalf("defaults set an option: %+v", in)
	}

	// The cursor must come from the same scan direction it resumes: a
	// LastEvaluatedKey means "continue past this row", which points opposite ways
	// under WithReverse.
	c, err := newCursor(dirOut, "user-1", queryOptions{reverse: true}, lastKey("user-1", "OUT##2026#order-5"), s.codec)
	if err != nil {
		t.Fatalf("newCursor: %v", err)
	}

	in, err = s.queryInput(dirOut, "user-1", newQueryOptions([]QueryOption{
		WithLimit(20), WithReverse(), WithConsistentRead(), WithCursor(c),
	}))
	if err != nil {
		t.Fatalf("queryInput: %v", err)
	}
	if aws.ToInt32(in.Limit) != 20 {
		t.Errorf("limit = %d, want 20", aws.ToInt32(in.Limit))
	}
	if in.ScanIndexForward == nil || *in.ScanIndexForward {
		t.Error("WithReverse did not set ScanIndexForward false")
	}
	if !aws.ToBool(in.ConsistentRead) {
		t.Error("WithConsistentRead did not set ConsistentRead")
	}
	if got, _ := stringAttr(in.ExclusiveStartKey["SK"]); got != "OUT##2026#order-5" {
		t.Errorf("exclusive start key SK = %q", got)
	}
}

func TestQueryInputRejects(t *testing.T) {
	t.Parallel()

	s := New(nil, "edges")

	tests := []struct {
		name string
		node string
		opts []QueryOption
		want error
	}{
		{"delimiter in node", "USER#1", nil, ErrInvalidID},
		{"empty node", "", nil, ErrInvalidID},
		{"delimiter in label", "user-1", []QueryOption{WithLabel("PLA#CED")}, ErrInvalidID},
		{"delimiter in range", "user-1", []QueryOption{WithSortRange("2026#01", "")}, ErrInvalidID},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if _, err := s.queryInput(dirOut, tt.node, newQueryOptions(tt.opts)); !errors.Is(err, tt.want) {
				t.Fatalf("queryInput error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestQueryInputHonoursKeyNames(t *testing.T) {
	t.Parallel()

	s := New(nil, "edges", WithKeyNames("pk", "sk"))
	in, err := s.queryInput(dirOut, "user-1", newQueryOptions(nil))
	if err != nil {
		t.Fatalf("queryInput: %v", err)
	}
	if in.ExpressionAttributeNames["#pk"] != "pk" || in.ExpressionAttributeNames["#sk"] != "sk" {
		t.Fatalf("attribute names = %v", in.ExpressionAttributeNames)
	}
}
