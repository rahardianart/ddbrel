package ddbrel

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func lastKey(pk, sk string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"PK": &types.AttributeValueMemberS{Value: pk},
		"SK": &types.AttributeValueMemberS{Value: sk},
	}
}

func TestCursorRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		dir  string
		node string
		q    queryOptions
	}{
		{"out", dirOut, "user-1", queryOptions{label: "PLACED"}},
		{"in", dirIn, "order-5", queryOptions{}},
		{"with range", dirOut, "user-1", queryOptions{label: "PLACED", lo: "2026", hi: "2027", rangeSet: true}},
		{"unicode node", dirOut, "utilisateur-café", queryOptions{label: "PASSÉ"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c, err := newCursor(tt.dir, tt.node, tt.q, lastKey(tt.node, "OUT#PLACED#2026#order-5"), defaultCodec())
			if err != nil {
				t.Fatalf("newCursor: %v", err)
			}

			got, err := ParseCursor(c.String())
			if err != nil {
				t.Fatalf("ParseCursor: %v", err)
			}
			if got.p != c.p {
				t.Fatalf("round trip = %+v, want %+v", got.p, c.p)
			}

			key, err := got.startKey(tt.dir, tt.node, tt.q, defaultCodec())
			if err != nil {
				t.Fatalf("startKey: %v", err)
			}
			if v, _ := stringAttr(key["PK"]); v != tt.node {
				t.Errorf("start key PK = %q, want %q", v, tt.node)
			}
		})
	}
}

func TestCursorMismatch(t *testing.T) {
	t.Parallel()

	base := queryOptions{label: "PLACED", lo: "2026", hi: "2027", rangeSet: true}
	c, err := newCursor(dirOut, "user-1", base, lastKey("user-1", "OUT#PLACED#2026#order-5"), defaultCodec())
	if err != nil {
		t.Fatalf("newCursor: %v", err)
	}

	tests := []struct {
		name string
		dir  string
		node string
		q    queryOptions
		want error
	}{
		{"same query", dirOut, "user-1", base, nil},
		{"other direction", dirIn, "user-1", base, ErrCursorMismatch},
		{"other node", dirOut, "user-2", base, ErrCursorMismatch},
		{"other label", dirOut, "user-1", queryOptions{label: "VIEWED", lo: "2026", hi: "2027", rangeSet: true}, ErrCursorMismatch},
		{"other range", dirOut, "user-1", queryOptions{label: "PLACED", lo: "2020", hi: "2027", rangeSet: true}, ErrCursorMismatch},
		{"range dropped", dirOut, "user-1", queryOptions{label: "PLACED"}, ErrCursorMismatch},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := c.startKey(tt.dir, tt.node, tt.q, defaultCodec())
			if !errors.Is(err, tt.want) {
				t.Fatalf("startKey error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestParseCursorRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, raw string
	}{
		{"not base64", "not base64!"},
		{"not json", "bm90IGpzb24"},
		{"missing key", "e30"},
		{"empty", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if _, err := ParseCursor(tt.raw); err == nil {
				t.Fatalf("ParseCursor(%q) = nil error, want error", tt.raw)
			}
		})
	}
}

func TestNilCursorString(t *testing.T) {
	t.Parallel()

	var c *Cursor
	if c.String() != "" {
		t.Fatalf("nil cursor String = %q, want empty", c.String())
	}
}
