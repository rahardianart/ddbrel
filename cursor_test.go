package ddbrel

import (
	"encoding/base64"
	"encoding/json"
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

// TestCursorCarriesNoPartitionKey pins the wire format: the partition is derived
// from the node the caller passes, never from the cursor, so a tampered cursor
// cannot address another partition.
func TestCursorCarriesNoPartitionKey(t *testing.T) {
	t.Parallel()

	c, err := newCursor(dirOut, "user-1", queryOptions{}, lastKey("user-1", "OUT##2026#order-5"), defaultCodec())
	if err != nil {
		t.Fatalf("newCursor: %v", err)
	}

	raw, err := base64.RawURLEncoding.DecodeString(c.String())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	var generic map[string]any
	if err := json.Unmarshal(raw, &generic); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, present := generic["pk"]; present {
		t.Errorf("cursor payload still carries a partition key: %s", raw)
	}

	key, err := c.startKey(dirOut, "user-1", queryOptions{}, defaultCodec())
	if err != nil {
		t.Fatalf("startKey: %v", err)
	}
	if got := key["PK"].(*types.AttributeValueMemberS).Value; got != "user-1" {
		t.Errorf("start key PK = %q, want the queried node", got)
	}
}

func TestCursorMismatchAcrossQueryShapes(t *testing.T) {
	t.Parallel()

	base := queryOptions{}
	last := lastKey("n", "OUT#L#001#x")

	tests := []struct {
		name string
		from queryOptions
		to   queryOptions
	}{
		{"wildcard vs explicit empty label", queryOptions{}, queryOptions{labelSet: true}},
		{"explicit empty label vs wildcard", queryOptions{labelSet: true}, queryOptions{}},
		{"different label", queryOptions{label: "A", labelSet: true}, queryOptions{label: "B", labelSet: true}},
		{"range vs no range", queryOptions{label: "L", labelSet: true, rangeSet: true},
			queryOptions{label: "L", labelSet: true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := newCursor(dirOut, "n", tt.from, last, defaultCodec())
			if err != nil {
				t.Fatalf("newCursor: %v", err)
			}
			if _, err := c.startKey(dirOut, "n", tt.to, defaultCodec()); !errors.Is(err, ErrCursorMismatch) {
				t.Errorf("startKey err = %v, want ErrCursorMismatch", err)
			}
		})
	}

	// the matching case still resumes
	c, err := newCursor(dirOut, "n", base, last, defaultCodec())
	if err != nil {
		t.Fatalf("newCursor: %v", err)
	}
	if _, err := c.startKey(dirOut, "n", base, defaultCodec()); err != nil {
		t.Errorf("identical query should resume, got %v", err)
	}
}
