package ddbrel

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Cursor resumes a query where the previous page stopped. It is opaque; String
// renders it for transport and ParseCursor reads it back.
type Cursor struct {
	p cursorPayload
}

// cursorPayload carries no partition key. The partition is always the node being
// queried, which the caller passes to Out or In, so storing it would mean trusting
// a client-supplied value to address a partition. It is derived instead.
type cursorPayload struct {
	Node     string `json:"n"`
	Dir      string `json:"d"`
	Label    string `json:"l,omitempty"`
	LabelSet bool   `json:"ls,omitempty"`
	Lo       string `json:"lo,omitempty"`
	Hi       string `json:"hi,omitempty"`
	RangeSet bool   `json:"rs,omitempty"`
	Reverse  bool   `json:"rv,omitempty"`
	SK       string `json:"sk"`
}

// String encodes the cursor as URL-safe base64.
func (c *Cursor) String() string {
	if c == nil {
		return ""
	}
	b, err := json.Marshal(c.p)
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

// ParseCursor decodes a cursor produced by Cursor.String.
func ParseCursor(s string) (*Cursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("ddbrel: decode cursor: %w", err)
	}
	var p cursorPayload
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, fmt.Errorf("ddbrel: decode cursor: %w", err)
	}
	if p.Node == "" || p.SK == "" {
		return nil, fmt.Errorf("ddbrel: decode cursor: missing key")
	}
	return &Cursor{p: p}, nil
}

func newCursor(dir, node string, q queryOptions, last map[string]types.AttributeValue, c codec) (*Cursor, error) {
	sk, ok := stringAttr(last[c.sk])
	if !ok {
		return nil, fmt.Errorf("ddbrel: last evaluated key is missing %q", c.sk)
	}
	return &Cursor{p: cursorPayload{
		Node:     node,
		Dir:      dir,
		Label:    q.label,
		LabelSet: q.labelSet,
		Lo:       q.lo,
		Hi:       q.hi,
		RangeSet: q.rangeSet,
		Reverse:  q.reverse,
		SK:       sk,
	}}, nil
}

// startKey rebuilds the ExclusiveStartKey from the caller's node and the cursor's
// sort key. Every field that changes which rows follow the start key is compared.
//
// That includes the labelSet and rangeSet flags — an omitted label queries every
// label while WithLabel("") queries only the empty one — and the scan direction.
// A LastEvaluatedKey means "continue past this row", which points opposite ways
// under WithReverse: resuming a descending page ascending re-serves rows the
// caller has already seen, with no error to notice.
func (c *Cursor) startKey(dir, node string, q queryOptions, cd codec) (map[string]types.AttributeValue, error) {
	if c.p.Node != node || c.p.Dir != dir ||
		c.p.Label != q.label || c.p.LabelSet != q.labelSet ||
		c.p.Lo != q.lo || c.p.Hi != q.hi || c.p.RangeSet != q.rangeSet ||
		c.p.Reverse != q.reverse {
		return nil, ErrCursorMismatch
	}
	return cd.key(node, c.p.SK), nil
}
