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

type cursorPayload struct {
	Node  string `json:"n"`
	Dir   string `json:"d"`
	Label string `json:"l"`
	Lo    string `json:"lo,omitempty"`
	Hi    string `json:"hi,omitempty"`
	PK    string `json:"pk"`
	SK    string `json:"sk"`
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
	if p.PK == "" || p.SK == "" {
		return nil, fmt.Errorf("ddbrel: decode cursor: missing key")
	}
	return &Cursor{p: p}, nil
}

func newCursor(dir, node string, q queryOptions, last map[string]types.AttributeValue, c codec) (*Cursor, error) {
	pk, ok := stringAttr(last[c.pk])
	if !ok {
		return nil, fmt.Errorf("ddbrel: last evaluated key is missing %q", c.pk)
	}
	sk, ok := stringAttr(last[c.sk])
	if !ok {
		return nil, fmt.Errorf("ddbrel: last evaluated key is missing %q", c.sk)
	}
	return &Cursor{p: cursorPayload{
		Node:  node,
		Dir:   dir,
		Label: q.label,
		Lo:    q.lo,
		Hi:    q.hi,
		PK:    pk,
		SK:    sk,
	}}, nil
}

func (c *Cursor) startKey(dir, node string, q queryOptions, cd codec) (map[string]types.AttributeValue, error) {
	if c.p.Node != node || c.p.Dir != dir || c.p.Label != q.label || c.p.Lo != q.lo || c.p.Hi != q.hi {
		return nil, ErrCursorMismatch
	}
	return cd.key(c.p.PK, c.p.SK), nil
}
