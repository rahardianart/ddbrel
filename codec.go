package ddbrel

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	delim = "#"

	dirOut = "OUT"
	dirIn  = "IN"

	attrLabel = "Label"
	attrNode  = "Node"
	attrSort  = "Sort"
)

// codec turns edges into items and back. The partition and sort attribute names
// are configurable so the store can sit on an existing table's key schema.
type codec struct {
	pk, sk string
}

func defaultCodec() codec { return codec{pk: "PK", sk: "SK"} }

func (c codec) reserved(name string) bool {
	switch name {
	case c.pk, c.sk, attrLabel, attrNode, attrSort:
		return true
	}
	return false
}

// prefixFor is the sort key prefix shared by every edge of one direction and
// label. An empty label is a valid label, not a wildcard.
func prefixFor(dir, label string) string {
	return dir + delim + label + delim
}

// dirPrefix matches every label in one direction.
func dirPrefix(dir string) string { return dir + delim }

func sortKey(dir, label, sort, node string) string {
	return dir + delim + label + delim + sort + delim + node
}

func parseSortKey(sk string) (dir, label, sort, node string, err error) {
	parts := strings.Split(sk, delim)
	if len(parts) != 4 {
		return "", "", "", "", fmt.Errorf("ddbrel: malformed sort key %q", sk)
	}
	dir, label, sort, node = parts[0], parts[1], parts[2], parts[3]
	if dir != dirOut && dir != dirIn {
		return "", "", "", "", fmt.Errorf("ddbrel: unknown direction %q in sort key %q", dir, sk)
	}
	return dir, label, sort, node, nil
}

// upperBound is the exclusive sort key bound that includes every key beginning
// with prefix. It bumps the trailing delimiter, so it holds for any label, sort
// value or node ID built from characters above '#'.
func upperBound(prefix string) string {
	if prefix == "" {
		return ""
	}
	return prefix[:len(prefix)-1] + string(prefix[len(prefix)-1]+1)
}

func validateComponent(what, value string, allowEmpty bool) error {
	if value == "" {
		if allowEmpty {
			return nil
		}
		return invalidID(what, value, "is empty")
	}
	if strings.Contains(value, delim) {
		return invalidID(what, value, "contains the key delimiter '#'")
	}
	return nil
}

func validateEdge(from, to, label, sort string) error {
	if err := validateComponent("node id", from, false); err != nil {
		return err
	}
	if err := validateComponent("node id", to, false); err != nil {
		return err
	}
	if err := validateComponent("label", label, true); err != nil {
		return err
	}
	return validateComponent("sort value", sort, true)
}

func (c codec) key(pk, sk string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		c.pk: &types.AttributeValueMemberS{Value: pk},
		c.sk: &types.AttributeValueMemberS{Value: sk},
	}
}

func (c codec) item(pk, sk, label, node, sort string, attrs map[string]types.AttributeValue) (map[string]types.AttributeValue, error) {
	item := make(map[string]types.AttributeValue, len(attrs)+5)
	for k, v := range attrs {
		if c.reserved(k) {
			return nil, fmt.Errorf("ddbrel: attribute %q is reserved by the edge item", k)
		}
		item[k] = v
	}
	item[c.pk] = &types.AttributeValueMemberS{Value: pk}
	item[c.sk] = &types.AttributeValueMemberS{Value: sk}
	item[attrLabel] = &types.AttributeValueMemberS{Value: label}
	item[attrNode] = &types.AttributeValueMemberS{Value: node}
	item[attrSort] = &types.AttributeValueMemberS{Value: sort}
	return item, nil
}

func (c codec) edge(item map[string]types.AttributeValue) (Edge, error) {
	pk, ok := stringAttr(item[c.pk])
	if !ok {
		return Edge{}, fmt.Errorf("ddbrel: item is missing a string %q attribute", c.pk)
	}
	sk, ok := stringAttr(item[c.sk])
	if !ok {
		return Edge{}, fmt.Errorf("ddbrel: item is missing a string %q attribute", c.sk)
	}

	dir, label, sort, keyNode, err := parseSortKey(sk)
	if err != nil {
		return Edge{}, err
	}
	node := keyNode
	if v, ok := stringAttr(item[attrNode]); ok {
		node = v
	}

	e := Edge{Label: label, Sort: sort}
	if dir == dirOut {
		e.From, e.To = pk, node
	} else {
		e.From, e.To = node, pk
	}

	for k, v := range item {
		if c.reserved(k) {
			continue
		}
		if e.Attrs == nil {
			e.Attrs = make(map[string]types.AttributeValue)
		}
		e.Attrs[k] = v
	}
	return e, nil
}

func stringAttr(v types.AttributeValue) (string, bool) {
	s, ok := v.(*types.AttributeValueMemberS)
	if !ok {
		return "", false
	}
	return s.Value, true
}
