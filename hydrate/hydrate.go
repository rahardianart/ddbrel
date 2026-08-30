// Package hydrate resolves edges into entities. It is opt-in: ddbrel itself
// never imports it, so the edge store works without any entity table existing.
//
// Hydration is a second round trip by design — the edge table is separate from
// the entity tables — and it is where BatchGetItem's sharp edges are handled:
// the 100 key limit per request, UnprocessedKeys, and targets that no longer
// exist, which are reported in Result.Missing rather than silently dropped.
package hydrate

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/rahardianart/ddbrel"
)

const (
	maxBatchKeys          = 100
	maxUnprocessedRetries = 8
	baseBackoff           = 50 * time.Millisecond
	maxBackoff            = 2 * time.Second
)

var (
	// ErrUnregisteredType reports a node ID that matches no registered type.
	ErrUnregisteredType = errors.New("ddbrel/hydrate: no type registered for node")

	// ErrUnprocessed reports keys DynamoDB never processed, after retries.
	ErrUnprocessed = errors.New("ddbrel/hydrate: keys left unprocessed")
)

// KeyFunc builds the primary key of an entity item from a node ID.
type KeyFunc func(id string) map[string]types.AttributeValue

// Registry maps node ID types to entity tables.
type Registry struct {
	mu      sync.RWMutex
	entries []entry
}

type entry struct {
	typ   string
	table string
	key   KeyFunc
}

// Register maps nodes of type typ to an entity table and the key that addresses
// them there. Node IDs are opaque to ddbrel, so typ is matched as a prefix of
// the node ID — Register("ORDER:", ...) claims "ORDER:5" — and the longest
// registered prefix wins. Registering the same typ twice replaces it.
func (r *Registry) Register(typ, table string, key KeyFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()

	e := entry{typ: typ, table: table, key: key}
	for i := range r.entries {
		if r.entries[i].typ == typ {
			r.entries[i] = e
			return
		}
	}
	r.entries = append(r.entries, e)
	sort.SliceStable(r.entries, func(i, j int) bool {
		return len(r.entries[i].typ) > len(r.entries[j].typ)
	})
}

func (r *Registry) lookup(id string) (entry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, e := range r.entries {
		if strings.HasPrefix(id, e.typ) {
			return e, true
		}
	}
	return entry{}, false
}

// Result holds the hydrated entities and the edges that pointed at nothing.
type Result[T any] struct {
	Items []T

	// Missing lists the target node IDs with no entity item, in edge order.
	// Nothing enforces referential integrity across tables, so dangling edges
	// are reported as data instead of shrinking Items silently.
	Missing []string
}

type target struct {
	id    string
	table string
	key   map[string]types.AttributeValue
	sig   string
}

// All loads the target entity of every edge into T. Targets are deduplicated,
// and Items follows the order in which targets first appear in edges.
func All[T any](ctx context.Context, c *dynamodb.Client, r *Registry, edges []ddbrel.Edge) (Result[T], error) {
	var res Result[T]

	targets, err := plan(r, edges)
	if err != nil {
		return res, err
	}
	if len(targets) == 0 {
		return res, nil
	}

	found := make(map[string]map[string]types.AttributeValue, len(targets))
	for _, chunk := range chunk(targets, maxBatchKeys) {
		if err := fetch(ctx, c, chunk, found); err != nil {
			return res, err
		}
	}

	for _, t := range targets {
		item, ok := found[t.table+"\x00"+t.sig]
		if !ok {
			res.Missing = append(res.Missing, t.id)
			continue
		}
		var v T
		if err := attributevalue.UnmarshalMap(item, &v); err != nil {
			return res, fmt.Errorf("ddbrel/hydrate: unmarshal %q from %q: %w", t.id, t.table, err)
		}
		res.Items = append(res.Items, v)
	}
	return res, nil
}

// Sweep removes the edges whose targets are missing, turning a Result into
// cleanup. It uses the sort value carried by each edge, so it costs no resolve
// query.
func Sweep(ctx context.Context, s *ddbrel.Store, edges []ddbrel.Edge, missing []string) (int, error) {
	gone := make(map[string]struct{}, len(missing))
	for _, id := range missing {
		gone[id] = struct{}{}
	}

	removed := 0
	for _, e := range edges {
		if _, ok := gone[e.To]; !ok {
			continue
		}
		if err := s.RemoveExact(ctx, e.From, e.To, e.Sort, ddbrel.WithLabel(e.Label)); err != nil {
			return removed, err
		}
		removed++
	}
	return removed, nil
}

func plan(r *Registry, edges []ddbrel.Edge) ([]target, error) {
	seen := make(map[string]struct{}, len(edges))
	targets := make([]target, 0, len(edges))

	for _, e := range edges {
		if _, dup := seen[e.To]; dup {
			continue
		}
		seen[e.To] = struct{}{}

		en, ok := r.lookup(e.To)
		if !ok {
			return nil, fmt.Errorf("%w: %q", ErrUnregisteredType, e.To)
		}
		key := en.key(e.To)
		sig, err := keySignature(key)
		if err != nil {
			return nil, fmt.Errorf("ddbrel/hydrate: key for %q: %w", e.To, err)
		}
		targets = append(targets, target{id: e.To, table: en.table, key: key, sig: sig})
	}
	return targets, nil
}

func fetch(ctx context.Context, c *dynamodb.Client, targets []target, found map[string]map[string]types.AttributeValue) error {
	names := make(map[string][]string, 2)
	req := make(map[string]types.KeysAndAttributes, 2)
	for _, t := range targets {
		ka := req[t.table]
		ka.Keys = append(ka.Keys, t.key)
		req[t.table] = ka
		if _, ok := names[t.table]; !ok {
			names[t.table] = keyNames(t.key)
		}
	}

	for attempt := 0; ; attempt++ {
		out, err := c.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: req})
		if err != nil {
			return fmt.Errorf("ddbrel/hydrate: batch get: %w", err)
		}

		for table, items := range out.Responses {
			for _, item := range items {
				sig, err := keySignature(project(item, names[table]))
				if err != nil {
					return fmt.Errorf("ddbrel/hydrate: item from %q: %w", table, err)
				}
				found[table+"\x00"+sig] = item
			}
		}

		if countKeys(out.UnprocessedKeys) == 0 {
			return nil
		}
		if attempt >= maxUnprocessedRetries {
			return fmt.Errorf("%w: %d after %d retries", ErrUnprocessed,
				countKeys(out.UnprocessedKeys), maxUnprocessedRetries)
		}
		if err := wait(ctx, backoff(attempt)); err != nil {
			return err
		}
		req = out.UnprocessedKeys
	}
}

func countKeys(req map[string]types.KeysAndAttributes) int {
	n := 0
	for _, ka := range req {
		n += len(ka.Keys)
	}
	return n
}

func backoff(attempt int) time.Duration {
	d := baseBackoff << attempt
	if d > maxBackoff || d <= 0 {
		return maxBackoff
	}
	return d
}

func wait(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func chunk(targets []target, n int) [][]target {
	var out [][]target
	for i := 0; i < len(targets); i += n {
		end := i + n
		if end > len(targets) {
			end = len(targets)
		}
		out = append(out, targets[i:end])
	}
	return out
}

func keyNames(key map[string]types.AttributeValue) []string {
	names := make([]string, 0, len(key))
	for k := range key {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

func project(item map[string]types.AttributeValue, names []string) map[string]types.AttributeValue {
	out := make(map[string]types.AttributeValue, len(names))
	for _, n := range names {
		if v, ok := item[n]; ok {
			out[n] = v
		}
	}
	return out
}

// keySignature identifies an entity key so a returned item can be matched back
// to the node that asked for it.
func keySignature(key map[string]types.AttributeValue) (string, error) {
	if len(key) == 0 {
		return "", errors.New("empty key")
	}
	parts := make([]string, 0, len(key))
	for _, name := range keyNames(key) {
		v, err := scalar(key[name])
		if err != nil {
			return "", fmt.Errorf("attribute %q: %w", name, err)
		}
		parts = append(parts, name+"="+v)
	}
	return strings.Join(parts, "\x00"), nil
}

func scalar(v types.AttributeValue) (string, error) {
	switch t := v.(type) {
	case *types.AttributeValueMemberS:
		return "S:" + t.Value, nil
	case *types.AttributeValueMemberN:
		return "N:" + t.Value, nil
	case *types.AttributeValueMemberB:
		return "B:" + base64.StdEncoding.EncodeToString(t.Value), nil
	default:
		return "", errors.New("key attributes must be S, N or B")
	}
}
