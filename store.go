// Package ddbrel manages directed labelled edges between opaque node IDs in a
// dedicated DynamoDB table. Entity tables are never touched; resolving edges
// into entities is the job of the optional ddbrel/hydrate subpackage.
//
// Each edge is stored as two items — forward and inverse — written in one
// TransactWriteItems, so both directions live in the base table and both can be
// read with ConsistentRead. The key layout is:
//
//	PK = <node-id>
//	SK = <DIR>#<LABEL>#<sort>#<node-id>     DIR is OUT or IN
//
// '#' is the key delimiter and is not escaped: node IDs, labels and sort values
// containing it are rejected with ErrInvalidID.
//
// The sort value is opaque to the library and is only compared as a string, as
// part of the sort key. Callers that rely on ordering must choose a format whose
// lexicographic order is the intended order: fixed width, and built from
// characters above '#' — zero-padded numbers, or a timestamp layout such as
// 20260830T120400Z.
package ddbrel

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// maxEdgesPerTransaction keeps RemoveAll batches inside the 100 item limit of
// TransactWriteItems, at three items per edge — forward, inverse and pointer.
const maxEdgesPerTransaction = 33

// maxWriteAttempts bounds the optimistic retry on Add and Remove. Each attempt
// re-reads the pointer, so a writer only loops while others are actively moving
// the same edge; a caller hammering one edge gives up rather than spinning.
const maxWriteAttempts = 5

// Store reads and writes edges in one table.
type Store struct {
	client *dynamodb.Client
	table  string
	codec  codec
}

// New returns a Store over the given edge table.
func New(c *dynamodb.Client, table string, opts ...Option) *Store {
	s := &Store{client: c, table: table, codec: defaultCodec()}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Edge is one relationship. From and To are node IDs as the caller supplied
// them, regardless of which direction the item was read from.
type Edge struct {
	From, To, Label, Sort string
	Attrs                 map[string]types.AttributeValue
}

// Page is one query response. Next is non-nil when more edges match.
type Page struct {
	Edges []Edge
	Next  *Cursor
}

// Add upserts the edge (from, label, to), so calling it again with a different
// sort value moves the edge rather than duplicating it. Use WithSort to set the
// sort value and WithLabel to name the relationship.
//
// It reads the edge's pointer item, then writes under a condition on that
// pointer, so a concurrent Add of the same edge fails its condition instead of
// creating a second copy. A losing writer re-reads and retries; after
// maxWriteAttempts it gives up with ErrConflict.
func (s *Store) Add(ctx context.Context, from, to string, o ...WriteOption) error {
	w := newWriteOptions(o)
	if err := validateEdge(from, to, w.label, w.sort); err != nil {
		return err
	}

	for attempt := 0; attempt < maxWriteAttempts; attempt++ {
		prev, found, err := s.resolveSort(ctx, from, to, w.label)
		if err != nil {
			return err
		}
		g := guardAbsent
		if found {
			g = guardExisting
		}
		err = s.write(ctx, from, to, w, prev, g)
		if err == nil {
			return nil
		}
		if !isConditionFailed(err) {
			return err
		}
	}
	return fmt.Errorf("ddbrel: add edge %q -> %q: %w", from, to, ErrConflict)
}

// AddExact writes the edge at the given sort value without the resolve query,
// for callers that already know it. It does not detect an existing copy of the
// same edge stored under a different sort value.
func (s *Store) AddExact(ctx context.Context, from, to, sort string, o ...WriteOption) error {
	w := newWriteOptions(o)
	w.sort = sort
	if err := validateEdge(from, to, w.label, w.sort); err != nil {
		return err
	}
	return s.write(ctx, from, to, w, "", guardNone)
}

// Remove deletes the edge (from, label, to) in both directions. It resolves the
// stored sort value first and is idempotent: removing an edge that is not there
// is not an error.
func (s *Store) Remove(ctx context.Context, from, to string, o ...WriteOption) error {
	w := newWriteOptions(o)
	if err := validateEdge(from, to, w.label, ""); err != nil {
		return err
	}

	for attempt := 0; attempt < maxWriteAttempts; attempt++ {
		sort, found, err := s.resolveSort(ctx, from, to, w.label)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		err = s.deleteEdge(ctx, Edge{From: from, To: to, Label: w.label, Sort: sort}, true)
		if err == nil {
			return nil
		}
		if !isConditionFailed(err) {
			return err
		}
	}
	return fmt.Errorf("ddbrel: remove edge %q -> %q: %w", from, to, ErrConflict)
}

// RemoveExact deletes the edge at the given sort value without the resolve
// query. It is idempotent.
func (s *Store) RemoveExact(ctx context.Context, from, to, sort string, o ...WriteOption) error {
	w := newWriteOptions(o)
	if err := validateEdge(from, to, w.label, sort); err != nil {
		return err
	}
	return s.deleteEdge(ctx, Edge{From: from, To: to, Label: w.label, Sort: sort}, false)
}

// RemoveAll deletes every edge touching node, in both directions, including each
// edge's pointer item, whose key is derivable from the edge. The OUT# and IN#
// sweeps never see REF# items, so pointers are removed via the edges that own
// them rather than by a third query. Optionally
// narrowed to one label with WithLabel. Inverse items live in other partitions,
// so the sweep is a query followed by batched transactional deletes rather than
// one atomic operation. On failure it returns the number of edges removed so
// far; calling it again resumes.
func (s *Store) RemoveAll(ctx context.Context, node string, o ...WriteOption) (int, error) {
	w := newWriteOptions(o)
	if err := validateComponent("node id", node, false); err != nil {
		return 0, err
	}
	if err := validateComponent("label", w.label, true); err != nil {
		return 0, err
	}

	prefixes := []string{dirPrefix(dirOut), dirPrefix(dirIn)}
	if w.labelSet {
		prefixes = []string{prefixFor(dirOut, w.label), prefixFor(dirIn, w.label)}
	}

	seen := make(map[string]struct{})
	var batch []Edge
	removed := 0

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := s.deleteEdges(ctx, batch); err != nil {
			return err
		}
		removed += len(batch)
		batch = batch[:0]
		return nil
	}

	for _, prefix := range prefixes {
		p := dynamodb.NewQueryPaginator(s.client, s.prefixQuery(node, prefix, true))
		for p.HasMorePages() {
			out, err := p.NextPage(ctx)
			if err != nil {
				return removed, fmt.Errorf("ddbrel: scan edges of %q: %w", node, err)
			}
			for _, item := range out.Items {
				e, err := s.codec.edge(item)
				if err != nil {
					return removed, err
				}
				e.Attrs = nil
				id := strings.Join([]string{e.From, e.Label, e.Sort, e.To}, "\x00")
				if _, dup := seen[id]; dup {
					continue
				}
				seen[id] = struct{}{}

				batch = append(batch, e)
				if len(batch) == maxEdgesPerTransaction {
					if err := flush(); err != nil {
						return removed, err
					}
				}
			}
		}
	}
	if err := flush(); err != nil {
		return removed, err
	}
	return removed, nil
}

// Out returns edges pointing away from node.
func (s *Store) Out(ctx context.Context, node string, o ...QueryOption) (Page, error) {
	return s.query(ctx, dirOut, node, o)
}

// In returns edges pointing at node.
func (s *Store) In(ctx context.Context, node string, o ...QueryOption) (Page, error) {
	return s.query(ctx, dirIn, node, o)
}

func (s *Store) query(ctx context.Context, dir, node string, opts []QueryOption) (Page, error) {
	q := newQueryOptions(opts)
	in, err := s.queryInput(dir, node, q)
	if err != nil {
		return Page{}, err
	}

	out, err := s.client.Query(ctx, in)
	if err != nil {
		return Page{}, fmt.Errorf("ddbrel: query %s edges of %q: %w", strings.ToLower(dir), node, err)
	}

	page := Page{Edges: make([]Edge, 0, len(out.Items))}
	for _, item := range out.Items {
		e, err := s.codec.edge(item)
		if err != nil {
			return Page{}, err
		}
		page.Edges = append(page.Edges, e)
	}
	if len(out.LastEvaluatedKey) > 0 {
		c, err := newCursor(dir, node, q, out.LastEvaluatedKey, s.codec)
		if err != nil {
			return Page{}, err
		}
		page.Next = c
	}
	return page, nil
}

func (s *Store) queryInput(dir, node string, q queryOptions) (*dynamodb.QueryInput, error) {
	if err := validateComponent("node id", node, false); err != nil {
		return nil, err
	}
	if err := validateComponent("label", q.label, true); err != nil {
		return nil, err
	}
	if err := validateComponent("sort value", q.lo, true); err != nil {
		return nil, err
	}
	if err := validateComponent("sort value", q.hi, true); err != nil {
		return nil, err
	}

	if q.rangeSet && !q.labelSet {
		return nil, fmt.Errorf("ddbrel: sort range needs a label: sort values order within a label, not across labels")
	}

	prefix := dirPrefix(dir)
	if q.labelSet {
		prefix = prefixFor(dir, q.label)
	}
	values := map[string]types.AttributeValue{
		":pk": &types.AttributeValueMemberS{Value: node},
	}

	cond := "#pk = :pk AND "
	if q.rangeSet {
		values[":lo"] = &types.AttributeValueMemberS{Value: prefix + q.lo}
		values[":hi"] = &types.AttributeValueMemberS{Value: rangeUpper(prefix, q.hi)}
		cond += "#sk BETWEEN :lo AND :hi"
	} else {
		values[":prefix"] = &types.AttributeValueMemberS{Value: prefix}
		cond += "begins_with(#sk, :prefix)"
	}

	in := &dynamodb.QueryInput{
		TableName:                 aws.String(s.table),
		KeyConditionExpression:    aws.String(cond),
		ExpressionAttributeNames:  map[string]string{"#pk": s.codec.pk, "#sk": s.codec.sk},
		ExpressionAttributeValues: values,
	}
	if q.limit > 0 {
		in.Limit = aws.Int32(int32(q.limit))
	}
	if q.reverse {
		in.ScanIndexForward = aws.Bool(false)
	}
	if q.consistent {
		in.ConsistentRead = aws.Bool(true)
	}
	if q.cursor != nil {
		start, err := q.cursor.startKey(dir, node, q, s.codec)
		if err != nil {
			return nil, err
		}
		in.ExclusiveStartKey = start
	}
	return in, nil
}

func rangeUpper(prefix, hi string) string {
	if hi == "" {
		return upperBound(prefix)
	}
	return upperBound(prefix + hi + delim)
}

func (s *Store) prefixQuery(node, prefix string, consistent bool) *dynamodb.QueryInput {
	return &dynamodb.QueryInput{
		TableName:              aws.String(s.table),
		KeyConditionExpression: aws.String("#pk = :pk AND begins_with(#sk, :prefix)"),
		ExpressionAttributeNames: map[string]string{
			"#pk": s.codec.pk,
			"#sk": s.codec.sk,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":     &types.AttributeValueMemberS{Value: node},
			":prefix": &types.AttributeValueMemberS{Value: prefix},
		},
		ConsistentRead: aws.Bool(consistent),
	}
}

// resolveSort finds the sort value an edge is currently stored under by reading
// the target's inverse range, which holds one node's edges rather than the
// source's. The read is strongly consistent: a stale answer here is exactly how
// the duplicate edge the upsert prevents would be created.
// resolveSort reads the edge's pointer item. The pointer's key is fully
// determined by (from, label, to), so this is a single strongly consistent
// GetItem — one read unit, constant time. It replaces a query over the target's
// entire incoming range, whose cost grew with the target's fan-in and which
// measured two writes per second against a node with 40,000 edges.
func (s *Store) resolveSort(ctx context.Context, from, to, label string) (string, bool, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(s.table),
		Key:            s.codec.key(to, refKey(label, from)),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return "", false, fmt.Errorf("ddbrel: resolve edge %q -> %q: %w", from, to, err)
	}
	if out.Item == nil {
		return "", false, nil
	}
	sort, ok := stringAttr(out.Item[attrSort])
	if !ok {
		return "", false, fmt.Errorf("ddbrel: pointer for %q -> %q has no %s", from, to, attrSort)
	}
	return sort, true, nil
}

// Has reports whether the edge (from, label, to) exists, and its sort value. It
// is a single GetItem on the pointer item, so it costs one read unit regardless
// of how many edges either endpoint has.
func (s *Store) Has(ctx context.Context, from, to string, o ...WriteOption) (sort string, ok bool, err error) {
	w := newWriteOptions(o)
	if err := validateEdge(from, to, w.label, ""); err != nil {
		return "", false, err
	}
	return s.resolveSort(ctx, from, to, w.label)
}

// write stores an edge as three items in one transaction: the forward and inverse
// items that carry ordering, and a sort-free pointer item that gives the edge a
// single addressable identity.
//
// The pointer put carries a condition. When no edge existed the condition is
// attribute_not_exists, so of two concurrent creators exactly one commits. When
// one existed the condition pins the sort value the caller resolved, so a
// concurrent move is rejected rather than interleaved. Either way the loser sees
// a cancelled transaction and retries.
func (s *Store) write(ctx context.Context, from, to string, w writeOptions, prevSort string, g guard) error {
	forward, err := s.codec.item(from, sortKey(dirOut, w.label, w.sort, to), w.label, to, w.sort, w.attrs)
	if err != nil {
		return err
	}
	inverse, err := s.codec.item(to, sortKey(dirIn, w.label, w.sort, from), w.label, from, w.sort, w.attrs)
	if err != nil {
		return err
	}
	pointer, err := s.codec.item(to, refKey(w.label, from), w.label, from, w.sort, nil)
	if err != nil {
		return err
	}

	var items []types.TransactWriteItem
	var names []string
	if g == guardExisting && prevSort != w.sort {
		items = append(items,
			s.deleteItem(from, sortKey(dirOut, w.label, prevSort, to)),
			s.deleteItem(to, sortKey(dirIn, w.label, prevSort, from)))
		names = append(names, "previous forward item delete", "previous inverse item delete")
	}
	items = append(items, s.putItem(forward), s.putItem(inverse), s.guardedPointer(pointer, prevSort, g))
	names = append(names, "forward item put", "inverse item put", "pointer put")

	return s.transact(ctx, items, names)
}

// guard selects the condition placed on the pointer put.
type guard int

const (
	// guardNone writes the pointer unconditionally. AddExact uses it: the caller
	// asserts the sort value, so repeating the same call must overwrite rather
	// than fail.
	guardNone guard = iota
	// guardAbsent requires that no edge exists, so of two concurrent creators
	// exactly one commits.
	guardAbsent
	// guardExisting pins the sort value the caller resolved, so a concurrent move
	// of the same edge is rejected rather than interleaved.
	guardExisting
)

// guardedPointer is the put that settles concurrent writers of one edge.
func (s *Store) guardedPointer(item map[string]types.AttributeValue, prevSort string, g guard) types.TransactWriteItem {
	put := &types.Put{TableName: aws.String(s.table), Item: item}
	switch g {
	case guardExisting:
		put.ConditionExpression = aws.String("#sort = :prev")
		put.ExpressionAttributeNames = map[string]string{"#sort": attrSort}
		put.ExpressionAttributeValues = map[string]types.AttributeValue{
			":prev": &types.AttributeValueMemberS{Value: prevSort},
		}
	case guardAbsent:
		put.ConditionExpression = aws.String("attribute_not_exists(#sk)")
		put.ExpressionAttributeNames = map[string]string{"#sk": s.codec.sk}
	}
	return types.TransactWriteItem{Put: put}
}

// deleteEdge removes one edge's three items. When guard is set the pointer delete
// is conditional on the sort value the caller resolved, so a concurrent move is
// rejected rather than half-applied.
func (s *Store) deleteEdge(ctx context.Context, e Edge, guard bool) error {
	del := &types.Delete{TableName: aws.String(s.table), Key: s.codec.key(e.To, refKey(e.Label, e.From))}
	if guard {
		del.ConditionExpression = aws.String("#sort = :prev")
		del.ExpressionAttributeNames = map[string]string{"#sort": attrSort}
		del.ExpressionAttributeValues = map[string]types.AttributeValue{
			":prev": &types.AttributeValueMemberS{Value: e.Sort},
		}
	}

	return s.transact(ctx,
		[]types.TransactWriteItem{
			s.deleteItem(e.From, sortKey(dirOut, e.Label, e.Sort, e.To)),
			s.deleteItem(e.To, sortKey(dirIn, e.Label, e.Sort, e.From)),
			{Delete: del},
		},
		[]string{"forward item delete", "inverse item delete", "pointer delete"})
}

func (s *Store) deleteEdges(ctx context.Context, edges []Edge) error {
	var items []types.TransactWriteItem
	var names []string
	seen := make(map[string]struct{}, len(edges)*3)

	for _, e := range edges {
		keys := [3]struct{ pk, sk, name string }{
			{e.From, sortKey(dirOut, e.Label, e.Sort, e.To), "forward item delete"},
			{e.To, sortKey(dirIn, e.Label, e.Sort, e.From), "inverse item delete"},
			{e.To, refKey(e.Label, e.From), "pointer delete"},
		}
		for _, k := range keys {
			id := k.pk + "\x00" + k.sk
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			items = append(items, s.deleteItem(k.pk, k.sk))
			names = append(names, k.name)
		}
	}
	if len(items) == 0 {
		return nil
	}
	return s.transact(ctx, items, names)
}

// transact leaves ClientRequestToken unset. The field carries the SDK's
// idempotencyToken trait, so aws-sdk-go-v2 generates one per call and reuses it
// across that call's retries — which is the only replay that must not apply
// twice. Deriving the token from the write's identity instead made a later,
// legitimate rewrite of the same edge look like a replay of the earlier one, and
// DynamoDB silently discarded it inside its 10 minute window.
func (s *Store) transact(ctx context.Context, items []types.TransactWriteItem, names []string) error {
	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: items,
	})
	if err != nil {
		return unwrapTransaction(err, names)
	}
	return nil
}

func (s *Store) putItem(item map[string]types.AttributeValue) types.TransactWriteItem {
	return types.TransactWriteItem{Put: &types.Put{TableName: aws.String(s.table), Item: item}}
}

func (s *Store) deleteItem(pk, sk string) types.TransactWriteItem {
	return types.TransactWriteItem{Delete: &types.Delete{TableName: aws.String(s.table), Key: s.codec.key(pk, sk)}}
}
