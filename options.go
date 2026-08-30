package ddbrel

import "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

// Option configures a Store.
type Option func(*Store)

// WithKeyNames sets the partition and sort key attribute names of the edge
// table. The default is PK and SK.
func WithKeyNames(pk, sk string) Option {
	return func(s *Store) { s.codec = codec{pk: pk, sk: sk} }
}

// WriteOption configures Add, Remove, RemoveAll and the exact variants.
type WriteOption interface{ applyWrite(*writeOptions) }

// QueryOption configures Out and In.
type QueryOption interface{ applyQuery(*queryOptions) }

// EdgeOption selects a labelled relationship and applies to both reads and writes.
type EdgeOption interface {
	WriteOption
	QueryOption
}

type writeOptions struct {
	label    string
	labelSet bool
	sort     string
	attrs    map[string]types.AttributeValue
}

type queryOptions struct {
	label      string
	lo, hi     string
	rangeSet   bool
	limit      int
	reverse    bool
	consistent bool
	cursor     *Cursor
}

func newWriteOptions(opts []WriteOption) writeOptions {
	var w writeOptions
	for _, o := range opts {
		o.applyWrite(&w)
	}
	return w
}

func newQueryOptions(opts []QueryOption) queryOptions {
	var q queryOptions
	for _, o := range opts {
		o.applyQuery(&q)
	}
	return q
}

type writeOptionFunc func(*writeOptions)

func (f writeOptionFunc) applyWrite(w *writeOptions) { f(w) }

type queryOptionFunc func(*queryOptions)

func (f queryOptionFunc) applyQuery(q *queryOptions) { f(q) }

type labelOption string

func (l labelOption) applyWrite(w *writeOptions) { w.label, w.labelSet = string(l), true }
func (l labelOption) applyQuery(q *queryOptions) { q.label = string(l) }

// WithLabel names the relationship. It defaults to the empty label, which is its
// own label rather than a wildcard: edges written without one are only matched
// by queries without one. On RemoveAll it narrows the sweep to that label.
func WithLabel(label string) EdgeOption { return labelOption(label) }

// WithSort supplies the sort value of an edge written by Add. The value is
// opaque to the library: it is stored verbatim and only checked for the key
// delimiter '#'. Since it is compared lexicographically as part of the sort key,
// a caller ordering by it must use a format that sorts correctly as a string —
// fixed-width and built from characters above '#' (zero-padded numbers, or a
// timestamp layout such as 20260830T120400Z).
func WithSort(sort string) WriteOption {
	return writeOptionFunc(func(w *writeOptions) { w.sort = sort })
}

// WithAttrs attaches attributes to both items of an edge. Attribute names that
// the edge item reserves (the two key attributes, Label, Node and Sort) are
// rejected at write time.
func WithAttrs(attrs map[string]types.AttributeValue) WriteOption {
	return writeOptionFunc(func(w *writeOptions) { w.attrs = attrs })
}

// WithSortRange restricts a query to edges whose sort value falls between lo and
// hi, both inclusive. An empty bound is open.
func WithSortRange(lo, hi string) QueryOption {
	return queryOptionFunc(func(q *queryOptions) { q.lo, q.hi, q.rangeSet = lo, hi, true })
}

// WithLimit caps the number of edges returned by a single query. A truncated
// query returns a non-nil Page.Next.
func WithLimit(n int) QueryOption {
	return queryOptionFunc(func(q *queryOptions) { q.limit = n })
}

// WithReverse returns edges in descending sort key order.
func WithReverse() QueryOption {
	return queryOptionFunc(func(q *queryOptions) { q.reverse = true })
}

// WithConsistentRead opts into a strongly consistent read. Both directions of an
// edge live in the base table, so this is available on Out and In alike.
func WithConsistentRead() QueryOption {
	return queryOptionFunc(func(q *queryOptions) { q.consistent = true })
}

// WithCursor resumes a query from a previous Page.Next. The cursor carries the
// key range it was produced by; using it on a different one fails with
// ErrCursorMismatch.
func WithCursor(c *Cursor) QueryOption {
	return queryOptionFunc(func(q *queryOptions) { q.cursor = c })
}
