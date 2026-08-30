//go:build integration

package ddbrel_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"

	"github.com/rahardianart/ddbrel"
	"github.com/rahardianart/ddbrel/hydrate"
)

func testClient(t *testing.T, opts ...func(*dynamodb.Options)) *dynamodb.Client {
	t.Helper()

	endpoint := os.Getenv("DDB_ENDPOINT")
	if endpoint == "" {
		t.Skip("DDB_ENDPOINT not set; skipping integration tests")
	}

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	opts = append([]func(*dynamodb.Options){func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	}}, opts...)

	return dynamodb.NewFromConfig(cfg, opts...)
}

func createTable(t *testing.T, c *dynamodb.Client, prefix string, key []types.KeySchemaElement, attrs []types.AttributeDefinition) string {
	t.Helper()

	safe := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	name := fmt.Sprintf("%s_%s_%d", prefix, safe, time.Now().UnixNano())

	ctx := context.Background()
	_, err := c.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String(name),
		BillingMode:          types.BillingModePayPerRequest,
		AttributeDefinitions: attrs,
		KeySchema:            key,
	})
	if err != nil {
		t.Fatalf("create table %s: %v", name, err)
	}

	if err := dynamodb.NewTableExistsWaiter(c).Wait(ctx,
		&dynamodb.DescribeTableInput{TableName: aws.String(name)},
		30*time.Second,
	); err != nil {
		t.Fatalf("wait for table %s: %v", name, err)
	}

	t.Cleanup(func() {
		_, _ = c.DeleteTable(context.Background(),
			&dynamodb.DeleteTableInput{TableName: aws.String(name)})
	})

	return name
}

func newTable(t *testing.T, c *dynamodb.Client) string {
	t.Helper()

	return createTable(t, c, "edges",
		[]types.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("SK"), KeyType: types.KeyTypeRange},
		},
		[]types.AttributeDefinition{
			{AttributeName: aws.String("PK"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("SK"), AttributeType: types.ScalarAttributeTypeS},
		})
}

func newEntityTable(t *testing.T, c *dynamodb.Client) string {
	t.Helper()

	return createTable(t, c, "orders",
		[]types.KeySchemaElement{
			{AttributeName: aws.String("ID"), KeyType: types.KeyTypeHash},
		},
		[]types.AttributeDefinition{
			{AttributeName: aws.String("ID"), AttributeType: types.ScalarAttributeTypeS},
		})
}

func newStore(t *testing.T) (*ddbrel.Store, *dynamodb.Client, string) {
	t.Helper()

	c := testClient(t)
	table := newTable(t, c)
	return ddbrel.New(c, table), c, table
}

func edgeStrings(edges []ddbrel.Edge) []string {
	out := make([]string, 0, len(edges))
	for _, e := range edges {
		out = append(out, fmt.Sprintf("%s-%s->%s@%s", e.From, e.Label, e.To, e.Sort))
	}
	return out
}

func mustOut(t *testing.T, s *ddbrel.Store, node string, o ...ddbrel.QueryOption) ddbrel.Page {
	t.Helper()

	p, err := s.Out(context.Background(), node, append(o, ddbrel.WithConsistentRead())...)
	if err != nil {
		t.Fatalf("Out(%q): %v", node, err)
	}
	return p
}

func mustIn(t *testing.T, s *ddbrel.Store, node string, o ...ddbrel.QueryOption) ddbrel.Page {
	t.Helper()

	p, err := s.In(context.Background(), node, append(o, ddbrel.WithConsistentRead())...)
	if err != nil {
		t.Fatalf("In(%q): %v", node, err)
	}
	return p
}

func TestTransactionAtomicity(t *testing.T) {
	t.Parallel()

	s, _, _ := newStore(t)
	ctx := context.Background()

	if err := s.Add(ctx, "user-1", "order-5",
		ddbrel.WithLabel("PLACED"), ddbrel.WithSort("20260830T120400Z")); err != nil {
		t.Fatalf("Add: %v", err)
	}

	forward := mustOut(t, s, "user-1", ddbrel.WithLabel("PLACED"))
	inverse := mustIn(t, s, "order-5", ddbrel.WithLabel("PLACED"))
	if len(forward.Edges) != 1 || len(inverse.Edges) != 1 {
		t.Fatalf("forward %v, inverse %v; want one item each",
			edgeStrings(forward.Edges), edgeStrings(inverse.Edges))
	}
	if edgeStrings(forward.Edges)[0] != edgeStrings(inverse.Edges)[0] {
		t.Errorf("forward %+v and inverse %+v describe different edges",
			forward.Edges[0], inverse.Edges[0])
	}

	t.Run("rejected write leaves nothing behind", func(t *testing.T) {
		big := strings.Repeat("x", 400*1024)
		err := s.Add(ctx, "user-2", "order-6",
			ddbrel.WithLabel("PLACED"),
			ddbrel.WithAttrs(map[string]types.AttributeValue{
				"Blob": &types.AttributeValueMemberS{Value: big},
			}))
		if err == nil {
			t.Skip("emulator accepted an oversized item; no rejected transaction to observe")
		}

		if p := mustOut(t, s, "user-2"); len(p.Edges) != 0 {
			t.Errorf("forward item survived a cancelled transaction: %v", edgeStrings(p.Edges))
		}
		if p := mustIn(t, s, "order-6"); len(p.Edges) != 0 {
			t.Errorf("inverse item survived a cancelled transaction: %v", edgeStrings(p.Edges))
		}
	})

	t.Run("remove deletes both items", func(t *testing.T) {
		if err := s.Remove(ctx, "user-1", "order-5", ddbrel.WithLabel("PLACED")); err != nil {
			t.Fatalf("Remove: %v", err)
		}
		if p := mustOut(t, s, "user-1"); len(p.Edges) != 0 {
			t.Errorf("forward item survived Remove: %v", edgeStrings(p.Edges))
		}
		if p := mustIn(t, s, "order-5"); len(p.Edges) != 0 {
			t.Errorf("inverse item survived Remove: %v", edgeStrings(p.Edges))
		}
		if err := s.Remove(ctx, "user-1", "order-5", ddbrel.WithLabel("PLACED")); err != nil {
			t.Errorf("second Remove is not idempotent: %v", err)
		}
	})
}

// TestDirectionIsolation is the regression test for direction living in the sort
// key prefix: a node's partition mixes its outgoing edges with the inverse items
// of edges pointing at it.
func TestDirectionIsolation(t *testing.T) {
	t.Parallel()

	s, _, _ := newStore(t)
	ctx := context.Background()

	if err := s.Add(ctx, "user-1", "order-5", ddbrel.WithLabel("PLACED"), ddbrel.WithSort("20260830T120400Z")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := s.Add(ctx, "session-9", "user-1", ddbrel.WithLabel("AUTHENTICATED"), ddbrel.WithSort("20260830T120000Z")); err != nil {
		t.Fatalf("Add: %v", err)
	}

	tests := []struct {
		name string
		page ddbrel.Page
		want string
	}{
		{"out of user-1", mustOut(t, s, "user-1"), "user-1-PLACED->order-5@20260830T120400Z"},
		{"in of user-1", mustIn(t, s, "user-1"), "session-9-AUTHENTICATED->user-1@20260830T120000Z"},
		{"in of order-5", mustIn(t, s, "order-5"), "user-1-PLACED->order-5@20260830T120400Z"},
		{"out of session-9", mustOut(t, s, "session-9"), "session-9-AUTHENTICATED->user-1@20260830T120000Z"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := edgeStrings(tt.page.Edges)
			if len(got) != 1 || got[0] != tt.want {
				t.Fatalf("edges = %v, want [%s]", got, tt.want)
			}
		})
	}

	if p := mustOut(t, s, "order-5"); len(p.Edges) != 0 {
		t.Errorf("order-5 has outgoing edges: %v", edgeStrings(p.Edges))
	}
	if p := mustIn(t, s, "session-9"); len(p.Edges) != 0 {
		t.Errorf("session-9 has incoming edges: %v", edgeStrings(p.Edges))
	}
}

func TestResolveFirstUpsert(t *testing.T) {
	t.Parallel()

	s, _, _ := newStore(t)
	ctx := context.Background()

	for _, sort := range []string{"20260830T120400Z", "20260901T090000Z", "20260901T090000Z"} {
		if err := s.Add(ctx, "user-1", "order-5", ddbrel.WithLabel("PLACED"), ddbrel.WithSort(sort)); err != nil {
			t.Fatalf("Add at %s: %v", sort, err)
		}
	}

	forward := mustOut(t, s, "user-1")
	inverse := mustIn(t, s, "order-5")
	if len(forward.Edges) != 1 || len(inverse.Edges) != 1 {
		t.Fatalf("forward %v, inverse %v; want one item each",
			edgeStrings(forward.Edges), edgeStrings(inverse.Edges))
	}
	if forward.Edges[0].Sort != "20260901T090000Z" {
		t.Errorf("sort = %q, want the latest value", forward.Edges[0].Sort)
	}

	t.Run("a different label is a different edge", func(t *testing.T) {
		if err := s.Add(ctx, "user-1", "order-5", ddbrel.WithLabel("VIEWED"), ddbrel.WithSort("20260901T100000Z")); err != nil {
			t.Fatalf("Add: %v", err)
		}
		if p := mustOut(t, s, "user-1"); len(p.Edges) != 2 {
			t.Fatalf("edges = %v, want two labelled edges", edgeStrings(p.Edges))
		}
		if p := mustOut(t, s, "user-1", ddbrel.WithLabel("PLACED")); len(p.Edges) != 1 {
			t.Fatalf("PLACED edges = %v, want one", edgeStrings(p.Edges))
		}
	})

	t.Run("AddExact skips the resolve", func(t *testing.T) {
		if err := s.AddExact(ctx, "user-2", "order-9", "0000000001", ddbrel.WithLabel("PLACED")); err != nil {
			t.Fatalf("AddExact: %v", err)
		}
		if err := s.RemoveExact(ctx, "user-2", "order-9", "0000000001", ddbrel.WithLabel("PLACED")); err != nil {
			t.Fatalf("RemoveExact: %v", err)
		}
		if p := mustOut(t, s, "user-2"); len(p.Edges) != 0 {
			t.Fatalf("edges = %v, want none", edgeStrings(p.Edges))
		}
	})
}

func TestPagination(t *testing.T) {
	t.Parallel()

	s, _, _ := newStore(t)
	ctx := context.Background()

	const total = 7
	want := make([]string, 0, total)
	for i := 1; i <= total; i++ {
		sort := fmt.Sprintf("%010d", i)
		to := fmt.Sprintf("order-%02d", i)
		if err := s.AddExact(ctx, "user-1", to, sort, ddbrel.WithLabel("PLACED")); err != nil {
			t.Fatalf("AddExact %s: %v", to, err)
		}
		want = append(want, fmt.Sprintf("user-1-PLACED->%s@%s", to, sort))
	}

	t.Run("forward across page boundaries", func(t *testing.T) {
		var got []string
		var cursor *ddbrel.Cursor
		for pages := 0; ; pages++ {
			if pages > total {
				t.Fatalf("pagination did not terminate after %d pages", pages)
			}
			opts := []ddbrel.QueryOption{ddbrel.WithLabel("PLACED"), ddbrel.WithLimit(3)}
			if cursor != nil {
				opts = append(opts, ddbrel.WithCursor(cursor))
			}
			p := mustOut(t, s, "user-1", opts...)
			if len(p.Edges) > 3 {
				t.Fatalf("page holds %d edges, over the limit", len(p.Edges))
			}
			got = append(got, edgeStrings(p.Edges)...)
			if p.Next == nil {
				break
			}
			cursor = p.Next
		}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("edges = %v, want %v", got, want)
		}
	})

	t.Run("cursor survives encoding", func(t *testing.T) {
		p := mustOut(t, s, "user-1", ddbrel.WithLabel("PLACED"), ddbrel.WithLimit(2))
		if p.Next == nil {
			t.Fatal("first page has no cursor")
		}
		c, err := ddbrel.ParseCursor(p.Next.String())
		if err != nil {
			t.Fatalf("ParseCursor: %v", err)
		}
		next := mustOut(t, s, "user-1", ddbrel.WithLabel("PLACED"), ddbrel.WithLimit(2), ddbrel.WithCursor(c))
		if got := edgeStrings(next.Edges); strings.Join(got, ",") != strings.Join(want[2:4], ",") {
			t.Fatalf("second page = %v, want %v", got, want[2:4])
		}
	})

	t.Run("cursor from another query is rejected", func(t *testing.T) {
		p := mustOut(t, s, "user-1", ddbrel.WithLabel("PLACED"), ddbrel.WithLimit(2))
		if p.Next == nil {
			t.Fatal("first page has no cursor")
		}
		_, err := s.In(ctx, "user-1", ddbrel.WithLabel("PLACED"), ddbrel.WithCursor(p.Next))
		if !errors.Is(err, ddbrel.ErrCursorMismatch) {
			t.Fatalf("In with a foreign cursor = %v, want ErrCursorMismatch", err)
		}
	})

	t.Run("reverse", func(t *testing.T) {
		p := mustOut(t, s, "user-1", ddbrel.WithLabel("PLACED"), ddbrel.WithReverse())
		got := edgeStrings(p.Edges)
		for i, j := 0, len(want)-1; j >= 0; i, j = i+1, j-1 {
			if got[i] != want[j] {
				t.Fatalf("reversed edges = %v, want the reverse of %v", got, want)
			}
		}
	})

	t.Run("sort range", func(t *testing.T) {
		p := mustOut(t, s, "user-1", ddbrel.WithLabel("PLACED"), ddbrel.WithSortRange("0000000003", "0000000005"))
		if got := edgeStrings(p.Edges); strings.Join(got, ",") != strings.Join(want[2:5], ",") {
			t.Fatalf("range = %v, want %v", got, want[2:5])
		}
	})
}

// failTransactAfter injects a client-side failure once n TransactWriteItems calls
// have been made, so an interrupted RemoveAll can be observed.
func failTransactAfter(n int32) func(*dynamodb.Options) {
	var calls atomic.Int32
	return func(o *dynamodb.Options) {
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("ddbrelFaultInjector",
				func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (
					middleware.InitializeOutput, middleware.Metadata, error) {
					if _, ok := in.Parameters.(*dynamodb.TransactWriteItemsInput); ok && calls.Add(1) > n {
						return middleware.InitializeOutput{}, middleware.Metadata{},
							errors.New("injected transport failure")
					}
					return next.HandleInitialize(ctx, in)
				}), middleware.Before)
		})
	}
}

func TestRemoveAllResumability(t *testing.T) {
	t.Parallel()

	c := testClient(t)
	table := newTable(t, c)
	s := ddbrel.New(c, table)
	ctx := context.Background()

	const total = 60
	for i := 0; i < total; i++ {
		to := fmt.Sprintf("order-%03d", i)
		if err := s.AddExact(ctx, "user-1", to, fmt.Sprintf("%010d", i), ddbrel.WithLabel("PLACED")); err != nil {
			t.Fatalf("AddExact %s: %v", to, err)
		}
	}

	interrupted := ddbrel.New(testClient(t, failTransactAfter(1)), table)
	removed, err := interrupted.RemoveAll(ctx, "user-1")
	if err == nil {
		t.Fatal("RemoveAll survived an injected failure")
	}
	if removed != 50 {
		t.Fatalf("interrupted RemoveAll removed %d, want the first batch of 50", removed)
	}

	rest, err := s.RemoveAll(ctx, "user-1")
	if err != nil {
		t.Fatalf("resumed RemoveAll: %v", err)
	}
	if rest != total-removed {
		t.Fatalf("resumed RemoveAll removed %d, want %d", rest, total-removed)
	}

	again, err := s.RemoveAll(ctx, "user-1")
	if err != nil {
		t.Fatalf("third RemoveAll: %v", err)
	}
	if again != 0 {
		t.Fatalf("third RemoveAll removed %d, want 0", again)
	}

	if p := mustOut(t, s, "user-1"); len(p.Edges) != 0 {
		t.Errorf("edges survived RemoveAll: %v", edgeStrings(p.Edges))
	}
	for i := 0; i < total; i++ {
		node := fmt.Sprintf("order-%03d", i)
		if p := mustIn(t, s, node); len(p.Edges) != 0 {
			t.Fatalf("inverse item survived RemoveAll at %s: %v", node, edgeStrings(p.Edges))
		}
	}
}

type order struct {
	ID    string `dynamodbav:"ID"`
	Total int    `dynamodbav:"Total"`
}

func TestHydrateReportsDanglingEdges(t *testing.T) {
	t.Parallel()

	c := testClient(t)
	s := ddbrel.New(c, newTable(t, c))
	entities := newEntityTable(t, c)
	ctx := context.Background()

	for _, id := range []string{"order:5", "order:6"} {
		_, err := c.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(entities),
			Item: map[string]types.AttributeValue{
				"ID":    &types.AttributeValueMemberS{Value: id},
				"Total": &types.AttributeValueMemberN{Value: "99"},
			},
		})
		if err != nil {
			t.Fatalf("put %s: %v", id, err)
		}
	}

	for i, to := range []string{"order:5", "order:6"} {
		if err := s.AddExact(ctx, "user-1", to, fmt.Sprintf("%010d", i), ddbrel.WithLabel("PLACED")); err != nil {
			t.Fatalf("AddExact %s: %v", to, err)
		}
	}

	if _, err := c.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(entities),
		Key:       map[string]types.AttributeValue{"ID": &types.AttributeValueMemberS{Value: "order:6"}},
	}); err != nil {
		t.Fatalf("delete order:6: %v", err)
	}

	var reg hydrate.Registry
	reg.Register("order:", entities, func(id string) map[string]types.AttributeValue {
		return map[string]types.AttributeValue{"ID": &types.AttributeValueMemberS{Value: id}}
	})

	edges := mustOut(t, s, "user-1").Edges
	res, err := hydrate.All[order](ctx, c, &reg, edges)
	if err != nil {
		t.Fatalf("hydrate.All: %v", err)
	}
	if len(res.Items) != 1 || res.Items[0].ID != "order:5" || res.Items[0].Total != 99 {
		t.Fatalf("items = %+v, want only order:5", res.Items)
	}
	if len(res.Missing) != 1 || res.Missing[0] != "order:6" {
		t.Fatalf("missing = %v, want [order:6]", res.Missing)
	}

	t.Run("sweep removes the dangling edge", func(t *testing.T) {
		removed, err := hydrate.Sweep(ctx, s, edges, res.Missing)
		if err != nil {
			t.Fatalf("Sweep: %v", err)
		}
		if removed != 1 {
			t.Fatalf("Sweep removed %d, want 1", removed)
		}
		p := mustOut(t, s, "user-1")
		if len(p.Edges) != 1 || p.Edges[0].To != "order:5" {
			t.Fatalf("edges after sweep = %v, want only order:5", edgeStrings(p.Edges))
		}
		if p := mustIn(t, s, "order:6"); len(p.Edges) != 0 {
			t.Fatalf("inverse item survived the sweep: %v", edgeStrings(p.Edges))
		}
	})
}

// TestRewriteAfterRemoveIsNotSwallowed guards a silent data-loss bug: the
// ClientRequestToken used to be derived from the write's identity, so an Add
// after a Remove produced a byte-identical request and DynamoDB replayed it as a
// no-op inside its 10 minute idempotency window. The call reported success and
// the edge was never recreated.
func TestRewriteAfterRemoveIsNotSwallowed(t *testing.T) {
	t.Parallel()

	s, _, _ := newStore(t)
	ctx := context.Background()

	count := func(node string) int {
		t.Helper()
		p, err := s.Out(ctx, node, ddbrel.WithLabel("L"), ddbrel.WithConsistentRead())
		if err != nil {
			t.Fatalf("Out: %v", err)
		}
		return len(p.Edges)
	}

	tests := []struct {
		name   string
		add    func() error
		remove func() error
	}{
		{
			name:   "Add after Remove",
			add:    func() error { return s.Add(ctx, "u", "o", ddbrel.WithLabel("L"), ddbrel.WithSort("s1")) },
			remove: func() error { return s.Remove(ctx, "u", "o", ddbrel.WithLabel("L")) },
		},
		{
			name:   "AddExact after RemoveExact",
			add:    func() error { return s.AddExact(ctx, "u2", "o2", "s1", ddbrel.WithLabel("L")) },
			remove: func() error { return s.RemoveExact(ctx, "u2", "o2", "s1", ddbrel.WithLabel("L")) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := "u"
			if strings.Contains(tt.name, "Exact") {
				node = "u2"
			}

			if err := tt.add(); err != nil {
				t.Fatalf("first add: %v", err)
			}
			if got := count(node); got != 1 {
				t.Fatalf("after first add: %d edges, want 1", got)
			}

			if err := tt.remove(); err != nil {
				t.Fatalf("remove: %v", err)
			}
			if got := count(node); got != 0 {
				t.Fatalf("after remove: %d edges, want 0", got)
			}

			if err := tt.add(); err != nil {
				t.Fatalf("second add: %v", err)
			}
			if got := count(node); got != 1 {
				t.Fatalf("after re-add: %d edges, want 1 — the write was swallowed as a replay", got)
			}

			if err := tt.remove(); err != nil {
				t.Fatalf("second remove: %v", err)
			}
			if got := count(node); got != 0 {
				t.Fatalf("after second remove: %d edges, want 0 — the delete was swallowed as a replay", got)
			}
		})
	}
}
