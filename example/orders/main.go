package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func main() {
	serve := flag.String("serve", "", "listen address for the HTTP API, e.g. :8080; omit to run the scenario")
	flag.Parse()

	endpoint := os.Getenv("DDB_ENDPOINT")
	if endpoint == "" {
		log.Fatal("DDB_ENDPOINT is required (http://localhost:4566 for LocalStack, :8000 for DynamoDB Local)")
	}

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	c := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	if *serve != "" {
		const edgeTable, orderTable = "orders_edges", "orders"
		ensureTable(ctx, c, edgeTable, "PK", "SK")
		ensureTable(ctx, c, orderTable, "id", "")

		a := &api{svc: NewService(c, edgeTable, orderTable)}
		log.Printf("listening on %s, DynamoDB at %s", *serve, endpoint)
		log.Fatal(http.ListenAndServe(*serve, a.routes()))
	}

	edgeTable := fmt.Sprintf("demo_edges_%d", time.Now().UnixNano())
	orderTable := fmt.Sprintf("demo_orders_%d", time.Now().UnixNano())
	mustTable(ctx, c, edgeTable, "PK", "SK")
	mustTable(ctx, c, orderTable, "id", "")
	defer drop(ctx, c, edgeTable, orderTable)

	s := NewService(c, edgeTable, orderTable)

	step("1. place three orders for user-1 (one-to-many)")
	for _, o := range []struct {
		Order
		at string
	}{
		{Order{"order-1", 1200}, "20260801T090000Z"},
		{Order{"order-2", 450}, "20260815T140000Z"},
		{Order{"order-3", 8900}, "20260829T101500Z"},
	} {
		if err := s.PlaceOrder(ctx, "user-1", o.Order, o.at); err != nil {
			log.Fatalf("PlaceOrder: %v", err)
		}
	}
	fmt.Println("   placed order-1, order-2, order-3")

	step("2. two most recent orders, newest first (ordered query + hydration)")
	orders, missing, err := s.RecentOrders(ctx, "user-1", 2)
	if err != nil {
		log.Fatalf("RecentOrders: %v", err)
	}
	for _, o := range orders {
		fmt.Printf("   %s  total=%d\n", o.ID, o.Total)
	}
	fmt.Printf("   missing=%v\n", missing)

	step("3. who placed order-3 (reverse traversal, same edge)")
	buyer, err := s.BuyerOf(ctx, "order-3")
	if err != nil {
		log.Fatalf("BuyerOf: %v", err)
	}
	fmt.Printf("   buyer of order-3 = %s\n", buyer)

	step("4. tag orders (many-to-many, both directions)")
	for _, t := range []struct{ order, tag, at string }{
		{"order-1", "tag-electronics", "20260801T090000Z"},
		{"order-1", "tag-gift", "20260801T090001Z"},
		{"order-3", "tag-electronics", "20260829T101500Z"},
	} {
		if err := s.Tag(ctx, t.order, t.tag, t.at); err != nil {
			log.Fatalf("Tag: %v", err)
		}
	}
	tags, err := s.TagsOf(ctx, "order-1")
	if err != nil {
		log.Fatalf("TagsOf: %v", err)
	}
	fmt.Printf("   tags of order-1        = %v\n", tags)

	withTag, err := s.OrdersWithTag(ctx, "tag-electronics")
	if err != nil {
		log.Fatalf("OrdersWithTag: %v", err)
	}
	fmt.Printf("   orders in electronics  = %v\n", withTag)

	step("5. all outgoing edges of order-1, no label filter (the bug fixed today)")
	edges, err := s.EverythingAbout(ctx, "order-1")
	if err != nil {
		log.Fatalf("EverythingAbout: %v", err)
	}
	for _, e := range edges {
		fmt.Printf("   %s -%s-> %s @ %s\n", e.From, e.Label, e.To, e.Sort)
	}

	step("6. delete order-2's row, leaving a dangling edge")
	if err := s.DeleteOrderRow(ctx, "order-2"); err != nil {
		log.Fatalf("DeleteOrderRow: %v", err)
	}
	orders, missing, err = s.RecentOrders(ctx, "user-1", 10)
	if err != nil {
		log.Fatalf("RecentOrders: %v", err)
	}
	fmt.Printf("   hydrated=%d ", len(orders))
	for _, o := range orders {
		fmt.Printf("%s ", o.ID)
	}
	fmt.Printf("\n   missing =%v\n", missing)

	step("7. close the account (RemoveAll, both directions)")
	n, err := s.CloseAccount(ctx, "user-1")
	if err != nil {
		log.Fatalf("CloseAccount: %v", err)
	}
	fmt.Printf("   removed %d edges\n", n)
	left, _, err := s.RecentOrders(ctx, "user-1", 10)
	if err != nil {
		log.Fatalf("RecentOrders: %v", err)
	}
	fmt.Printf("   user-1 now has %d orders\n", len(left))
	stillTagged, err := s.OrdersWithTag(ctx, "tag-electronics")
	if err != nil {
		log.Fatalf("OrdersWithTag: %v", err)
	}
	fmt.Printf("   order->tag edges survive (untouched by user-1 sweep) = %v\n", stillTagged)

	fmt.Println("\nAll steps completed.")
}

func step(s string) { fmt.Printf("\n== %s\n", s) }

func mustTable(ctx context.Context, c *dynamodb.Client, name, pk, sk string) {
	attrs := []types.AttributeDefinition{{AttributeName: aws.String(pk), AttributeType: types.ScalarAttributeTypeS}}
	key := []types.KeySchemaElement{{AttributeName: aws.String(pk), KeyType: types.KeyTypeHash}}
	if sk != "" {
		attrs = append(attrs, types.AttributeDefinition{AttributeName: aws.String(sk), AttributeType: types.ScalarAttributeTypeS})
		key = append(key, types.KeySchemaElement{AttributeName: aws.String(sk), KeyType: types.KeyTypeRange})
	}

	if _, err := c.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String(name),
		BillingMode:          types.BillingModePayPerRequest,
		AttributeDefinitions: attrs,
		KeySchema:            key,
	}); err != nil {
		log.Fatalf("create table %s: %v", name, err)
	}
	if err := dynamodb.NewTableExistsWaiter(c).Wait(ctx,
		&dynamodb.DescribeTableInput{TableName: aws.String(name)}, 30*time.Second); err != nil {
		log.Fatalf("wait for %s: %v", name, err)
	}
}

// ensureTable is mustTable for the long-lived server tables, which survive
// restarts and so may already exist.
func ensureTable(ctx context.Context, c *dynamodb.Client, name, pk, sk string) {
	if _, err := c.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)}); err == nil {
		return
	}
	mustTable(ctx, c, name, pk, sk)
}

func drop(ctx context.Context, c *dynamodb.Client, names ...string) {
	for _, n := range names {
		_, _ = c.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(n)})
	}
}
