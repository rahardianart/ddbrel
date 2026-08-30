package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/rahardianart/ddbrel"
	"github.com/rahardianart/ddbrel/hydrate"
)

type Order struct {
	ID    string `dynamodbav:"id" json:"id"`
	Total int    `dynamodbav:"total" json:"total"`
}

const (
	labelPlaced = "PLACED"
	labelTagged = "TAGGED"
)

type Service struct {
	edges  *ddbrel.Store
	client *dynamodb.Client
	orders string
	reg    *hydrate.Registry
}

func NewService(c *dynamodb.Client, edgeTable, orderTable string) *Service {
	reg := &hydrate.Registry{}
	reg.Register("order-", orderTable, func(id string) map[string]types.AttributeValue {
		return map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: id}}
	})

	return &Service{
		edges:  ddbrel.New(c, edgeTable),
		client: c,
		orders: orderTable,
		reg:    reg,
	}
}

func (s *Service) PlaceOrder(ctx context.Context, userID string, o Order, placedAt string) error {
	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.orders,
		Item: map[string]types.AttributeValue{
			"id":    &types.AttributeValueMemberS{Value: o.ID},
			"total": &types.AttributeValueMemberN{Value: fmt.Sprint(o.Total)},
		},
	})
	if err != nil {
		return fmt.Errorf("put order %s: %w", o.ID, err)
	}

	return s.edges.Add(ctx, userID, o.ID,
		ddbrel.WithLabel(labelPlaced), ddbrel.WithSort(placedAt))
}

func (s *Service) RecentOrders(ctx context.Context, userID string, n int) ([]Order, []string, error) {
	page, err := s.edges.Out(ctx, userID,
		ddbrel.WithLabel(labelPlaced), ddbrel.WithReverse(), ddbrel.WithLimit(n),
		ddbrel.WithConsistentRead())
	if err != nil {
		return nil, nil, err
	}

	res, err := hydrate.All[Order](ctx, s.client, s.reg, page.Edges)
	if err != nil {
		return nil, nil, err
	}
	if res.Items == nil {
		res.Items = []Order{}
	}
	if res.Missing == nil {
		res.Missing = []string{}
	}
	return res.Items, res.Missing, nil
}

func (s *Service) BuyerOf(ctx context.Context, orderID string) (string, error) {
	page, err := s.edges.In(ctx, orderID,
		ddbrel.WithLabel(labelPlaced), ddbrel.WithConsistentRead())
	if err != nil {
		return "", err
	}
	if len(page.Edges) == 0 {
		return "", nil
	}
	return page.Edges[0].From, nil
}

func (s *Service) Tag(ctx context.Context, orderID, tagID, at string) error {
	return s.edges.Add(ctx, orderID, tagID, ddbrel.WithLabel(labelTagged), ddbrel.WithSort(at))
}

func (s *Service) TagsOf(ctx context.Context, orderID string) ([]string, error) {
	page, err := s.edges.Out(ctx, orderID, ddbrel.WithLabel(labelTagged), ddbrel.WithConsistentRead())
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(page.Edges))
	for _, e := range page.Edges {
		ids = append(ids, e.To)
	}
	return ids, nil
}

func (s *Service) OrdersWithTag(ctx context.Context, tagID string) ([]string, error) {
	page, err := s.edges.In(ctx, tagID, ddbrel.WithLabel(labelTagged),
		ddbrel.WithReverse(), ddbrel.WithConsistentRead())
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(page.Edges))
	for _, e := range page.Edges {
		ids = append(ids, e.From)
	}
	return ids, nil
}

func (s *Service) EverythingAbout(ctx context.Context, node string) ([]ddbrel.Edge, error) {
	page, err := s.edges.Out(ctx, node, ddbrel.WithConsistentRead())
	if err != nil {
		return nil, err
	}
	return page.Edges, nil
}

func (s *Service) DeleteOrderRow(ctx context.Context, orderID string) error {
	_, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &s.orders,
		Key:       map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: orderID}},
	})
	return err
}

func (s *Service) CloseAccount(ctx context.Context, userID string) (int, error) {
	return s.edges.RemoveAll(ctx, userID)
}
