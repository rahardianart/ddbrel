package ddbrel

import (
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func cancelled(codes ...string) error {
	reasons := make([]types.CancellationReason, 0, len(codes))
	for _, c := range codes {
		reasons = append(reasons, types.CancellationReason{
			Code:    aws.String(c),
			Message: aws.String("reason " + c),
		})
	}
	return &types.TransactionCanceledException{
		Message:             aws.String("Transaction cancelled"),
		CancellationReasons: reasons,
	}
}

func TestUnwrapTransaction(t *testing.T) {
	t.Parallel()

	names := []string{"forward item put", "inverse item put"}

	tests := []struct {
		name        string
		err         error
		names       []string
		wantTyped   bool
		wantReasons []CancellationReason
	}{
		{
			name:  "not a cancellation",
			err:   errors.New("dial tcp: connection refused"),
			names: names,
		},
		{
			name:      "inverse item throttled",
			err:       cancelled(reasonNone, "ThrottlingError"),
			names:     names,
			wantTyped: true,
			wantReasons: []CancellationReason{
				{Item: "inverse item put", Code: "ThrottlingError", Message: "reason ThrottlingError"},
			},
		},
		{
			name:      "both items",
			err:       cancelled("ConditionalCheckFailed", "ThrottlingError"),
			names:     names,
			wantTyped: true,
			wantReasons: []CancellationReason{
				{Item: "forward item put", Code: "ConditionalCheckFailed", Message: "reason ConditionalCheckFailed"},
				{Item: "inverse item put", Code: "ThrottlingError", Message: "reason ThrottlingError"},
			},
		},
		{
			name:      "more reasons than named items",
			err:       cancelled(reasonNone, reasonNone, "ThrottlingError"),
			names:     names,
			wantTyped: true,
			wantReasons: []CancellationReason{
				{Item: "item 2", Code: "ThrottlingError", Message: "reason ThrottlingError"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := unwrapTransaction(tt.err, tt.names)

			var te *TransactionError
			if !errors.As(got, &te) {
				if tt.wantTyped {
					t.Fatalf("unwrapTransaction = %v, want *TransactionError", got)
				}
				if !errors.Is(got, tt.err) {
					t.Fatalf("unwrapTransaction = %v, want the original error", got)
				}
				return
			}
			if !tt.wantTyped {
				t.Fatalf("unwrapTransaction wrapped %v unexpectedly", tt.err)
			}
			if len(te.Reasons) != len(tt.wantReasons) {
				t.Fatalf("reasons = %+v, want %+v", te.Reasons, tt.wantReasons)
			}
			for i, want := range tt.wantReasons {
				if te.Reasons[i] != want {
					t.Errorf("reason %d = %+v, want %+v", i, te.Reasons[i], want)
				}
			}
			if !errors.Is(te.Unwrap(), tt.err) {
				t.Errorf("Unwrap = %v, want the original error", te.Unwrap())
			}
			for _, want := range tt.wantReasons {
				if !strings.Contains(te.Error(), want.Item) {
					t.Errorf("Error() = %q, missing %q", te.Error(), want.Item)
				}
			}
		})
	}
}
