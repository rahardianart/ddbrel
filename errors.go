package ddbrel

import (
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var (
	// ErrInvalidID reports a node ID, label or sort value that cannot be stored:
	// it is empty where a value is required, or it contains the key delimiter '#'.
	ErrInvalidID = errors.New("ddbrel: invalid id")

	// ErrCursorMismatch reports a cursor handed to a query whose key range differs
	// from the one the cursor was produced by. Resuming would silently return
	// edges from a different node, direction, label or sort range.
	ErrCursorMismatch = errors.New("ddbrel: cursor does not match query")
)

// CancellationReason is one entry of a cancelled transaction, mapped back to the
// edge item it belongs to.
type CancellationReason struct {
	// Item names the write the reason belongs to, such as "forward item put".
	Item    string
	Code    string
	Message string
}

// TransactionError reports a cancelled TransactWriteItems. The SDK returns
// cancellation reasons as a slice parallel to the transact items, which is only
// interpretable against the items that were submitted; this maps them back.
type TransactionError struct {
	Reasons []CancellationReason
	Err     error
}

func (e *TransactionError) Error() string {
	if len(e.Reasons) == 0 {
		return fmt.Sprintf("ddbrel: transaction cancelled: %v", e.Err)
	}
	parts := make([]string, 0, len(e.Reasons))
	for _, r := range e.Reasons {
		parts = append(parts, fmt.Sprintf("%s: %s", r.Item, r.Code))
	}
	return "ddbrel: transaction cancelled: " + strings.Join(parts, "; ")
}

func (e *TransactionError) Unwrap() error { return e.Err }

const reasonNone = "None"

func unwrapTransaction(err error, items []string) error {
	var cancelled *types.TransactionCanceledException
	if !errors.As(err, &cancelled) {
		return err
	}

	te := &TransactionError{Err: err}
	for i, r := range cancelled.CancellationReasons {
		code := aws.ToString(r.Code)
		if code == "" || code == reasonNone {
			continue
		}
		item := fmt.Sprintf("item %d", i)
		if i < len(items) {
			item = items[i]
		}
		te.Reasons = append(te.Reasons, CancellationReason{
			Item:    item,
			Code:    code,
			Message: aws.ToString(r.Message),
		})
	}
	return te
}

func invalidID(what, value string, reason string) error {
	return fmt.Errorf("ddbrel: %s %q %s: %w", what, value, reason, ErrInvalidID)
}
