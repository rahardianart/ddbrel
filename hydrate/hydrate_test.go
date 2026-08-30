package hydrate

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/rahardianart/ddbrel"
)

func stringKey(name string) KeyFunc {
	return func(id string) map[string]types.AttributeValue {
		return map[string]types.AttributeValue{name: &types.AttributeValueMemberS{Value: id}}
	}
}

func TestRegistryLookup(t *testing.T) {
	t.Parallel()

	var r Registry
	r.Register("ORDER:", "orders", stringKey("ID"))
	r.Register("USER:", "users", stringKey("ID"))
	r.Register("USER:ADMIN:", "admins", stringKey("ID"))

	tests := []struct {
		name, id, wantTable string
		wantFound           bool
	}{
		{name: "exact type", id: "ORDER:5", wantTable: "orders", wantFound: true},
		{name: "longest prefix wins", id: "USER:ADMIN:1", wantTable: "admins", wantFound: true},
		{name: "shorter prefix still matches", id: "USER:1", wantTable: "users", wantFound: true},
		{name: "unregistered", id: "SESSION:9"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, ok := r.lookup(tt.id)
			if ok != tt.wantFound {
				t.Fatalf("lookup(%q) found = %v, want %v", tt.id, ok, tt.wantFound)
			}
			if ok && e.table != tt.wantTable {
				t.Errorf("lookup(%q) table = %q, want %q", tt.id, e.table, tt.wantTable)
			}
		})
	}
}

func TestRegistryRegisterReplaces(t *testing.T) {
	t.Parallel()

	var r Registry
	r.Register("ORDER:", "orders", stringKey("ID"))
	r.Register("ORDER:", "orders_v2", stringKey("ID"))

	if len(r.entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(r.entries))
	}
	if e, _ := r.lookup("ORDER:5"); e.table != "orders_v2" {
		t.Fatalf("table = %q, want orders_v2", e.table)
	}
}

func TestPlan(t *testing.T) {
	t.Parallel()

	var r Registry
	r.Register("ORDER:", "orders", stringKey("ID"))

	edges := []ddbrel.Edge{
		{From: "USER:1", To: "ORDER:5"},
		{From: "USER:2", To: "ORDER:5"},
		{From: "USER:1", To: "ORDER:6"},
	}

	targets, err := plan(&r, edges)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if len(targets) != 2 {
		t.Fatalf("targets = %d, want 2 (deduplicated)", len(targets))
	}
	if targets[0].id != "ORDER:5" || targets[1].id != "ORDER:6" {
		t.Fatalf("targets out of edge order: %q, %q", targets[0].id, targets[1].id)
	}
	if targets[0].sig == targets[1].sig {
		t.Error("distinct targets share a key signature")
	}
}

func TestPlanRejectsUnregisteredType(t *testing.T) {
	t.Parallel()

	var r Registry
	r.Register("ORDER:", "orders", stringKey("ID"))

	if _, err := plan(&r, []ddbrel.Edge{{From: "USER:1", To: "SESSION:9"}}); err == nil {
		t.Fatal("plan accepted an unregistered node type")
	}
}

func TestKeySignature(t *testing.T) {
	t.Parallel()

	composite := func(pk, sk string) map[string]types.AttributeValue {
		return map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: pk},
			"SK": &types.AttributeValueMemberN{Value: sk},
		}
	}

	a, err := keySignature(composite("o-5", "1"))
	if err != nil {
		t.Fatalf("keySignature: %v", err)
	}
	b, err := keySignature(composite("o-5", "1"))
	if err != nil {
		t.Fatalf("keySignature: %v", err)
	}
	if a != b {
		t.Errorf("signature is not stable: %q != %q", a, b)
	}

	c, err := keySignature(composite("o-5", "2"))
	if err != nil {
		t.Fatalf("keySignature: %v", err)
	}
	if a == c {
		t.Errorf("signature collides across keys: %q", a)
	}

	// A returned item carries the whole entity; only its key attributes may
	// contribute to the signature that matches it back to a target.
	item := composite("o-5", "1")
	item["Total"] = &types.AttributeValueMemberN{Value: "99"}
	d, err := keySignature(project(item, keyNames(composite("o-5", "1"))))
	if err != nil {
		t.Fatalf("keySignature: %v", err)
	}
	if d != a {
		t.Errorf("projected signature = %q, want %q", d, a)
	}
}

func TestKeySignatureRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		key  map[string]types.AttributeValue
	}{
		{"empty", nil},
		{"non scalar", map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberBOOL{Value: true},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if _, err := keySignature(tt.key); err == nil {
				t.Fatal("keySignature accepted an invalid key")
			}
		})
	}
}

func TestChunk(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		n        int
		size     int
		wantLens []int
	}{
		{"empty", 0, maxBatchKeys, nil},
		{"under the cap", 7, maxBatchKeys, []int{7}},
		{"exactly the cap", 100, maxBatchKeys, []int{100}},
		{"over the cap", 250, maxBatchKeys, []int{100, 100, 50}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			targets := make([]target, tt.n)
			got := chunk(targets, tt.size)
			if len(got) != len(tt.wantLens) {
				t.Fatalf("chunks = %d, want %d", len(got), len(tt.wantLens))
			}
			for i, want := range tt.wantLens {
				if len(got[i]) != want {
					t.Errorf("chunk %d = %d keys, want %d", i, len(got[i]), want)
				}
			}
		})
	}
}

func TestBackoffIsBoundedAndGrows(t *testing.T) {
	t.Parallel()

	prev := time.Duration(0)
	for attempt := 0; attempt <= maxUnprocessedRetries; attempt++ {
		d := backoff(attempt)
		if d > maxBackoff {
			t.Fatalf("backoff(%d) = %v, over the cap %v", attempt, d, maxBackoff)
		}
		if d < prev {
			t.Fatalf("backoff(%d) = %v, shrank from %v", attempt, d, prev)
		}
		prev = d
	}
}

func TestCountKeys(t *testing.T) {
	t.Parallel()

	req := map[string]types.KeysAndAttributes{
		"orders": {Keys: []map[string]types.AttributeValue{{}, {}}},
		"users":  {Keys: []map[string]types.AttributeValue{{}}},
		"empty":  {},
	}
	if got := countKeys(req); got != 3 {
		t.Fatalf("countKeys = %d, want 3", got)
	}
	if got := countKeys(nil); got != 0 {
		t.Fatalf("countKeys(nil) = %d, want 0", got)
	}
}

func TestCanonicalNumber(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{in: "1", want: "1"},
		{in: "1.00", want: "1"},
		{in: "0003", want: "3"},
		{in: "2e2", want: "200"},
		{in: "2E2", want: "200"},
		{in: "1e-2", want: "0.01"},
		{in: "-0.500", want: "-0.5"},
		{in: "+7", want: "7"},
		{in: "0", want: "0"},
		{in: "0.0", want: "0"},
		{in: "-0", want: "0"},
		{in: "-0.000", want: "0"},
		{in: "0003.10", want: "3.1"},
		{in: "1.5e3", want: "1500"},
		{in: "123.456", want: "123.456"},
		{in: ".5", want: "0.5"},
		{in: "-1e-3", want: "-0.001"},
		// 38 significant digits must survive intact; a float64 would round here.
		{in: "12345678901234567890123456789012345678", want: "12345678901234567890123456789012345678"},
		{in: "0.10000000000000000000000000000000000001", want: "0.10000000000000000000000000000000000001"},
		{in: "", wantErr: true},
		{in: "abc", wantErr: true},
		{in: "1e", wantErr: true},
		{in: "1.2.3", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := canonicalNumber(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("canonicalNumber(%q) = %q, want error", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("canonicalNumber(%q): %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("canonicalNumber(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestKeySignatureMatchesAcrossNumberForms(t *testing.T) {
	t.Parallel()

	// The caller's KeyFunc and the key DynamoDB echoes back must sign identically
	// even when written in different but equal forms.
	for _, pair := range [][2]string{
		{"1.00", "1"},
		{"2e2", "200"},
		{"0003", "3"},
		{"-0.500", "-0.5"},
	} {
		a, err := keySignature(map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberN{Value: pair[0]},
		})
		if err != nil {
			t.Fatalf("keySignature(%q): %v", pair[0], err)
		}
		b, err := keySignature(map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberN{Value: pair[1]},
		})
		if err != nil {
			t.Fatalf("keySignature(%q): %v", pair[1], err)
		}
		if a != b {
			t.Errorf("signatures differ for equal numbers %q and %q: %q vs %q", pair[0], pair[1], a, b)
		}
	}
}
