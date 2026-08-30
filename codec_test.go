package ddbrel

import (
	"errors"
	"math/rand/v2"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestSortKeyRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                   string
		dir, label, sort, node string
		wantSK                 string
	}{
		{"labelled with sort", dirOut, "PLACED", "20260830T1204Z", "order-5", "OUT#PLACED#20260830T1204Z#order-5"},
		{"inverse", dirIn, "PLACED", "20260830T1204Z", "user-1", "IN#PLACED#20260830T1204Z#user-1"},
		{"empty label", dirOut, "", "20260830T1204Z", "order-5", "OUT##20260830T1204Z#order-5"},
		{"empty sort", dirOut, "ROLE", "", "role-admin", "OUT#ROLE##role-admin"},
		{"empty label and sort", dirIn, "", "", "n", "IN###n"},
		{"unicode id", dirOut, "SUIVI", "2026", "Ω-café-🙂", "OUT#SUIVI#2026#Ω-café-🙂"},
		{"unicode label", dirOut, "ПОДПИСКА", "", "n", "OUT#ПОДПИСКА##n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sk := sortKey(tt.dir, tt.label, tt.sort, tt.node)
			if sk != tt.wantSK {
				t.Fatalf("sortKey = %q, want %q", sk, tt.wantSK)
			}
			if !strings.HasPrefix(sk, prefixFor(tt.dir, tt.label)) {
				t.Errorf("sort key %q does not carry prefix %q", sk, prefixFor(tt.dir, tt.label))
			}

			dir, label, srt, node, err := parseSortKey(sk)
			if err != nil {
				t.Fatalf("parseSortKey(%q): %v", sk, err)
			}
			if dir != tt.dir || label != tt.label || srt != tt.sort || node != tt.node {
				t.Errorf("parseSortKey = (%q, %q, %q, %q), want (%q, %q, %q, %q)",
					dir, label, srt, node, tt.dir, tt.label, tt.sort, tt.node)
			}
		})
	}
}

func TestParseSortKeyRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, sk string
	}{
		{"too few parts", "OUT#PLACED#order-5"},
		{"too many parts", "OUT#PLACED#2026#order#5"},
		{"unknown direction", "SIDE#PLACED#2026#order-5"},
		{"empty", ""},
		{"no delimiters", "OUT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if _, _, _, _, err := parseSortKey(tt.sk); err == nil {
				t.Fatalf("parseSortKey(%q) = nil error, want error", tt.sk)
			}
		})
	}
}

func TestValidateEdge(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		from, to, label, sort string
		wantErr               bool
	}{
		{name: "plain", from: "user-1", to: "order-5", label: "PLACED", sort: "2026"},
		{name: "empty label and sort", from: "user-1", to: "order-5"},
		{name: "unicode", from: "utilisateur-café", to: "commande-🙂", label: "PASSÉ", sort: "2026"},
		{name: "delimiter in from", from: "USER#1", to: "order-5", wantErr: true},
		{name: "delimiter in to", from: "user-1", to: "ORDER#5", wantErr: true},
		{name: "delimiter in label", from: "user-1", to: "order-5", label: "PLA#CED", wantErr: true},
		{name: "delimiter in sort", from: "user-1", to: "order-5", sort: "2026#08", wantErr: true},
		{name: "injected direction prefix", from: "user-1", to: "x#IN#PLACED#2026#victim", wantErr: true},
		{name: "empty from", from: "", to: "order-5", wantErr: true},
		{name: "empty to", from: "user-1", to: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateEdge(tt.from, tt.to, tt.label, tt.sort)
			if tt.wantErr {
				if !errors.Is(err, ErrInvalidID) {
					t.Fatalf("validateEdge = %v, want ErrInvalidID", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateEdge = %v, want nil", err)
			}
		})
	}
}

// TestSortKeyOrder is the property the whole sort component exists for: the
// lexicographic order of sort keys must be the order the caller intended, which
// is (sort value, node ID). It holds for any sort value built from characters
// above the '#' delimiter, including values of different lengths.
func TestSortKeyOrder(t *testing.T) {
	t.Parallel()

	const alphabet = "0123456789:.-TZabxyzΩ"
	r := rand.New(rand.NewPCG(0x5eed, 0x1eaf))

	runes := []rune(alphabet)
	pick := func(maxLen int) string {
		n := 1 + r.IntN(maxLen)
		var b strings.Builder
		for i := 0; i < n; i++ {
			b.WriteRune(runes[r.IntN(len(runes))])
		}
		return b.String()
	}

	type pair struct{ sort, node string }
	pairs := make([]pair, 0, 500)
	for i := 0; i < 500; i++ {
		pairs = append(pairs, pair{sort: pick(12), node: pick(8)})
	}

	byKey := append([]pair(nil), pairs...)
	sort.SliceStable(byKey, func(i, j int) bool {
		return sortKey(dirOut, "L", byKey[i].sort, byKey[i].node) <
			sortKey(dirOut, "L", byKey[j].sort, byKey[j].node)
	})

	byIntent := append([]pair(nil), pairs...)
	sort.SliceStable(byIntent, func(i, j int) bool {
		if byIntent[i].sort != byIntent[j].sort {
			return byIntent[i].sort < byIntent[j].sort
		}
		return byIntent[i].node < byIntent[j].node
	})

	if !reflect.DeepEqual(byKey, byIntent) {
		for i := range byKey {
			if byKey[i] != byIntent[i] {
				t.Fatalf("order diverges at %d: key order %+v, intended %+v", i, byKey[i], byIntent[i])
			}
		}
	}
}

func TestSortKeyOrderIsChronological(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		chronological []string
		wantOrdered   bool
	}{
		{"fixed width timestamps", []string{
			"20260830T090000Z", "20260830T100000Z", "20260830T235959Z", "20260901T000000Z",
		}, true},
		{"zero padded counters", []string{"0000000009", "0000000010", "0000000100"}, true},
		// The trap the sort value format has to avoid: unpadded numbers stop
		// sorting chronologically the moment they cross a digit width boundary.
		{"unpadded counters", []string{"9", "10"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			keys := make([]string, len(tt.chronological))
			for i, s := range tt.chronological {
				keys[i] = sortKey(dirOut, "PLACED", s, "n")
			}
			sorted := append([]string(nil), keys...)
			sort.Strings(sorted)

			if ordered := reflect.DeepEqual(keys, sorted); ordered != tt.wantOrdered {
				t.Fatalf("lexicographic order matches chronological = %v, want %v (keys %v)",
					ordered, tt.wantOrdered, keys)
			}
		})
	}
}

func TestUpperBound(t *testing.T) {
	t.Parallel()

	prefix := prefixFor(dirOut, "PLACED")
	bound := upperBound(prefix)

	if bound <= prefix {
		t.Fatalf("upperBound(%q) = %q, want greater", prefix, bound)
	}
	for _, sk := range []string{
		sortKey(dirOut, "PLACED", "", "a"),
		sortKey(dirOut, "PLACED", "zzzz", "zzzz"),
		sortKey(dirOut, "PLACED", "9", "🙂"),
	} {
		if sk >= bound {
			t.Errorf("%q >= upper bound %q", sk, bound)
		}
	}
	for _, sk := range []string{
		sortKey(dirOut, "PLACED2", "0", "a"),
		sortKey(dirOut, "OTHER", "0", "a"),
		sortKey(dirIn, "PLACED", "0", "a"),
	} {
		if sk >= prefix && sk < bound {
			t.Errorf("%q falls inside [%q, %q)", sk, prefix, bound)
		}
	}
}

func TestCodecItemRoundTrip(t *testing.T) {
	t.Parallel()

	c := defaultCodec()
	attrs := map[string]types.AttributeValue{"Note": &types.AttributeValueMemberS{Value: "n"}}

	tests := []struct {
		name string
		dir  string
		pk   string
		node string
		want Edge
	}{
		{
			name: "forward",
			dir:  dirOut,
			pk:   "user-1",
			node: "order-5",
			want: Edge{From: "user-1", To: "order-5", Label: "PLACED", Sort: "2026", Attrs: attrs},
		},
		{
			name: "inverse",
			dir:  dirIn,
			pk:   "order-5",
			node: "user-1",
			want: Edge{From: "user-1", To: "order-5", Label: "PLACED", Sort: "2026", Attrs: attrs},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sk := sortKey(tt.dir, "PLACED", "2026", tt.node)
			item, err := c.item(tt.pk, sk, "PLACED", tt.node, "2026", attrs)
			if err != nil {
				t.Fatalf("item: %v", err)
			}

			got, err := c.edge(item)
			if err != nil {
				t.Fatalf("edge: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("edge = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestCodecItemRejectsReservedAttrs(t *testing.T) {
	t.Parallel()

	c := defaultCodec()
	for _, name := range []string{"PK", "SK", attrLabel, attrNode, attrSort} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			attrs := map[string]types.AttributeValue{name: &types.AttributeValueMemberS{Value: "x"}}
			if _, err := c.item("a", sortKey(dirOut, "L", "", "b"), "L", "b", "", attrs); err == nil {
				t.Fatalf("item accepted reserved attribute %q", name)
			}
		})
	}
}

func TestCodecEdgeRejectsMalformedItems(t *testing.T) {
	t.Parallel()

	c := defaultCodec()
	tests := []struct {
		name string
		item map[string]types.AttributeValue
	}{
		{"missing pk", map[string]types.AttributeValue{"SK": &types.AttributeValueMemberS{Value: "OUT#L##b"}}},
		{"missing sk", map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "a"}}},
		{"pk not a string", map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberN{Value: "1"},
			"SK": &types.AttributeValueMemberS{Value: "OUT#L##b"},
		}},
		{"malformed sk", map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "a"},
			"SK": &types.AttributeValueMemberS{Value: "OUT#L#b"},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if _, err := c.edge(tt.item); err == nil {
				t.Fatalf("edge accepted %v", tt.item)
			}
		})
	}
}
