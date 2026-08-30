# ddbrel

Directed, labelled relationships between entities in DynamoDB.

> **Status: early.** The API is not stable and there is a known performance
> problem in `Add`/`Remove` — see [Known limits](#known-limits) before using this
> for anything real.

## Why

DynamoDB has no joins. Relating two entities is not a feature you switch on; it
is a key design you commit to up front. Every project rebuilds the same edge
handling by hand and gets the same details wrong: duplicate edges, broken
pagination, dangling references after deletes.

`ddbrel` packages that work once, in a dedicated edge table. **Your entity tables
are never touched.**

It deliberately does *not* provide joins, filtering on far-side attributes, or
multi-hop traversal. Those need a query planner over a database that intentionally
has none, and every library that has tried degrades into N+1 round trips or scans.
What you get is correct edge management with good ergonomics.

## Install

```sh
go get github.com/rahardianart/ddbrel
```

The edge table needs a string partition key `PK`, a string sort key `SK`, and **no
secondary index**.

## Quick start

```go
store := ddbrel.New(client, "edges")

// user-1 placed order-5
err := store.Add(ctx, "user-1", "order-5",
    ddbrel.WithLabel("PLACED"),
    ddbrel.WithSort("20260830T120400Z"))

// that user's 20 newest orders
page, err := store.Out(ctx, "user-1",
    ddbrel.WithLabel("PLACED"), ddbrel.WithReverse(), ddbrel.WithLimit(20))

// who placed the order — same edge, no second write
page, err = store.In(ctx, "order-5", ddbrel.WithLabel("PLACED"))
```

Node IDs are opaque strings. **`#` is the key delimiter and is rejected**, so use
`user-1`, not `USER#1`.

## How it works

Each edge is written as **two items in one `TransactWriteItems`**:

| PK | SK |
|---|---|
| `user-1` | `OUT#PLACED#20260830T120400Z#order-5` |
| `order-5` | `IN#PLACED#20260830T120400Z#user-1` |

```
SK = <DIR>#<LABEL>#<sort>#<node-id>      DIR is OUT or IN
```

Both directions live in the base table, so **both can be read with
`ConsistentRead`** — write an edge and read it back immediately from either side.
An inverted GSI cannot do this: GSIs only ever support eventually consistent reads.
The cost is roughly 2x storage and 4x the write units of a plain `PutItem`.

Direction is a key prefix rather than an attribute because a node's partition holds
a mix of its own outgoing edges and the inverse items of edges pointing at it, and
attributes cannot be range-queried.

The `<sort>` component is **opaque** — the library only compares it as a string.
If you rely on ordering, use a format whose lexicographic order is the intended
order: fixed width, built from characters above `#`. `20260830T120400Z` works;
unpadded numbers do not.

## API

| Call | Round trips | Notes |
|---|---|---|
| `Out` / `In` | 1 per page | `Query` on the base table |
| `Add` | 2 | Upserts on `(from, label, to)` |
| `Remove` | 2 | Idempotent |
| `AddExact` / `RemoveExact` | 1 | You supply the sort value |
| `RemoveAll` | 1 + ⌈edges/50⌉ | Not atomic; idempotent and resumable |

`Add` and `Remove` resolve the stored sort value first, so re-adding an edge with a
new sort value *moves* it rather than duplicating it. The `Exact` variants skip
that lookup and do not detect an existing copy under a different sort value.

Options apply at four different points:

| Kind | Options | Applies to |
|---|---|---|
| `Option` | `WithKeyNames` | `New` |
| `EdgeOption` | `WithLabel` | reads *and* writes |
| `WriteOption` | `WithSort`, `WithAttrs` | `Add`, `Remove`, `RemoveAll`, `*Exact` |
| `QueryOption` | `WithSortRange`, `WithLimit`, `WithReverse`, `WithCursor`, `WithConsistentRead` | `Out`, `In` |

Omitting `WithLabel` on a read matches **every** label in that direction.
`WithLabel("")` is different — it narrows to edges written without a label.

Errors: `ErrInvalidID`, `ErrCursorMismatch`, and `TransactionError`, which unwraps
DynamoDB's positional `CancellationReasons` into named causes.

## Hydration

The core returns edges and knows nothing about your entity tables. `ddbrel/hydrate`
is opt-in:

```go
reg := &hydrate.Registry{}
reg.Register("order-", "orders", func(id string) map[string]types.AttributeValue {
    return map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: id}}
})

res, err := hydrate.All[Order](ctx, client, reg, page.Edges)
// res.Items   — loaded entities
// res.Missing — edges whose target no longer exists
```

It handles the `BatchGetItem` 100-key cap and retries `UnprocessedKeys` with
backoff. Because IDs carry no `#`, `Register` matches its type as a **prefix** of
the node ID, longest match winning.

Nothing enforces referential integrity, so edges outlive deleted entities.
`Missing` reports them as data rather than silently shrinking the result;
`hydrate.Sweep` turns that list into deletes.

## Testing

Unit tests need no Docker and no AWS:

```sh
go test ./...
```

Integration tests run against LocalStack or DynamoDB Local, interchangeably:

```sh
docker compose up -d --wait
DDB_ENDPOINT=http://localhost:4566 go test -tags=integration -count=1 ./...
```

Full setup, test wiring, and what the emulators **cannot** verify:
[`docs/testing.md`](docs/testing.md).

## Example

[`example/orders`](example/orders) is a working service — one-to-many, reverse
lookups, many-to-many, hydration, and dangling-edge reporting. Run the scenario:

```sh
DDB_ENDPOINT=http://localhost:4566 go run ./example/orders
```

Or serve it over HTTP:

```sh
DDB_ENDPOINT=http://localhost:4566 go run ./example/orders -serve :8080
curl "localhost:8080/users/user-1/orders?limit=20"
```

## Known limits

Documented rather than solved.

- **`Add` and `Remove` are O(fan-in of the target).** The resolve step paginates
  the target's incoming-label range. Fine for one-to-many; **bad for many-to-many
  with a hot target** — adding a post to a tag with a million posts paginates all
  of them. Use `AddExact`/`RemoveExact` where you already know the sort value.
  A pointer-item design that makes this O(1) is agreed but not implemented.
- **Cardinality is not enforced.** Nothing prevents a second edge where the domain
  wants one-to-one.
- **`RemoveAll` is not atomic.** Inverse items live in other partitions, so it is a
  query plus batched transactional deletes — idempotent and resumable instead.
- **Ordering is nested under label.** A query spanning labels interleaves by label
  first, so global chronological ordering across labels is not one query.
- **Hot partitions.** A node with millions of edges concentrates load. Write
  sharding is out of scope.

## Design

The full record, including the alternatives that were rejected and why:
[`docs/superpowers/specs/2026-08-30-ddb-relationship-library-design.md`](docs/superpowers/specs/2026-08-30-ddb-relationship-library-design.md).
