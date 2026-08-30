# ddbrel — A Relationship Layer for DynamoDB

**Date:** 2026-08-30
**Status:** Approved design, not yet implemented
**Module:** `github.com/rahardianart/ddbrel`

## Problem

DynamoDB has no joins. Relating two entities is not a schema feature you enable;
it is a key design you commit to up front. Every project rebuilds the same edge
handling by hand, and gets the same details wrong: duplicate edges, incorrect
pagination, dangling references after deletes.

`ddbrel` packages that work once. It manages a dedicated edge table and leaves
existing entity tables untouched.

## Scope

**In scope:** storing, querying, and removing directed labelled edges between
opaque node IDs; optional hydration of edge targets into caller structs.

**Out of scope, deliberately:** joins, filtering on far-side entity attributes,
multi-hop traversal, and any form of query planning. Those require a planner over
a database that intentionally has none, and every library that has tried degrades
into N+1 round trips or scans. The value here is ergonomics and correct edge
management, not join semantics.

## Design Decisions

Four decisions were settled during design. Each records what was rejected, because
the rejected options are the ones a future reader will be tempted to revisit.

### 1. Dedicated edge table

Entity tables are never modified. One separate table holds only edges.

- **Cost accepted:** every read that needs entity bodies is two round trips —
  query edges, then `BatchGetItem` the entities.
- **Problem inherited:** nothing enforces referential integrity, so edges outlive
  deleted entities. See *Dangling edges* below.
- **Rejected — library owns the entity key schema:** more powerful and the only
  form that gives single-round-trip reads, but it forces adopting tables to
  migrate to the library's conventions.
- **Rejected — adapt to existing tables via struct tags:** capability would vary
  per table depending on which indexes already happen to exist, so the library
  could not offer a uniform contract.

### 2. Layered read path: core returns IDs, hydration is opt-in

`ddbrel` returns edges and knows nothing about entity tables. A separate
`ddbrel/hydrate` package resolves edges into entities.

- Core is testable with no entity tables in existence.
- Hydration is where `BatchGetItem`'s sharp edges live — the 100-item cap,
  `UnprocessedKeys`, retry with backoff, the 16 MB response limit — so it is
  worth owning, but not worth forcing on callers who only need IDs.
- **Rejected — always hydrate:** requires global entity-table registration before
  the library can do anything, and charges ID-only callers for reads they do not want.
- **Rejected — IDs only, no hydrator:** leaves every consumer to rewrite the same
  batching and retry logic.

### 3. Dual items in a transaction, not an inverted GSI

Each edge is written as two items — forward and inverse — in a single
`TransactWriteItems`.

**Rationale.** DynamoDB GSIs support eventually consistent reads *only*;
`ConsistentRead` is not available on a GSI under any configuration. With an
inverted GSI, "write an edge, immediately read it from the other side" can
legitimately return nothing. In a library other code depends on, that is a
surprise that costs someone an afternoon of debugging. Storing both directions in
the base table makes strong consistency *available* on both sides.

- **Cost accepted:** ~2x storage, and `TransactWriteItems` bills at 2x WCU, so
  roughly 4x the write cost of a single plain `PutItem`. Edge items are tiny and
  are written far less often than they are read.
- **Benefit beyond consistency:** no GSI to provision, monitor, or backfill, and
  no risk of a throttled GSI applying back-pressure to base table writes.
- **Rejected — configurable per edge type:** two write paths, two failure modes,
  and it forces callers to understand DynamoDB consistency semantics to choose
  well. Migrating an edge type between modes is its own backfill problem.

### 4. Optional caller-supplied sort component in the sort key

`SK` carries an optional sort value. Omitted, edges sort by target ID.

**Rationale.** Without it, "this user's 20 most recent orders" is not expressible
as a query — you would fetch every edge and sort in memory, which is unbounded on
a hot node and *silently breaks pagination*, because `LastEvaluatedKey` follows
DynamoDB's ordering, not the caller's.

- **Rejected — type and ID only:** simplest key, but forfeits ordered queries and
  correct pagination.
- **Rejected — sort component always required:** forces a filler value onto
  relationships with no natural ordering (`USER#1 -> ROLE#admin`), and filler
  values become de-facto API surface that callers start depending on.

## Data Model

### Key format

```
PK = <node-id>
SK = <DIR>#<LABEL>#<sort>#<id>        DIR is OUT or IN
```

`LABEL` defaults to the target's type, and is given explicitly when two
relationships share endpoint types (`PLACED` and `VIEWED`, both `USER -> ORDER`).

### Direction must live in the key

A node's partition holds a mix of its own outgoing edges *and* the inverse items
of edges pointing at it. Edge `USER#1 -> ORDER#5` writes an item at `PK=USER#1`;
edge `SESSION#9 -> USER#1` also writes one at `PK=USER#1`.

Direction is therefore a sort key prefix, not an attribute — attributes cannot be
range-queried, so an attribute-based marker would require filtering after the read
and would break `Limit`.

### Item shapes

Edge `USER#1 --PLACED--> ORDER#5` at `2026-08-30T12:04Z` writes both items in one
`TransactWriteItems`:

| PK | SK | Label | Node | Sort |
|---|---|---|---|---|
| `USER#1` | `OUT#PLACED#20260830T1204Z#5` | `PLACED` | `ORDER#5` | `20260830T1204Z` |
| `ORDER#5` | `IN#PLACED#20260830T1204Z#1` | `PLACED` | `USER#1` | `20260830T1204Z` |

`Node` stores the sort-independent canonical target so callers never parse keys.

### Access patterns

| Pattern | Query |
|---|---|
| User's 20 newest orders | `PK=USER#1`, `begins_with(SK,"OUT#PLACED#")`, reverse, limit 20 |
| Who placed order 5 | `PK=ORDER#5`, `begins_with(SK,"IN#PLACED#")` |
| Orders in a date range | `PK=USER#1`, `SK BETWEEN "OUT#PLACED#<lo>" AND "OUT#PLACED#<hi>"` |

All are single `Query` calls on the base table, and all may set
`ConsistentRead: true`.

### Edge identity and the resolve-first upsert

The sort value is part of the sort key, therefore part of the edge's identity.
This has a consequence that is easy to miss: `Remove(ctx, "USER#1", "ORDER#5")`
cannot construct either sort key, because neither contains a reconstructible sort
value. The same gap makes a naive `Add` write a *second* forward item when called
again with a different timestamp — a silent duplicate edge.

**Resolution.** `Add` and `Remove` first issue a consistent `Query` against the
target's `IN#` range (`PK=ORDER#5`, `begins_with(SK,"IN#PLACED#")` — a small
partition, since it holds one order's edges rather than the user's) to discover any
existing sort value, then transact. This costs one extra round trip and makes `Add`
a true upsert on `(from, label, to)`.

`AddExact` and `RemoveExact` accept the sort value and skip the lookup, for callers
who already have it.

The alternative — declaring `(from, label, to, sort)` the identity and documenting
duplicates as the caller's problem — is cheaper but was rejected: the duplicate
surfaces as a double-charged order, not as an error.

## API

### `ddbrel` — edge store

Functional options rather than a fluent builder. A chain terminating in `.Do()`
reads well but permits a silently unexecuted query that still compiles.

```go
type Store struct{ /* client, table, codec */ }

func New(c *dynamodb.Client, table string, opts ...Option) *Store

type Edge struct {
    From, To, Label, Sort string
    Attrs map[string]types.AttributeValue
}

// Writes. Add upserts on (from, label, to); Remove is idempotent.
func (s *Store) Add(ctx context.Context, from, to string, o ...WriteOption) error
func (s *Store) Remove(ctx context.Context, from, to string, o ...WriteOption) error
func (s *Store) RemoveAll(ctx context.Context, node string, o ...WriteOption) (removed int, err error)

// Exact variants skip the resolve query — one round trip.
func (s *Store) AddExact(ctx context.Context, from, to, sort string, o ...WriteOption) error
func (s *Store) RemoveExact(ctx context.Context, from, to, sort string) error

// Reads.
func (s *Store) Out(ctx context.Context, node string, o ...QueryOption) (Page, error)
func (s *Store) In(ctx context.Context, node string, o ...QueryOption) (Page, error)

type Page struct {
    Edges []Edge
    Next  *Cursor
}
```

Query options: `WithLabel`, `WithSortRange`, `WithLimit`, `WithReverse`,
`WithCursor`, `WithConsistentRead`, `WithAttrs`.

`Out` and `In` are separate methods rather than a direction argument. Direction is
already baked into the key prefix, and two methods make call sites self-documenting.

### `ddbrel/hydrate` — opt-in entity loading

A subpackage. The core package never imports it, so the core API and its tests
stay free of any entity-table concept — the separation is in the import direction,
not in the module graph.

```go
type Registry struct{}

func (r *Registry) Register(typ, table string, key KeyFunc)

type Result[T any] struct {
    Items   []T
    Missing []string // dangling edges, surfaced rather than swallowed
}

func All[T any](ctx context.Context, c *dynamodb.Client, r *Registry, edges []ddbrel.Edge) (Result[T], error)
```

### Dangling edges

Referential integrity is not enforceable at the database level in this design, so
it is surfaced instead of hidden. `Result.Missing` reports edges whose target no
longer exists, as data rather than as a silent gap in the result set. A
`hydrate.Sweep` helper turns that list into `Remove` calls when the caller wants
cleanup.

### Key delimiter hazard

`#` is the key delimiter. An ID containing `#` corrupts the key and would let a
caller forge edges into another partition. Node IDs and labels containing `#` are
rejected at write time with `ErrInvalidID`.

Escaping was rejected: it makes keys unreadable in the AWS console and turns a
loud failure into a silent one.

## Errors and Retries

- Typed sentinels for cases callers branch on: `ErrInvalidID`, `ErrCursorMismatch`.
- `TransactionCanceledException` is unwrapped rather than passed through. The SDK
  returns it with a parallel `CancellationReasons` slice that must be indexed
  against the transact items to interpret. The library owns that mapping and
  returns a typed reason ("the inverse item write was throttled") instead of
  making every caller re-derive it.
- Retries stay with the SDK's built-in retryer. Reimplementing backoff is how a
  library ends up fighting its own transport.
- Every `TransactWriteItems` carries a `ClientRequestToken` derived from the edge
  identity, so an SDK-level retry cannot double-apply a write.
- `RemoveAll` returns `(removed int, err error)` on partial failure and is safe to
  call again, since every operation beneath it is idempotent.

## Consistency

Reads default to eventually consistent — the standard, half-price default.
`WithConsistentRead` opts in, and that option is *meaningful* here, which is the
entire point of choosing dual items over a GSI.

The one place it is not optional: the internal resolve-before-write query in `Add`
and `Remove` forces `ConsistentRead: true`. Resolving a stale sort value is exactly
how the duplicate edge that the upsert exists to prevent would be created.

## Testing

Table-driven throughout, in two tiers.

**Pure codec tests, no I/O.** Key encode/decode is the highest-risk logic and needs
none of DynamoDB to exercise: `#` injection attempts, empty and missing labels,
unicode IDs, round-trip identity. Plus a property test asserting that lexicographic
sort key order matches chronological order — a real trap, since a naive timestamp
format sorts incorrectly the moment it crosses a digit-width boundary.

**Integration against an emulator in Docker.** No cloud dependency. The endpoint
is read from `DDB_ENDPOINT`, so LocalStack and DynamoDB Local are interchangeable
and the suite names neither. Covers transaction atomicity, direction isolation,
resolve-first upsert, cursor correctness across page boundaries, `RemoveAll`
resumability, and dangling edge to `Missing` reporting.

*Caveats:* no emulator models throttling, so `TransactionCanceledException`
unwrapping is exercised only by client-side fault injection. Both emulators are a
single local store, so `WithConsistentRead` is indistinguishable from an eventually
consistent read locally — the dual-item decision rests on strong consistency being
available on a base-table query in real DynamoDB, which no emulator can confirm or
refute. `ClientRequestToken` idempotency enforcement in emulators is unverified.

Setup, Compose file, and test wiring: [`docs/testing.md`](../../testing.md).

## Accepted Limits

Documented rather than solved:

- **Hot partitions.** A node with millions of edges concentrates load on one
  partition. Write sharding is out of scope.
- **`RemoveAll` is not atomic.** Inverse items live in other partitions, so it is a
  query followed by batched transactional deletes. It is idempotent and resumable
  instead of atomic.
- **Two round trips for hydrated reads.** Inherent to leaving entity tables
  untouched, and accepted when that trade was made.
