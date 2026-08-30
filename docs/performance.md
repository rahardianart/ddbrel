# Performance

Measured against LocalStack 4.14 (DynamoDB Local underneath) on an M-series Mac,
Colima VM with 4 CPU / 8 GB. API calls were counted with a smithy middleware on
the `Initialize` step, so the numbers are actual requests issued, not estimates.

> **These runs produced zero errors, and that is a limitation, not a result.**
> No emulator models throughput capacity. LocalStack and DynamoDB Local ignore
> provisioned and on-demand limits entirely, so `ProvisionedThroughputExceededException`
> is unreachable locally. What is measured here is the *work per call* that
> predicts where real DynamoDB would throttle.

## Headline

`Add` and `Remove` against a high fan-in target are roughly **400x slower** than
every other operation, and the gap widens as fan-in grows.

32 concurrent workers, 5 seconds each, against a node with 40,000 edges:

| Operation | Throughput | Errors |
|---|---|---|
| `In(WithLimit(20))` | 836 ops/s | 0 |
| `AddExact` | 1008 ops/s | 0 |
| **`Add` (resolve path)** | **2 ops/s** | 0 |

## Why: the resolve step scans the target's whole incoming range

`Add` and `Remove` must discover an edge's stored sort value before they can write
or delete it, because that value is part of the sort key. They do so by querying
the target's entire `IN#<label>#` range and filtering client-side.

Cost of a single `Add` as the target's fan-in grows:

| Fan-in | Query calls | Latency |
|---|---|---|
| 1,000 | 1 | 31 ms |
| 5,000 | 1 | 96 ms |
| 10,000 | 1 | 153 ms |
| 20,000 | 2 | 313 ms |
| 40,000 | 3 | 719 ms |

Two things to read here.

**Call count is not the cost below ~10k edges.** DynamoDB returns up to 1 MB per
page, and small edge items fit in one page well past 1,000 rows. Pagination only
begins once the range exceeds 1 MB. Anyone reasoning about this as "N+1 round
trips" has the wrong model — it is one round trip reading an unbounded amount.

**Latency grows linearly throughout**, because the work is data scanned rather
than requests issued.

### What this predicts on real DynamoDB

The resolve query forces `ConsistentRead: true`, which doubles the read units of
a scan. At 40,000 edges the range is roughly 6 MB, or about 1,500 RCU per `Add`.
A single partition sustains about 3,000 RCU/s.

That is **roughly two `Add` calls per second before throttling** — the same order
as the 2 ops/s measured locally, though by a different mechanism. Locally it is
latency-bound; on real DynamoDB it would be capacity-bound and would surface as
`ProvisionedThroughputExceededException` inside a `TransactionCanceledException`.

## Bounded reads are flat

The same 40,000-edge node, read with a limit:

| Query | Edges | Query calls | Latency |
|---|---|---|---|
| `In(WithLimit(10))` | 10 | 1 | 3 ms |
| `In(WithLimit(50))` | 50 | 1 | 4 ms |
| `In(WithLimit(100))` | 100 | 1 | 4 ms |
| `In(WithLimit(20))` on 40k | 20 | 1 | 2 ms |

Cost tracks rows returned, not rows stored. This is the operation the key layout
was designed for and it behaves correctly.

## `RemoveAll` is bounded by edge count, as designed

Sweeping a node with 1,000 edges: 2 Query calls and 20 `TransactWriteItems`, 488 ms
total. Two items per edge against the 100-item transaction limit gives 50 edges per
transaction, so 1,000 edges is 20 transactions. That matches exactly.

It is not atomic and does not pretend to be — it is idempotent and resumable.

## Guidance

**Use `AddExact` and `RemoveExact` wherever the sort value is already known.**
They skip the resolve entirely and ran at 1008 ops/s against the same hot node.
This is the single most effective change available today, and it costs nothing
when the caller is writing an edge it just constructed.

**Reserve `Add` and `Remove` for low fan-in targets.** One-to-many is fine: an
order has one buyer, so resolving `user -> order` scans one row. Many-to-many with
a popular target — posts to a tag, items to a merchant, merchants to a city — is
where this becomes a production incident.

**Bulk and backfill paths must use the exact variants.** A loop calling `Add`
against a shared target degrades as it runs, because each iteration makes the next
one slower.

## Known fix, not yet implemented

A third item per edge with a fully deterministic key — a pointer at
`PK=<to>`, `SK=REF#<label>#<from>` holding the sort value — turns the resolve into
a single `GetItem`: one RCU, constant time, independent of fan-in.

Cost is three items per edge instead of two, roughly 1.4x storage and 6x the write
units of a plain `PutItem` instead of 4x. The pointer would also serve as a
uniqueness anchor via a condition expression, which would close the separate
"cardinality is not enforced" limit.

The migration is **additive** — no existing key changes, so it is a backfill that
writes pointers for existing edges and can run online, with a missing pointer
falling back to today's query path.

## Reproducing

The harness is not in the repository. It:

1. Creates an edge table and seeds one node to a target fan-in with `AddExact`.
2. Counts API calls per operation with a smithy `Initialize` middleware.
3. Times a single `Add` at each fan-in level, reporting calls and latency.
4. Runs 32 goroutines for 5 seconds per operation, recording ops/s and errors.

Numbers vary with host and emulator; the *ratios* between operations are the
finding, not the absolute figures.
