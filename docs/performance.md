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

The pointer item removed the fan-in cliff. `Add` resolves an edge with one
`GetItem` on a key determined by `(from, label, to)`, instead of scanning the
target's incoming range.

32 concurrent workers, 5 seconds each, against a node with 40,000 edges:

| Operation | Before | After |
|---|---|---|
| `Add` (resolve path) | **2 ops/s** | **463 ops/s** |
| `AddExact` | 1008 ops/s | 603 ops/s |
| `In(WithLimit(20))` | 836 ops/s | 843 ops/s |

`Add` improved about 230x. `AddExact` slowed by a third, which is the expected
cost of a third item in its transaction — it went from two writes to three.
Bounded reads are unchanged; they never touched the resolve path.

## Cost of a single Add, by target fan-in

| Fan-in | Before: Query calls | Before: latency | After: latency |
|---|---|---|---|
| 1,000 | 1 | 31 ms | 4 ms |
| 5,000 | 1 | 96 ms | 4 ms |
| 10,000 | 1 | 153 ms | 4 ms |
| 20,000 | 2 | 313 ms | 4 ms |
| 40,000 | 3 | 719 ms | 4 ms |

**Flat.** The resolve is now a `GetItem`, so it issues no `Query` at all and its
cost does not depend on how many edges the target has.

### What the old numbers meant

Worth keeping, because the mechanism was widely mis-stated — including by me.

Below about 10,000 edges the old resolve was **one** Query call, not N+1:
DynamoDB returns up to 1 MB per page and small edge items fit well past 1,000
rows. Pagination began only past 1 MB. It was one round trip reading an unbounded
amount, and latency grew linearly with data scanned rather than with requests
issued. The fix therefore had to reduce *bytes read*, which is what a
key-addressable pointer does.

The old resolve also forced `ConsistentRead`, doubling its read units. At 40,000
edges that was roughly 1,500 RCU per `Add` against a ~3,000 RCU/s partition
ceiling — about two `Add`s per second before throttling on real DynamoDB. The new
resolve is a single strongly consistent `GetItem`: **1 RCU**, whatever the fan-in.

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

**Fan-in no longer affects write cost.** `Add` against a tag with a million posts
costs the same as against an empty one. Choosing edge direction is back to being a
modelling decision rather than a performance one.

**`AddExact` and `RemoveExact` are still faster**, at 603 versus 463 ops/s — they
skip the read and the condition entirely. Use them in imports and backfills, where
you constructed the edge and no concurrent writer is contending for it.

**`Has` is one read unit.** Authorization checks — "is this user a member?" — no
longer need a query or a known sort value.

## What it cost

Three items per edge instead of two: roughly 1.4x storage, and about 6x the write
units of a plain `PutItem` instead of 4x, since `TransactWriteItems` bills at 2x
per item. `RemoveAll` batches 33 edges per transaction instead of 50.

That is the whole bill. It bought a 230x improvement on `Add`, closed the
concurrent-write race that duplicated edges, and made `Has` a single read unit.

## Reproducing

The harness is not in the repository. It:

1. Creates an edge table and seeds one node to a target fan-in with `AddExact`.
2. Counts API calls per operation with a smithy `Initialize` middleware.
3. Times a single `Add` at each fan-in level, reporting calls and latency.
4. Runs 32 goroutines for 5 seconds per operation, recording ops/s and errors.

Numbers vary with host and emulator; the *ratios* between operations are the
finding, not the absolute figures.
