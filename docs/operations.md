# Operations

What fails in production, how it surfaces, and what to do about it.

## Conflict retries

`Add` and `Remove` are read-then-write: they read the edge's pointer item, then
write under a condition on that pointer. If another writer moves the same edge in
between, the condition fails and the transaction is cancelled.

The library absorbs this. A losing writer re-reads the pointer and retries, up to
**5 attempts**. Only if all five lose does the call return `ErrConflict`:

```go
if errors.Is(err, ddbrel.ErrConflict) {
    // several writers are actively contending for this exact edge
}
```

### What it means when you see it

`ErrConflict` is not a transient network problem. It means **five separate
writers won the race against this one within a few milliseconds**, all targeting
the same `(from, label, to)`. Retrying is safe — every operation underneath is
idempotent — but blind retrying is treating a symptom.

The usual causes, in order of likelihood:

- **A hot edge.** Many workers updating one relationship's sort value: a
  leaderboard rank, a last-seen timestamp, a counter-shaped edge. Sort values are
  part of the key, so every update is a move, and moves serialise.
- **A retry storm.** An upstream retry loop amplifying one user action into many
  concurrent `Add` calls for the same edge.
- **Genuinely concurrent business events** that happen to touch one edge.

### What to do

**If you know the sort value, use `AddExact` or `RemoveExact`.** They skip both
the read and the condition, so they never conflict. This is the right answer for
imports, backfills, and any path that constructed the edge it is writing.

**If the edge's sort value changes constantly, it is probably not a sort value.**
Move the volatile field into `WithAttrs`, where updating it is an unconditional
overwrite of the same key rather than a move. Keep in the sort value only what you
actually order or range-query by.

**Do not wrap `Add` in your own retry loop** without backoff. The library already
retried five times immediately; more attempts at the same rate will not help and
will add load to a partition that is already contended.

`ErrConflict` is not a throttle. Never treat it as a capacity signal.

## Throttling

DynamoDB rejects work when a partition or table exceeds its limits. This surfaces
as a cancelled transaction, which the library unwraps into `TransactionError`:

```go
var te *ddbrel.TransactionError
if errors.As(err, &te) {
    for _, r := range te.Reasons {
        log.Printf("%s: %s", r.Item, r.Code) // e.g. "inverse item put: ThrottlingError"
    }
}
```

Each reason names the item it belongs to, because DynamoDB returns
`CancellationReasons` as a bare positional array that means nothing without the
transact items it lines up against.

### What costs what

| Operation | Reads | Writes |
|---|---|---|
| `Out` / `In` | 1 query, cost tracks rows **returned** | — |
| `Has` | 1 strongly consistent `GetItem`, 1 RCU | — |
| `Add` / `Remove` | 1 strongly consistent `GetItem`, 1 RCU | 3 items in a transaction (5 when moving) |
| `AddExact` / `RemoveExact` | — | 3 items in a transaction |
| `RemoveAll` | 2 queries over the node's edges | 3 items per edge, 33 edges per transaction |

`TransactWriteItems` bills at **2x** the write units of a plain `PutItem`, so a
three-item edge write costs about 6 WCU-equivalents. That is the price of atomic
forward, inverse and pointer items.

### Hot partitions are the real limit

A single partition sustains roughly **3,000 RCU/s and 1,000 WCU/s**, and every
edge of one node lives in that node's partition. A node with millions of edges is
therefore a throughput ceiling regardless of table-level capacity.

This is a modelling problem, not a tuning one. If one node is hot:

- Reads: keep them bounded. `WithLimit` means cost tracks rows returned, so a
  bounded read of a 40,000-edge node costs the same as one of a 20-edge node.
- Writes: the partition limit still applies. Sharding a hot node across `node#0` …
  `node#N` is the standard answer and is **out of scope for this library** — you
  would fan out reads across shards yourself.

### On retries

Retries for throttling stay with the AWS SDK's built-in retryer. Do not add your
own layer: two retry policies stacked on one transport amplify a throttling
episode rather than absorbing it.

`ClientRequestToken` is deliberately left unset so the SDK generates one per call
and reuses it across that call's retries. Do not set it from application code —
deriving it from anything stable makes DynamoDB silently discard a later,
legitimate write as a replay.

## Partial failures

`RemoveAll` is **not atomic**. Inverse items live in other partitions, so it is a
query followed by batched transactional deletes. On failure it returns how many
edges it removed:

```go
removed, err := store.RemoveAll(ctx, node)
if err != nil {
    // removed is accurate; calling again resumes
}
```

Every operation beneath it is idempotent, so retrying is always safe and never
double-counts. Do not treat a partial `RemoveAll` as a failed one — the work it
reports is done.

## What the emulators cannot tell you

Neither LocalStack nor DynamoDB Local models capacity, so **the throttling paths
in this document are unreachable locally**. A green integration suite says nothing
about how the library behaves under throttling.

The `TransactionError` unwrapping is exercised by client-side fault injection
instead. See [`testing.md`](testing.md).

Conflict retries **are** reachable locally — condition failures are real in the
emulators — and are covered by `TestConcurrentAddDoesNotDuplicate`.
