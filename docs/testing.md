# Testing ddbrel

Two tiers, deliberately separated.

**Unit tests** cover the key codec and need no network, no Docker, and no AWS.
They run on a bare `go test ./...` and must stay that way — the codec is the
highest-risk logic in the library and its tests should never be gated behind a
container starting up.

**Integration tests** run against an emulator over a configurable endpoint. They
are behind a build tag so they never slow down the default test run.

## Endpoint is configuration, not a commitment

Tests read `DDB_ENDPOINT` and point the AWS SDK at whatever is there.

| `DDB_ENDPOINT` | Backend |
|---|---|
| `http://localhost:4566` | LocalStack |
| `http://localhost:8000` | DynamoDB Local |
| unset | integration tests skip |

Nothing in the suite names a backend. Switching between them is an environment
variable, not a code change.

## docker-compose.yml

LocalStack is the default service. DynamoDB Local sits behind a Compose profile
for when you want a faster, smaller container.

```yaml
services:
  localstack:
    image: localstack/localstack:4
    ports:
      - "4566:4566"
    environment:
      SERVICES: dynamodb
      DEBUG: "0"
      # LocalStack accepts any credentials, but the SDK requires them to exist.
      AWS_DEFAULT_REGION: us-east-1
    healthcheck:
      test:
        - CMD-SHELL
        - curl -sf http://localhost:4566/_localstack/health | grep -qE '"dynamodb": *"(available|running)"'
      interval: 5s
      timeout: 5s
      retries: 20
      start_period: 10s

  # Leaner alternative: docker compose --profile ddblocal up -d --wait
  dynamodb-local:
    image: amazon/dynamodb-local:latest
    profiles: ["ddblocal"]
    ports:
      - "8000:8000"
    command: ["-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb"]
```

Pin the LocalStack major version rather than tracking `latest`. Its DynamoDB
provider has historically been the AWS DynamoDB Local emulator underneath, so
behaviour can shift between majors in ways that are not DynamoDB's behaviour
changing.

`-sharedDb` on DynamoDB Local matters more than it looks. Without it, DynamoDB
Local partitions data by access key *and* region, so a test that writes with one
credential set and reads with another sees an empty table and no error.

## Running

```bash
docker compose up -d --wait          # --wait blocks on the healthcheck
DDB_ENDPOINT=http://localhost:4566 go test -tags=integration -count=1 ./...
docker compose down -v
```

`--wait` requires Compose v2.17+. Without it, the first test races the container
and fails with a connection refused that looks like a library bug.

`-count=1` disables Go's test result cache. Integration tests depend on external
state that the cache key knows nothing about, so a cached pass is meaningless.

For DynamoDB Local instead:

```bash
docker compose --profile ddblocal up -d --wait
DDB_ENDPOINT=http://localhost:8000 go test -tags=integration -count=1 ./...
```

### Optional Makefile

Recipe lines must be real tabs, not spaces.

```make
.PHONY: test integration up down

test:
	go test ./...

up:
	docker compose up -d --wait

down:
	docker compose down -v

integration: up
	DDB_ENDPOINT=http://localhost:4566 go test -tags=integration -count=1 ./...
```

## Test wiring

### Client

```go
//go:build integration

package ddbrel_test

func testClient(t *testing.T) *dynamodb.Client {
	t.Helper()

	endpoint := os.Getenv("DDB_ENDPOINT")
	if endpoint == "" {
		t.Skip("DDB_ENDPOINT not set; skipping integration tests")
	}

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}
```

Static credentials are deliberate. Falling back to the ambient credential chain
means a developer with real AWS credentials in their environment can point a test
suite at a real account, and table-per-test with cleanup is not something you want
aimed at production by accident.

### Table per test

The edge table needs no secondary index — a direct consequence of choosing dual
items over an inverted GSI. `CreateTable` is two attributes and two key elements,
which is what makes per-test tables cheap enough to be the default.

```go
func newTable(t *testing.T, c *dynamodb.Client) string {
	t.Helper()

	// Table names allow [a-zA-Z0-9_.-] only; subtest names contain '/'.
	safe := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	name := fmt.Sprintf("edges_%s_%d", safe, time.Now().UnixNano())

	ctx := context.Background()
	_, err := c.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(name),
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("PK"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("SK"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("SK"), KeyType: types.KeyTypeRange},
		},
	})
	if err != nil {
		t.Fatalf("create table %s: %v", name, err)
	}

	if err := dynamodb.NewTableExistsWaiter(c).Wait(ctx,
		&dynamodb.DescribeTableInput{TableName: aws.String(name)},
		30*time.Second,
	); err != nil {
		t.Fatalf("wait for table %s: %v", name, err)
	}

	t.Cleanup(func() {
		_, _ = c.DeleteTable(context.Background(),
			&dynamodb.DeleteTableInput{TableName: aws.String(name)})
	})

	return name
}
```

A fresh table per test buys isolation and lets tests run with `t.Parallel()`
without a shared-state cleanup dance between them.

## What integration tests cover

- **Transaction atomicity.** Forward and inverse items either both appear or
  neither does.
- **Direction isolation.** Given `USER#1 -> ORDER#5` and `SESSION#9 -> USER#1`,
  `Out("USER#1")` returns only the order, `In("USER#1")` only the session. This is
  the regression test for direction living in the sort key prefix.
- **Resolve-first upsert.** `Add` twice with different sort values leaves exactly
  one forward and one inverse item, not two of each.
- **Pagination.** Cursors are correct across page boundaries, including the last
  page, and ordering holds under `WithReverse`.
- **`RemoveAll` resumability.** Interrupted midway, a second call completes and
  reports a sane `removed` count.
- **Dangling edges.** An edge whose target was deleted from the entity table
  appears in `hydrate.Result.Missing` rather than silently shrinking `Items`.

## What the emulators cannot test

Worth stating plainly, so nobody reads a green suite as more assurance than it is.

**Throttling and capacity are not modelled at all.** Neither emulator throttles,
so `TransactionCanceledException` unwrapping — one of the library's real
value-adds — is never exercised by them. That path needs client-side fault
injection: a `middleware` step or an HTTP transport that injects the error, tested
without an emulator at all.

**Consistency is indistinguishable.** Both emulators are a single local store, so
every read is effectively strongly consistent and `WithConsistentRead` changes
nothing observable. This does not undermine the dual-item decision. That decision
rests on strong consistency being *available* on a base-table query and structurally
unavailable on a GSI — a guarantee of real DynamoDB, not a behaviour we detect
locally. The emulator cannot confirm the design is right; it also cannot mislead us
into thinking it is wrong.

**`ClientRequestToken` idempotency is unverified.** Real DynamoDB honours the token
for a 10-minute window and collapses a retried `TransactWriteItems` into one apply.
Whether either emulator enforces this is unconfirmed — verify before writing a test
that depends on it, and do not assume a local pass proves the production behaviour.

Because of the first and third points, the integration suite is a correctness check
on key layout, transactions, and query semantics. It is not a check on failure
handling. Failure handling is unit-tested with injected errors.

## CI

Run Compose as an ordinary step rather than using service containers, so CI and
local development execute the same commands against the same file:

```yaml
- run: docker compose up -d --wait
- run: DDB_ENDPOINT=http://localhost:4566 go test -tags=integration -count=1 ./...
- run: docker compose down -v
  if: always()
```
