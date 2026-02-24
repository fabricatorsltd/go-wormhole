# Resilience

Databases fail. Connections drop. Deadlocks happen. `go-wormhole` integrates
natively with `go-foundation/pkg/resiliency` to handle these scenarios
gracefully instead of crashing.


## Retry with Exponential Backoff

Every database call in the SQL provider can be automatically retried on
transient errors (e.g. `driver: bad connection`, network timeouts):

```go
import (
    "time"
    "github.com/mirkobrombin/go-foundation/pkg/resiliency"
    wormholesql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

p := wormholesql.New(db,
    wormholesql.WithRetry(
        resiliency.WithMaxAttempts(3),
        resiliency.WithBackoff(100 * time.Millisecond),
    ),
)
```

This wraps `db.QueryContext` and `db.ExecContext` calls at the provider
level. If a call fails, it retries up to 3 times with exponential backoff
(100ms → 200ms → 400ms).


## Read Retry (DbContext Level)

For read operations (`Find`, `Execute`), the `DbContext` provides its own
retry layer — independent from the provider's retry:

```go
import wh "github.com/fabricatorsltd/go-wormhole/pkg/context"

ctx := wh.New(provider,
    wh.WithReadRetry(
        resiliency.WithMaxAttempts(5),
        resiliency.WithBackoff(50 * time.Millisecond),
    ),
)
```

This double-layer approach allows different retry policies:
- **Provider level:** aggressive retries for transient connection issues
- **DbContext level:** conservative retries for application-level reads


## Circuit Breaker

When a database goes offline, you don't want to flood it with requests
that are guaranteed to fail. The circuit breaker pattern prevents this:

```go
ctx := wh.New(provider,
    wh.WithCircuitBreaker(5, 30 * time.Second),
)
```

### How It Works

```
CLOSED ──(5 failures)──→ OPEN ──(30s timeout)──→ HALF-OPEN
  ↑                                                    │
  └──────────(success)─────────────────────────────────┘
```

1. **Closed** (normal): all calls pass through
2. **Open** (fail-fast): after 5 consecutive failures, the breaker opens
   and immediately returns an error without contacting the DB
3. **Half-Open** (probe): after 30 seconds, one call is allowed through
   - If it succeeds → breaker closes (back to normal)
   - If it fails → breaker stays open for another 30 seconds

```go
// This call returns instantly with an error when the breaker is open:
user, err := wh.Find[User](ctx, dbCtx, 42)
// err: "circuit breaker is open"
```


## MultiError in SaveChanges

When `SaveChanges()` runs lifecycle hooks (`BeforeSave()`, etc.) on
multiple entities, it doesn't stop at the first error. Instead, all
validation errors are collected into a `MultiError`:

```go
type User struct {
    ID   int
    Name string
    Age  int
}

func (u *User) BeforeSave() error {
    if u.Name == "" {
        return fmt.Errorf("name is required")
    }
    if u.Age < 0 {
        return fmt.Errorf("age must be positive")
    }
    return nil
}
```

If you batch-save 3 users with different validation failures:

```go
ctx.Add(&User{Name: "", Age: 30})
ctx.Add(&User{Name: "Bob", Age: -5})
ctx.Add(&User{Name: "Alice", Age: 25})  // valid

err := ctx.Save()
// err contains 2 errors:
//   User.BeforeSave: name is required
//   User.BeforeSave: age must be positive
```

The transaction is **not opened** when validation errors exist.
The DB is never contacted. This follows the "fail fast" principle.


## Combining All Three

For production use, enable all three resilience mechanisms:

```go
p := wormholesql.New(db,
    wormholesql.WithRetry(
        resiliency.WithMaxAttempts(3),
        resiliency.WithBackoff(100 * time.Millisecond),
    ),
)

ctx := wh.New(p,
    wh.WithReadRetry(
        resiliency.WithMaxAttempts(5),
        resiliency.WithBackoff(50 * time.Millisecond),
    ),
    wh.WithCircuitBreaker(5, 30 * time.Second),
)
```

### Request Flow

```
User code
  │
  ▼
DbContext.Find()
  │
  ├─ Circuit Breaker check
  │    └─ OPEN? → return error immediately
  │
  ├─ Read Retry (up to 5 attempts)
  │    │
  │    └─ Provider.Find()
  │         │
  │         └─ Provider Retry (up to 3 attempts)
  │              │
  │              └─ db.QueryContext()
  │
  └─ Success? → reset breaker
     Failure? → increment breaker counter
```

Maximum total attempts for a single read: 5 × 3 = 15 DB calls before
giving up. Each with exponential backoff, so the total timeout grows
gracefully.


## go-foundation Integration

All resilience primitives come from `go-foundation`:

| Primitive          | Package                              |
|--------------------|--------------------------------------|
| `Retry()`          | `github.com/mirkobrombin/go-foundation/pkg/resiliency` |
| `CircuitBreaker`   | `github.com/mirkobrombin/go-foundation/pkg/resiliency` |
| `MultiError`       | `github.com/mirkobrombin/go-foundation/pkg/errors`     |

No external dependencies. No third-party retry libraries. Pure Go.
