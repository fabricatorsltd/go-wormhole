# Providers

`go-wormhole` supports multiple storage backends through the `Provider`
interface. Each backend translates the neutral AST into native operations.

Currently two providers are built-in:

- **SQL Provider** — any `database/sql`-compatible driver (PostgreSQL, SQLite, MySQL)
- **Slipstream Provider** — embedded NoSQL using `go-slipstream` (Bitcask engine)


## Provider Interface

Every backend implements this contract:

```go
type Provider interface {
    Name() string
    Open(ctx context.Context, dsn string) error
    Close() error

    // CRUD
    Insert(ctx context.Context, meta *EntityMeta, entity any) (any, error)
    Update(ctx context.Context, meta *EntityMeta, entity any, changed []string) error
    Delete(ctx context.Context, meta *EntityMeta, pkValue any) error
    Find(ctx context.Context, meta *EntityMeta, pkValue any, dest any) error

    // Query
    Execute(ctx context.Context, meta *EntityMeta, q Query, dest any) error

    // Transactions
    Begin(ctx context.Context) (Tx, error)
}
```

The `Tx` interface mirrors `Provider` for transactional operations,
plus `Commit()` and `Rollback()`.


## Provider Registry

Providers are registered at startup using the `go-foundation/pkg/adapters`
registry:

```go
import (
    "github.com/fabricatorsltd/go-wormhole/pkg/provider"
    wormholesql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

// Register by name
sqlProv := wormholesql.New(db)
provider.Register("postgres", sqlProv)
provider.SetDefault("postgres")

// Resolve later
p := provider.Default()
// or: p, ok := provider.Resolve("postgres")
```

This decouples the `DbContext` from any specific backend. You can swap
providers (e.g. SQL → Slipstream) without touching business logic.


---

## SQL Provider

The SQL provider translates the query AST into standard SQL and uses
`database/sql` for execution.

### Setup

```go
import (
    "database/sql"
    _ "github.com/glebarez/sqlite"

    wormholesql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
    "github.com/fabricatorsltd/go-wormhole/pkg/provider"
)

db, _ := sql.Open("sqlite", "app.db")

p := wormholesql.New(db,
    wormholesql.WithName("sqlite"),
    wormholesql.WithRetry(
        resiliency.WithMaxAttempts(3),
        resiliency.WithBackoff(100 * time.Millisecond),
    ),
)
provider.Register("sqlite", p)
provider.SetDefault("sqlite")
```

### Options

| Option                 | Description                                |
|------------------------|--------------------------------------------|
| `WithName(n)`          | Override provider name (default: `"sql"`)  |
| `WithNumberedParams()` | Use `$1, $2` placeholders (PostgreSQL)     |
| `WithRetry(opts...)`   | Retry transient errors with backoff        |

### How the Compiler Works

The `Compiler` translates the query AST into SQL:

```go
// Input AST
query.From("users").
    Filter(query.Predicate{Field: "age", Op: query.OpGt, Value: 18}).
    OrderBy("name", query.Asc).
    Limit(10).
    Build()

// Output SQL
// SELECT "id", "name", "age" FROM "users" WHERE "age" > ? ORDER BY "name" ASC LIMIT 10
// Args: [18]
```

Features:
- **Partial UPDATE:** only changed columns are included in `SET` clause
- **Dynamic row scanning:** maps columns by name, not position
- **JOIN support:** compiles `Include()` into `LEFT JOIN`
- **Numbered params:** optional `$1, $2` style for PostgreSQL


### PostgreSQL

```go
p := wormholesql.New(db,
    wormholesql.WithName("postgres"),
    wormholesql.WithNumberedParams(),
)
```

Use `PostgresDialect` for migrations (SERIAL/BIGSERIAL support).


### MySQL

```go
p := wormholesql.New(db,
    wormholesql.WithName("mysql"),
)
```

Use `MySQLDialect` for migrations (backtick quoting, AUTO_INCREMENT).

> ⚠️ MySQL DDL statements cause an implicit COMMIT. See
> [migrations docs](04-migrations.md#-mysql-ddl-caveat) for details.


---

## Slipstream Provider (NoSQL)

The Slipstream provider uses `go-slipstream` — a high-performance,
embedded Bitcask key-value engine.

### Setup

```go
import "github.com/fabricatorsltd/go-wormhole/pkg/slipstream"

p, err := slipstream.New("./data",
    engine.WithSyncWrites(true),
)
provider.Register("slipstream", p)
```

### How It Works

- Entities are serialized as **JSON maps** (`map[string]any`)
- Keys follow the pattern `{tableName}#{pkValue}`
- Queries use secondary indexes when available, otherwise in-memory scan

```
Storage layout:
  users#1  →  {"id":1, "name":"Alice", "age":30}
  users#2  →  {"id":2, "name":"Bob",   "age":25}
```

### Secondary Indexes

For query performance, register indexes on the engine:

```go
sp := p.(*slipstream.Provider)
sp.Engine().AddIndex("users_age", func(r record) string {
    if v, ok := r["age"]; ok {
        return fmt.Sprint(v)
    }
    return ""
})
```

Queries on indexed fields use `GetByIndex()` instead of full scan.

### When to Use Slipstream

| Use Case                     | Recommendation |
|------------------------------|----------------|
| Embedded apps, CLI tools     | ✅ Slipstream  |
| High-throughput local cache  | ✅ Slipstream  |
| Multi-user web app           | ❌ Use SQL     |
| Complex JOINs / aggregations | ❌ Use SQL     |


---

## Multi-Provider Architecture

You can use multiple providers in the same application:

```go
// Register both
provider.Register("postgres", sqlProv)
provider.Register("cache", slipProv)

// Different DbContexts for different backends
mainCtx := wh.New(provider.MustResolve("postgres"))
cacheCtx := wh.New(provider.MustResolve("cache"))
```

Or use the DI container:

```go
import whctx "github.com/fabricatorsltd/go-wormhole/pkg/context"

container := di.New()
whctx.RegisterServices(container, sqlProv,
    whctx.WithRetry(resiliency.WithMaxAttempts(3)),
    whctx.WithCircuitBreaker(5, 30*time.Second),
)

ctx := whctx.FromContainer(container)
```
