# go-wormhole

An Entity Framework-inspired ORM / Data Mapper for Go, built on top of
[go-foundation](https://github.com/mirkobrombin/go-foundation).

## Features

- **Pointer-tracking DSL** — type-safe queries without code generation.
  `dsl.Gt(u, &u.Age, 18)` resolves the field name via memory offsets at
  runtime, producing a provider-neutral AST node.
- **Provider-neutral AST** — queries are built as an abstract syntax tree and
  translated by each backend (SQL, Slipstream / Bitcask, …).
- **Change Tracker (Unit of Work)** — in-memory identity map backed by a
  sharded concurrent map. Automatic dirty-checking via snapshot comparison.
- **Lifecycle Hooks** — `BeforeSave()`, `AfterInsert()`, etc. discovered
  automatically on entity structs via reflection.
- **Struct-tag driven schema** — `db:"column:name; type:varchar(100); primary_key"`
  parsed at startup with caching.
- **Pluggable backends** — register providers with `provider.Register("postgres", p)`;
  resolve at runtime via `provider.Default()`.
- **DI-ready** — first-class integration with `go-foundation/pkg/di`.
- **Resilient commits** — optional retry with exponential backoff around
  transactional flushes.
- **Resilient reads** — retry and circuit breaker on `Find` / `Execute`.
- **Aggregated errors** — `MultiError` collects validation failures across
  entities instead of failing at the first one.
- **Fluent EntitySet API** — `ctx.Set(&u).Find(1)` retrieves, populates and
  auto-tracks an entity in one call.
- **Code-First Migrations** — EF Core-style migration engine. Detects schema
  changes from Go structs, generates timestamped `.go` files with `Up()`/`Down()`
  methods, and applies them transactionally with history tracking.
- **Multi-Dialect DDL** — `PostgresDialect` (SERIAL/BIGSERIAL, `$N` placeholders),
  `MySQLDialect` (backtick quoting, AUTO_INCREMENT), `DefaultDialect` (SQLite).
- **Scaffold from DB** — reverse-engineers an existing database into Go structs
  with `db:"..."` tags via `wormhole dbcontext scaffold`.
- **Snapshot Rebuild** — reconstructs cumulative schema from migration history
  without a live DB connection.

## Architecture

```
pkg/
├── context/      DbContext (Unit of Work entry point) + DI helpers
├── dsl/          Pointer-tracking DSL (Eq, Gt, Contains, …)
├── model/        EntityState, EntityMeta, FieldMeta, Entry
├── provider/     Provider & Tx interfaces + adapter registry
├── query/        AST nodes, operators, fluent QueryBuilder
├── schema/       Struct-tag parser → EntityMeta (cached)
├── slipstream/   Provider: go-slipstream (Bitcask / NoSQL)
├── sql/          Provider: database/sql (Postgres, SQLite, …)
├── migrations/   Code-First migration engine (differ, runner, codegen)
└── tracker/      Identity Map + snapshot change detector
```

## Migrations

```go
runner := migrations.NewRunner(db)

runner.Add(migrations.Migration{
    ID: "001_create_users",
    Up: func(b *migrations.SchemaBuilder) {
        b.CreateTable("users",
            migrations.ColumnDef{Name: "id", SQLType: "INTEGER", PrimaryKey: true, AutoIncr: true},
            migrations.ColumnDef{Name: "name", SQLType: "TEXT"},
            migrations.ColumnDef{Name: "email", SQLType: "VARCHAR(255)", Nullable: true},
        )
        b.CreateIndex("idx_users_email", "users", true, "email")
    },
    Down: func(b *migrations.SchemaBuilder) {
        b.DropIndex("idx_users_email")
        b.DropTable("users")
    },
})

// Apply all pending migrations
runner.Up(ctx)
```

**CLI:**

```bash
wormhole migrations add CreateUsers   # generate migration from model diff
wormhole migrations list              # show pending/applied status
wormhole database update              # apply pending migrations
```

> **MySQL/MariaDB note:** DDL statements (`CREATE TABLE`, `ALTER TABLE`) cause an
> implicit `COMMIT` in MySQL. If a migration contains multiple DDL commands and one
> fails mid-way, the preceding commands **cannot be rolled back**. Postgres and SQLite
> support transactional DDL and are not affected. A future MySQL `Dialect` will handle
> this explicitly.

## Quick Start

```go
package main

import (
    "fmt"
    "time"

    wh "github.com/mirkobrombin/go-wormhole/pkg/context"
    "github.com/mirkobrombin/go-wormhole/pkg/dsl"
    "github.com/mirkobrombin/go-wormhole/pkg/provider"
    "github.com/mirkobrombin/go-wormhole/pkg/query"
    "github.com/mirkobrombin/go-foundation/pkg/resiliency"
)

type User struct {
    ID   int    `db:"primary_key; auto_increment"`
    Name string `db:"type:varchar(100)"`
    Age  int    `db:"nullable"`
}

func init() {
    dsl.Register(User{})
}

func (u *User) BeforeSave() error {
    if u.Name == "" {
        return fmt.Errorf("name required")
    }
    return nil
}

func main() {
    // provider.Register("postgres", myPostgresProvider)
    // provider.SetDefault("postgres")

    ctx := wh.New(provider.Default(),
        wh.WithReadRetry(resiliency.WithAttempts(3)),
        wh.WithCircuitBreaker(5, time.Minute),
    )
    defer ctx.Close()

    // EntitySet: Find by PK (auto-tracked as Unchanged)
    var u User
    ctx.Set(&u).Find(1)
    u.Age = 35
    ctx.Save() // partial UPDATE: only "age" column

    // EntitySet: Query with DSL predicates
    var users []User
    ctx.Set(&users).
        Where(dsl.Gt(&u, &u.Age, 18)).
        OrderBy("age", query.Desc).
        Limit(10).
        All()

    // Unit of Work
    alice := &User{Name: "Alice", Age: 30}
    ctx.Add(alice)
    ctx.Save()
}
```

## License

MIT
