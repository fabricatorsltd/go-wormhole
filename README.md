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
- **Aggregated errors** — `MultiError` collects validation failures across
  entities instead of failing at the first one.

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
└── tracker/      Identity Map + snapshot change detector
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"

    wh "github.com/mirkobrombin/go-wormhole/pkg/context"
    "github.com/mirkobrombin/go-wormhole/pkg/dsl"
    "github.com/mirkobrombin/go-wormhole/pkg/provider"
    "github.com/mirkobrombin/go-wormhole/pkg/query"
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

    ctx := wh.New(provider.Default())
    defer ctx.Close()

    // Type-safe queries — no magic strings
    u := &User{}
    q := ctx.Query("user").
        Filter(dsl.Gt(u, &u.Age, 18), dsl.Contains(u, &u.Name, "al")).
        OrderBy("Age", query.Desc).
        Limit(10).
        Build()
    _ = q

    // Unit of Work
    alice := &User{Name: "Alice", Age: 30}
    ctx.Add(alice)

    if err := ctx.SaveChanges(context.Background()); err != nil {
        panic(err)
    }
}
```

## License

MIT
